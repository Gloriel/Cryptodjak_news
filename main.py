import os
import sys
import asyncio
import logging
import hashlib
import re
import html
from typing import Optional, Dict, Any, List, Set, Tuple
from datetime import datetime, timedelta, timezone
from time import mktime

import aiohttp
import feedparser
from dotenv import load_dotenv
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest


# ========= ЛОГИ =========
class SecurityFilter(logging.Filter):
    def filter(self, record):
        if hasattr(record, 'msg'):
            sensitive_terms = ['BOT_TOKEN', 'token', 'password', 'secret']
            msg_str = str(record.msg)
            for term in sensitive_terms:
                if term in msg_str:
                    record.msg = msg_str.replace(term, '***')
        return True


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
logger.addFilter(SecurityFilter())


# ========= БОТ =========
class CryptoNewsBot:
    def __init__(self):
        load_dotenv()

        self.bot_token = os.getenv("BOT_TOKEN")
        self.channel_id = os.getenv("CHANNEL_ID")

        if not self.bot_token or not self.channel_id:
            missing = []
            if not self.bot_token:
                missing.append("BOT_TOKEN")
            if not self.channel_id:
                missing.append("CHANNEL_ID")
            raise ValueError(f"Отсутствуют переменные окружения: {', '.join(missing)}")

        # Стартовый пул русскоязычных лент
        self.rss_feeds: List[str] = [
            "https://ru.cointelegraph.com/feed/",
            "https://cryptodirectories.com/ru/blog/feed/",
            "https://coinspot.io/feed/",
            "https://bitnovosti.com/feed/",
            "https://coinlife.com/feed/",
            "https://mining-bitcoin.ru/feed/",
            "https://cryptofeed.ru/feed/",
            "https://bitcoininfo.ru/feed/",
            "https://cryptocat.org/feed/",
            "https://blockchain24.ru/feed/",
            "https://cryptorussia.ru/feed/",
            "https://bitcoinist.ru/feed/",
        ]

        self.bot = Bot(token=self.bot_token)
        self.session: Optional[aiohttp.ClientSession] = None

        # Частоты
        self.check_interval = 15 * 60       # каждые 15 минут опрос лент
        self.post_interval = 60 * 60        # минимум 1 час между постами
        self.request_timeout = 20

        # Лимиты/состояние
        self.max_posts_per_day = 5
        self.posts_today = 0
        self.last_reset_date = datetime.now().date()
        self.last_post_time: Optional[datetime] = None

        # Учёт и статус лент
        self.sent_news: Set[str] = set()  # хэши отправленных
        self.feed_usage: Dict[str, int] = {f: 0 for f in self.rss_feeds}
        self.feed_errors: Dict[str, int] = {f: 0 for f in self.rss_feeds}
        self.feed_quarantine_until: Dict[str, datetime] = {}  # когда можно снова пробовать ленту

        self.stats = {
            "total_posts": 0,
            "failed_posts": 0,
            "last_success": None,
            "feed_stats": {f: 0 for f in self.rss_feeds}
        }

        logger.info(f"Бот инициализирован. Лент: {len(self.rss_feeds)}")

    # ---------- HTTP ----------
    async def initialize(self) -> None:
        self.session = aiohttp.ClientSession(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            },
            timeout=aiohttp.ClientTimeout(total=self.request_timeout),
            connector=aiohttp.TCPConnector(ssl=True, limit=10)
        )
        logger.info("HTTP-сессия создана")

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("HTTP-сессия закрыта")

    # ---------- УТИЛИТЫ ----------
    @staticmethod
    def _letters_and_digits(text: str) -> str:
        return ''.join(ch for ch in text if ch.isalnum())

    def _is_russian_text(self, text: str) -> bool:
        """Текст считается русским, если ≥ 30% кириллицы среди букв/цифр."""
        if not text:
            return False
        core = self._letters_and_digits(text)
        if not core:
            return False
        russian = re.findall(r'[а-яА-ЯёЁ]', core)
        return (len(russian) / max(1, len(core))) >= 0.30

    @staticmethod
    def _clean_text(text: str, max_length: int = 600) -> str:
        if not text:
            return ""
        try:
            no_tags = re.sub(r'<[^>]+>', '', text)
            unesc = html.unescape(no_tags)
            compact = ' '.join(unesc.split())
            if len(compact) > max_length:
                compact = compact[:max_length].rstrip() + '…'
            return compact
        except Exception:
            return (text or "")[:max_length]

    @staticmethod
    def _escape_html(text: str) -> str:
        return html.escape(text or "")

    @staticmethod
    def _domain_of(url: str) -> str:
        try:
            from urllib.parse import urlparse
            netloc = urlparse(url).netloc.lower()
            return netloc.removeprefix('www.')
        except Exception:
            return ""

    def _reset_daily_if_needed(self) -> None:
        today = datetime.now().date()
        if today != self.last_reset_date:
            self.posts_today = 0
            self.last_reset_date = today
            logger.info("Сброс дневного счётчика постов")

    def _seconds_until_next_post(self) -> int:
        if not self.last_post_time:
            return 0
        elapsed = (datetime.now() - self.last_post_time).total_seconds()
        remain = int(self.post_interval - elapsed)
        return max(0, remain)

    def _can_post_now(self) -> bool:
        self._reset_daily_if_needed()
        if self.posts_today >= self.max_posts_per_day:
            logger.info(f"Дневной лимит: {self.posts_today}/{self.max_posts_per_day}")
            return False
        return self._seconds_until_next_post() == 0

    def _news_id(self, entry: Dict[str, Any]) -> str:
        base = entry.get('link') or entry.get('id') or (entry.get('title', '') + entry.get('published', ''))
        return hashlib.sha256(base.encode('utf-8', errors='ignore')).hexdigest()[:16]

    @staticmethod
    def _parse_date(entry: Dict[str, Any]) -> Optional[datetime]:
        """Получаем дату публикации как naive-local datetime."""
        tm = None
        if 'published_parsed' in entry and entry['published_parsed']:
            tm = entry['published_parsed']
        elif 'updated_parsed' in entry and entry['updated_parsed']:
            tm = entry['updated_parsed']

        if tm:
            try:
                # mktime возвращает localtime; делаем naive в локальной зоне
                return datetime.fromtimestamp(mktime(tm))
            except Exception:
                pass
        return None

    # ---------- ПРОГРЕВ ----------
    async def test_rss_feeds(self) -> List[str]:
        """Проверяем ленты и берём только те, где есть русские записи."""
        logger.info("Тестируем RSS-ленты…")
        ok: List[str] = []
        for url in self.rss_feeds:
            try:
                async with self.session.get(url, timeout=self.request_timeout) as resp:
                    if resp.status != 200:
                        logger.warning(f"{url} — HTTP {resp.status}")
                        continue
                    content = await resp.read()
                    parsed = feedparser.parse(content)
                    titles = [e.get('title', '') for e in parsed.entries[:5]]
                    if parsed.entries and any(self._is_russian_text(t) for t in titles):
                        ok.append(url)
                        logger.info(f"✅ {url} — работает, есть русские записи")
                    else:
                        logger.warning(f"⚠️ {url} — записей мало/нет русских")
            except Exception as e:
                logger.warning(f"❌ {url} — ошибка: {e!r}")
        logger.info(f"Готовы: {len(ok)}/{len(self.rss_feeds)}")
        return ok

    # ---------- ВЫБОР ЛЕНТЫ ----------
    def _eligible_feeds(self) -> List[str]:
        now = datetime.now()
        eligible = []
        for f in self.rss_feeds:
            until = self.feed_quarantine_until.get(f)
            if until and now < until:
                # ещё в карантине
                continue
            eligible.append(f)
        return eligible

    def _get_next_feed(self) -> Optional[str]:
        eligible = self._eligible_feeds()
        if not eligible:
            return None
        # брать ту, что реже всего использовалась
        min_usage = min(self.feed_usage.get(f, 0) for f in eligible)
        candidates = [f for f in eligible if self.feed_usage.get(f, 0) == min_usage]
        import random
        chosen = random.choice(candidates)
        self.feed_usage[chosen] = self.feed_usage.get(chosen, 0) + 1
        logger.info(f"Лента выбрана: {chosen} (исп.: {self.feed_usage[chosen]})")
        return chosen

    def _quarantine_feed(self, feed: str):
        """Экспоненциальный бэкофф для ленты."""
        self.feed_errors[feed] = self.feed_errors.get(feed, 0) + 1
        tries = self.feed_errors[feed]
        # 10мин, 30мин, 1ч, 2ч, 4ч…
        backoff_minutes = min(240, int(10 * (1.5 ** (tries - 1))))
        until = datetime.now() + timedelta(minutes=backoff_minutes)
        self.feed_quarantine_until[feed] = until
        logger.warning(f"Карантин ленты {feed} на {backoff_minutes} мин (ошибок: {tries})")

    def _heal_feed(self, feed: str):
        if self.feed_errors.get(feed, 0) > 0:
            logger.info(f"Сбрасываем счётчики ошибок для {feed}")
        self.feed_errors[feed] = 0
        self.feed_quarantine_until.pop(feed, None)

    # ---------- ЗАГРУЗКА НОВОСТЕЙ ----------
    async def fetch_latest_news(self, rss_url: str) -> List[Dict[str, Any]]:
        try:
            async with self.session.get(rss_url, timeout=self.request_timeout) as response:
                if response.status != 200:
                    logger.error(f"RSS {rss_url}: HTTP {response.status}")
                    self._quarantine_feed(rss_url)
                    return []

                content = await response.read()
                parsed = feedparser.parse(content)
                if not parsed.entries:
                    logger.error(f"Пустая/нечитаемая лента: {rss_url}")
                    self._quarantine_feed(rss_url)
                    return []

                collected: List[Dict[str, Any]] = []
                for entry in parsed.entries[:30]:
                    title_raw = entry.get('title', '')
                    link = entry.get('link', '')
                    desc_raw = entry.get('description') or entry.get('summary') or entry.get('content', [{}])[0].get('value', '')

                    title = self._clean_text(title_raw, 140)
                    if not title or len(title) < 10 or not self._is_russian_text(title):
                        continue
                    if not link or not link.startswith(('http://', 'https://')):
                        continue

                    nid = self._news_id(entry)
                    if nid in self.sent_news:
                        continue

                    pub_dt = self._parse_date(entry)  # может быть None
                    description = self._clean_text(desc_raw, 450)

                    collected.append({
                        "id": nid,
                        "title": title,
                        "link": link,
                        "description": description,
                        "published": pub_dt,
                        "source": rss_url,
                        "domain": self._domain_of(link) or self._domain_of(rss_url),
                    })

                # сортируем: свежие выше, затем длина описания (чуть богаче контент)
                collected.sort(key=lambda x: (
                    0 if x["published"] else 1,
                    -(x["published"].timestamp() if x["published"] else 0),
                    -len(x["description"])
                ))

                if collected:
                    self._heal_feed(rss_url)
                    logger.info(f"Найдено {len(collected)} подходящих новостей из {rss_url}")
                else:
                    logger.info(f"Подходящих новостей в {rss_url} не найдено")

                # берём не больше трёх — не спамим
                return collected[:3]

        except Exception as e:
            logger.error(f"Ошибка RSS {rss_url}: {e!r}")
            self._quarantine_feed(rss_url)
            return []

    # ---------- ПОДГОТОВКА ПОСТА ----------
    def _format_time_line(self, published: Optional[datetime], domain: str) -> str:
        parts = []
        if published:
            # показываем локальную дату/время кратко
            parts.append(published.strftime("%d %b %Y, %H:%M"))
        if domain:
            parts.append(domain)
        if not parts:
            return ""
        return " • ".join(parts)

    def prepare_post(self, item: Dict[str, Any]) -> Dict[str, Any]:
        title_html = self._escape_html(item['title'])
        desc_html = self._escape_html(item['description'])
        link = item['link']
        meta_line = self._format_time_line(item.get("published"), item.get("domain", ""))

        # компактный, но «цепляющий» формат:
        # <b>Заголовок</b>
        # 🕒 дата • источник
        # краткое описание
        # Читать далее → (кликабельно)
        message_lines = [f"<b>{title_html}</b>"]
        if meta_line:
            message_lines.append(f"🕒 {self._escape_html(meta_line)}")
        if desc_html:
            message_lines.append(desc_html)
        message_lines.append(f'<a href="{link}">Читать далее →</a>')

        message = "\n\n".join(message_lines)

        return {
            'id': item['id'],
            'message': message,
            'title': item['title'],
            'source': item['source'],
        }

    # ---------- ОТПРАВКА ----------
    async def send_post(self, post: Dict[str, Any]) -> bool:
        max_attempts = 5
        delay = 2
        for attempt in range(1, max_attempts + 1):
            try:
                await self.bot.send_message(
                    chat_id=self.channel_id,
                    text=post['message'],
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False,
                )
                # учёт
                self.posts_today += 1
                self.stats['total_posts'] += 1
                self.stats['last_success'] = datetime.now()
                self.stats['feed_stats'][post['source']] += 1
                self.sent_news.add(post['id'])
                self.last_post_time = datetime.now()

                logger.info(f"✅ Отправлено: {post['title'][:70]}… ({self.posts_today}/{self.max_posts_per_day} сегодня)")
                return True

            except RetryAfter as e:
                wait = int(getattr(e, "retry_after", delay))
                logger.warning(f"FloodWait/RetryAfter: ждём {wait} сек")
                await asyncio.sleep(wait)
            except (TimedOut, NetworkError) as e:
                logger.warning(f"Сеть (попытка {attempt}/{max_attempts}): {e}. Ждём {delay} сек")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
            except BadRequest as e:
                logger.error(f"BadRequest: {e}. Пропускаем пост.")
                break
            except Exception as e:
                logger.error(f"Неожиданная ошибка отправки: {e}. Пропускаем пост.")
                break

        self.stats['failed_posts'] += 1
        return False

    # ---------- ОСНОВНОЙ ЦИКЛ ----------
    async def run(self) -> None:
        await self.initialize()
        try:
            # 1) Прогрев: берём только рабочие ленты
            working_feeds = await self.test_rss_feeds()
            if not working_feeds:
                logger.error("Ни одна RSS-лента не прошла тест. Проверьте источники.")
                return

            # используем только прошедшие тест ленты
            self.rss_feeds = list(working_feeds)
            # пересобираем карты статуса под новый пул
            self.feed_usage = {f: 0 for f in self.rss_feeds}
            self.feed_errors = {f: 0 for f in self.rss_feeds}
            self.feed_quarantine_until = {}
            self.stats["feed_stats"] = {f: 0 for f in self.rss_feeds}

            logger.info(f"Работаем только с прошедшими тест лентами: {len(self.rss_feeds)} шт.")
            logger.info("Бот запущен ✅")
            logger.info(f"Лимит: {self.max_posts_per_day}/сутки, пауза между постами: {self.post_interval // 60} мин")

            # 2) Главный цикл
            while True:
                try:
                    # дневной лимит
                    self._reset_daily_if_needed()
                    if self.posts_today >= self.max_posts_per_day:
                        tomorrow = datetime.combine(datetime.now().date() + timedelta(days=1), datetime.min.time())
                        sleep_s = max(60, int((tomorrow - datetime.now()).total_seconds()))
                        logger.info(f"Дневной лимит исчерпан. Спим до завтра: ~{sleep_s // 3600} ч")
                        await asyncio.sleep(sleep_s)
                        continue

                    # интервал между постами
                    wait = self._seconds_until_next_post()
                    if wait > 0:
                        nap = min(wait, max(300, self.check_interval))
                        logger.info(f"Рано постить. Ждём {nap // 60} мин")
                        await asyncio.sleep(nap)
                        continue

                    # берём ленту
                    feed = self._get_next_feed()
                    if not feed:
                        # все ленты в карантине; ждём до ближайшего выхода из карантина
                        if self.feed_quarantine_until:
                            nearest = min(self.feed_quarantine_until.values())
                            sleep_s = max(60, int((nearest - datetime.now()).total_seconds()))
                            logger.info(f"Все ленты в карантине. Ждём {sleep_s // 60} мин")
                            await asyncio.sleep(sleep_s)
                        else:
                            await asyncio.sleep(self.check_interval)
                        continue

                    news = await self.fetch_latest_news(feed)
                    if not news:
                        await asyncio.sleep(self.check_interval)
                        continue

                    # выбираем лучшую (самая свежая/информативная)
                    best = news[0]
                    post = self.prepare_post(best)
                    ok = await self.send_post(post)

                    if ok:
                        # публиковали — ждём либо check_interval, либо до следующего окна (что меньше)
                        await asyncio.sleep(min(self.check_interval, self.post_interval))
                    else:
                        await asyncio.sleep(15 * 60)

                    # санитарная очистка кэша
                    if len(self.sent_news) > 1000:
                        self.sent_news = set(list(self.sent_news)[-500:])
                        logger.info("Очищен кэш отправленных новостей")

                    active_total = len(self._eligible_feeds())
                    logger.info(f"Статистика: {self.posts_today}/{self.max_posts_per_day} сегодня, доступных лент: {active_total}/{len(self.rss_feeds)}")

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Ошибка в основном цикле: {e!r}")
                    await asyncio.sleep(min(1800, self.check_interval))

        finally:
            await self.close()
            logger.info("Бот остановлен ⛔")


# ========= ENTRY =========
async def main():
    bot = CryptoNewsBot()
    try:
        await bot.run()
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")
    except Exception as e:
        logger.critical(f"Фатальная ошибка: {e!r}")
    finally:
        logger.info("Выход")


if __name__ == "__main__":
    # Для Windows-консоли
    if os.name == 'nt':
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass
    asyncio.run(main())
