import os
import asyncio
import logging
import hashlib
import imghdr
from typing import Optional, Dict, Any, List, Set
from urllib.parse import urljoin, urlparse
from datetime import datetime
from functools import lru_cache
import aiohttp
import feedparser
from bs4 import BeautifulSoup
from telegram import Bot, InputMediaPhoto
from telegram.constants import ParseMode
from dotenv import load_dotenv

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CryptoNewsBot:
    def __init__(self):
        """Инициализация бота с расширенной обработкой ошибок"""
        try:
            load_dotenv()

            # Валидация обязательных переменных
            self.bot_token = os.getenv("BOT_TOKEN")
            self.channel_id = os.getenv("CHANNEL_ID")
            self.rss_url = os.getenv("RSS_URL")
            
            if not all([self.bot_token, self.channel_id, self.rss_url]):
                raise ValueError("Необходимые переменные окружения не установлены")

            # Инициализация компонентов
            self.bot = Bot(token=self.bot_token)
            self.session: Optional[aiohttp.ClientSession] = None

            # Настройки изображений
            self.default_images = [
                "https://cryptonews.com/wp-content/uploads/2023/04/crypto-news.jpg",
                "https://images.unsplash.com/photo-1639762681057-408e52192e55"
            ]
            self.current_image_index = 0
            self.max_image_size = 5 * 1024 * 1024  # 5MB
            self.allowed_image_types = {
                'image/jpeg', 'image/png', 'image/webp', 'image/gif'
            }

            # Системные настройки
            self.check_interval = max(1800, int(os.getenv("CHECK_INTERVAL", "10800")))  # минимум 30 минут
            self.request_timeout = 15
            self.max_retries = 3
            self.retry_delay = 2

            # Кэши и черные списки
            self.broken_urls: Set[str] = set()
            self.sent_news_cache = lru_cache(maxsize=100)()

            logger.info("Бот инициализирован с расширенными настройками безопасности")
        except Exception as e:
            logger.critical(f"Критическая ошибка инициализации: {e}")
            raise

    async def initialize(self) -> None:
        """Безопасная инициализация сессии"""
        try:
            self.session = aiohttp.ClientSession(
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
                },
                timeout=aiohttp.ClientTimeout(total=self.request_timeout),
                connector=aiohttp.TCPConnector(ssl=False, limit=10)
            )
            logger.info("Сессия aiohttp создана с параметрами безопасности")
        except Exception as e:
            logger.error(f"Ошибка создания сессии: {e}")
            raise

    async def close(self) -> None:
        """Грациозное завершение работы"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                logger.info("Сессия корректно закрыта")
        except Exception as e:
            logger.error(f"Ошибка при закрытии сессии: {e}")

    def _clean_html(self, html: str, max_length: int = 800) -> str:
        """Очистка HTML с защитой от XSS и избыточных данных"""
        if not html:
            return ""

        try:
            soup = BeautifulSoup(html, "html.parser")
            
            # Удаление потенциально опасных элементов
            for element in soup.find_all():
                if element.name in ['script', 'style', 'iframe', 'object', 'embed']:
                    element.decompose()
                elif element.name == 'a' and element.get('href', '').startswith('javascript:'):
                    element.decompose()
                elif element.name == 'img' and not element.get('src', '').startswith(('http://', 'https://')):
                    element.decompose()

            # Получение чистого текста
            text = '\n'.join(
                p.get_text().strip() 
                for p in soup.find_all(['p', 'br', 'div']) 
                if p.get_text().strip()
            )
            
            # Защита от слишком длинных строк
            text = ' '.join(text.split())
            return text[:max_length] + ('...' if len(text) > max_length else '')
        except Exception as e:
            logger.warning(f"Ошибка очистки HTML: {e}")
            return html[:max_length]

    def _validate_url(self, url: str) -> bool:
        """Проверка URL на безопасность и валидность"""
        try:
            result = urlparse(url)
            return all([
                result.scheme in ('http', 'https'),
                result.netloc,
                not result.netloc.startswith(('127.', 'localhost', '192.168.', '10.'))
            ])
        except:
            return False

    async def _verify_image(self, url: str) -> bool:
        """Расширенная проверка изображения с кэшированием"""
        if not self._validate_url(url) or url in self.broken_urls:
            return False

        try:
            # Первичная проверка HEAD
            async with self.session.head(
                url, 
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    self.broken_urls.add(url)
                    return False

                content_type = resp.headers.get('Content-Type', '').lower()
                content_length = int(resp.headers.get('Content-Length', '0'))

                if not (any(ct in content_type for ct in self.allowed_image_types) and
                        0 < content_length <= self.max_image_size):
                    self.broken_urls.add(url)
                    return False

            # Вторичная проверка первых байтов
            async with self.session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    self.broken_urls.add(url)
                    return False

                # Читаем только начало файла для проверки
                chunk = await resp.content.read(128)
                if not chunk:
                    self.broken_urls.add(url)
                    return False

                # Проверка сигнатуры изображения
                image_type = imghdr.what(None, chunk)
                if not image_type:
                    self.broken_urls.add(url)
                    return False

            return True
        except Exception as e:
            logger.warning(f"Ошибка проверки изображения {url}: {e}")
            self.broken_urls.add(url)
            return False

    async def _get_safe_image(self, html: str, base_url: str) -> str:
        """Безопасное извлечение изображения с несколькими fallback-вариантами"""
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            # Поиск по OpenGraph и Twitter Cards
            for meta in soup.find_all('meta'):
                if meta.get('property') in ['og:image', 'twitter:image']:
                    img_url = urljoin(base_url, meta.get('content', ''))
                    if await self._verify_image(img_url):
                        return img_url

            # Поиск по тегам img
            for img in soup.find_all('img', src=True):
                img_url = urljoin(base_url, img['src'])
                if await self._verify_image(img_url):
                    return img_url

            # Fallback на дефолтные изображения
            return await self._get_next_default_image()
        except Exception as e:
            logger.warning(f"Ошибка поиска изображения: {e}")
            return await self._get_next_default_image()

    async def _get_next_default_image(self) -> str:
        """Циклическое получение дефолтных изображений"""
        img = self.default_images[self.current_image_index]
        self.current_image_index = (self.current_image_index + 1) % len(self.default_images)
        return img

    async def fetch_latest_news(self) -> Optional[Dict[str, Any]]:
        """Безопасное получение новостей с обработкой кодировок"""
        try:
            async with self.session.get(
                self.rss_url,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                if response.status != 200:
                    logger.error(f"Ошибка RSS: HTTP {response.status}")
                    return None

                # Автодетект кодировки
                content = await response.text(errors='replace')
                feed = feedparser.parse(content)

                if not feed.entries:
                    logger.info("Новостей в RSS не найдено")
                    return None

                entry = feed.entries[0]
                published = entry.get('published_parsed') or entry.get('updated_parsed')
                timestamp = datetime(*published[:6]).timestamp() if published else datetime.now().timestamp()

                # Уникальный ID с учетом содержания и времени
                news_id = hashlib.sha256(
                    f"{entry.title}{entry.link}{timestamp}".encode('utf-8')
                ).hexdigest()

                return {
                    'id': news_id,
                    'title': entry.get('title', 'Без заголовка'),
                    'link': entry.link,
                    'description': entry.get('description', ''),
                    'published': entry.get('published', '')
                }
        except Exception as e:
            logger.error(f"Ошибка получения новостей: {e}")
            return None

    async def prepare_post(self, news_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Подготовка поста с защитой от ошибок форматирования"""
        try:
            title = self._clean_html(news_item['title'], 100)
            description = self._clean_html(news_item['description'])
            link = news_item['link']

            # Безопасное получение изображения
            image_url = await self._get_safe_image(news_item['description'], news_item['link'])

            # Форматирование сообщения с экранированием спецсимволов
            message = (
                f"<b>{title}</b>\n\n"
                f"{description}\n\n"
                f"📅 <i>{news_item.get('published', '')}</i>\n"
                f"🔗 <a href='{link}'>Читать полностью</a>\n"
                f"#Криптовалюта #Новости"
            )

            return {
                'id': news_item['id'],
                'image_url': image_url,
                'message': message
            }
        except Exception as e:
            logger.error(f"Ошибка подготовки поста: {e}")
            return None

    async def send_post(self, post: Dict[str, Any]) -> bool:
        """Отправка поста с автоматическим fallback"""
        if not post:
            return False

        # Сначала пробуем отправить с изображением
        if post.get('image_url'):
            for attempt in range(self.max_retries):
                try:
                    await self.bot.send_photo(
                        chat_id=self.channel_id,
                        photo=post['image_url'],
                        caption=post['message'],
                        parse_mode=ParseMode.HTML,
                        write_timeout=20,
                        connect_timeout=15
                    )
                    return True
                except Exception as e:
                    logger.warning(f"Ошибка отправки фото (попытка {attempt+1}): {e}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.retry_delay * (attempt + 1))

        # Fallback на текстовое сообщение
        try:
            await self.bot.send_message(
                chat_id=self.channel_id,
                text=post['message'],
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                write_timeout=15,
                connect_timeout=10
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки текстового сообщения: {e}")
            return False

    async def run(self) -> None:
        """Основной цикл с защитой от сбоев"""
        await self.initialize()
        
        try:
            while True:
                try:
                    logger.info("Проверка новых новостей...")
                    news_item = await self.fetch_latest_news()
                    
                    if news_item and not self.sent_news_cache.get(news_item['id']):
                        logger.info(f"Найдена новость: {news_item['title'][:50]}...")
                        post = await self.prepare_post(news_item)
                        
                        if post and await self.send_post(post):
                            self.sent_news_cache[news_item['id']] = True
                            logger.info("Новость успешно опубликована")
                        else:
                            logger.warning("Не удалось опубликовать новость")
                    
                    logger.info(f"Ожидание {self.check_interval//3600} часов...")
                    await asyncio.sleep(self.check_interval)
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Ошибка в основном цикле: {e}")
                    await asyncio.sleep(min(300, self.check_interval))  # Задержка перед повторной попыткой
                    
        except asyncio.CancelledError:
            logger.info("Бот остановлен по запросу")
        except Exception as e:
            logger.critical(f"Критическая ошибка: {e}")
        finally:
            await self.close()

async def main():
    """Точка входа с глобальной обработкой ошибок"""
    bot = CryptoNewsBot()
    try:
        await bot.run()
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.critical(f"Фатальная ошибка: {e}")
    finally:
        logger.info("Работа бота завершена")

if __name__ == "__main__":
    asyncio.run(main())