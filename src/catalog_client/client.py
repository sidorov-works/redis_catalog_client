# src/catalog_client/client.py

import zlib
import msgpack
import asyncio
import random
import json
from typing import Optional, Dict, Any, List
from redis.asyncio import Redis, ConnectionPool
from redis.exceptions import RedisError, ConnectionError

import logging
logger = logging.getLogger(__name__)

# Поля в данных товара (товар по конкретному артикулу)
CATEGORY_FIELD = "category"                # Категория
NAME_FIELD = "name"                        # Наименование товара
CHARACTERISTICS_FIELD = "characteristics"  # Характеристики

# Поля для обобщенного описания товара
COMMON_CHARACTERISTICS_FIELD = "common_characteristics"
DIFFERENT_CHARACTERISTICS_FIELD = "different_characteristics"
ARTICLES_FIELD = "articles"


class CatalogClient:
    """
    Низкоуровневый клиент для работы с каталогом товаров в Redis.

    1. catalog:{tenant}:articles → Хеш {article: compressed_data}
        Данные товара по артикулу
    2. catalog:{tenant}:products → Хеш {product_name: json_list_of_articles}
        Индекс для поиска по названию товара
    """
    
    def __init__(
            self,
            # Либо принимаем готовый (внешний) пул соединений:
            redis_connection_pool: Optional[ConnectionPool] = None,
            # Либо создаем внутри себя единичное соединение со следующими параметрами:
            redis_host: str = "localhost",
            redis_port: int = 6379,
            redis_password: Optional[str] = None,
            redis_db_number: int = 0,
            redis_socket_timeout: float = 10.0,
            redis_socket_connect_timeout: float = 10.0,
            redis_health_check_interval: float = 30.0,
            max_execution_retries: int = 3
        ):
        """
        Args:
            redis_connection_pool (Optional[ConnectionPool], optional): Готовый пул соединений Redis. Defaults to None.
                **ВНИМАНИЕ:** Если передается готовый пул, то все остальные параметры игнорируются.
            redis_host (str, optional): Имя Redis хоста (без протокола). Defaults to "localhost".
            redis_port (int, optional): Номер порта Redis. Defaults to 6379.
            redis_password (Optional[str], optional): Пароль Redis. Defaults to None.
            redis_db_number (int, optional): Номер БД Redis с каталогом (от 0 до 15). Defaults to 0.
            redis_socket_timeout (float, optional): Максимальное время ожидания ответа от Redis на любую операцию. Defaults to 10.0.
            redis_socket_connect_timeout (float, optional): Максимальное время ожидания подключения. Defaults to 10.0.
            redis_health_check_interval (float, optional): Периодичность проверки доступности Redis. Defaults to 30.0.
            max_execution_retries (int, optional): Максимальное кол-во повторных попыток выполнения операций с Redis. Defaults to 3.
        """

        # Если передали готовый connection pool, то просто сохраняем его.
        if redis_connection_pool and isinstance(redis_connection_pool, ConnectionPool):
            self._external_pool = redis_connection_pool
            self._own_connection = None  
        # Если же готовый пул соединений не передаен, создадим его
        else:
            url = f"redis://:{redis_password}@" if redis_password else "redis://"
            url += f"{redis_host}:{redis_port}/{redis_db_number}"
            
            self._own_connection = Redis.from_url(
                url=url,
                socket_timeout=redis_socket_timeout,
                socket_connect_timeout=redis_socket_connect_timeout,
                health_check_interval=redis_health_check_interval,
                retry_on_timeout=True,
                socket_keepalive=True,
                decode_responses=False
            )

            self._external_pool = None

        self._redis = None
        self._is_closed = False
        self._connection_lock = asyncio.Lock()
        self._max_execution_retries = max_execution_retries

    async def _ensure_connection(self):
        """Инициализация подключения к Redis"""
        async with self._connection_lock:
            if self._is_closed:
                raise RuntimeError("CatalogClient connection is closed")
            
            if self._external_pool:
                # Используем пул
                if self._redis is None:
                    self._redis = Redis(connection_pool=self._external_pool)
            else:
                # Используем прямое соединение
                if self._own_connection is None:
                    raise RuntimeError("Direct connection not initialized")
                self._redis = self._own_connection
            
            # Проверяем соединение
            try:
                if not await self._redis.ping():
                    raise ConnectionError("Redis ping failed")
                logger.debug("CatalogClient connected successfully")
            except Exception as e:
                if not self._external_pool:
                    # Для прямого соединения сбрасываем
                    self._own_connection = None
                self._redis = None
                logger.error(f"CatalogClient connection failed: {e}")
                raise

    async def close(self):
        """Закрытие соединения с Redis"""
        async with self._connection_lock:
            if not self._is_closed:
                if self._redis is not None:
                    await self._redis.close()
                self._is_closed = True
                self._redis = None
                self._own_connection = None
                logger.debug("CatalogClient connection closed")

    async def _execute_with_retry(self, operation, *args, **kwargs):
        """Выполнение операции с повторными попытками"""
        for attempt in range(self._max_execution_retries):
            try:
                await self._ensure_connection()
                return await operation(*args, **kwargs)
            except (RedisError, ConnectionError) as e:
                if attempt == self._max_execution_retries - 1:
                    logger.error(f"Catalog operation failed after {attempt+1} attempts: {e}")
                    raise
                
                base_delay = 0.5 * (attempt + 1)
                jitter = random.uniform(0, 0.3)
                await asyncio.sleep(base_delay + jitter)

                async with self._connection_lock:
                    if self._redis is not None:
                        try:
                            await self._redis.close()
                        except Exception as close_error:
                            # Логируем ошибку закрытия, но НЕ выкидываем
                            logger.debug(f"Error closing connection: {close_error}")
                        finally:
                            self._redis = None
                        # Исходное исключение e продолжит всплывать после блока except


    async def get_by_article(self, article: str, tenant: str) -> Optional[Dict[str, Any]]:
        """
        Получение данных товара по артикулу.
        
        Args:
            article: Артикул товара
            tenant: Идентификатор тенанта
            
        Returns:
            Словарь с данными товара или None если не найден
        """
        async def op():
            compressed = await self._redis.hget(f"catalog:{tenant}:articles", article)
            if not compressed:
                return None
            return msgpack.unpackb(zlib.decompress(compressed))
        
        return await self._execute_with_retry(op)

    async def get_product_name_by_article(self, article: str, tenant: str) -> Optional[str]:
        """
        Получение наименования товара по артикулу.
        Использует поле name из данных товара.
        
        Args:
            article: Артикул товара
            tenant: Идентификатор тенанта
            
        Returns:
            str: Наименование товара или None если артикул не найден
        """
        async def op():
            compressed = await self._redis.hget(f"catalog:{tenant}:articles", article)
            if not compressed:
                return None
            
            product_data = msgpack.unpackb(zlib.decompress(compressed))
            # Возвращаем name, если есть, иначе None
            return product_data.get(NAME_FIELD)
        
        return await self._execute_with_retry(op)
        
        
    async def get_articles_by_product(self, product_name: str, tenant: str) -> List[str]:
        """
        Получение списка артикулов по названию товара.
        
        Args:
            product_name: Название товара (очищенное)
            tenant: Идентификатор тенанта
            
        Returns:
            Список артикулов для этого товара или пустой список если не найден
        
        Raises:
            RedisError: При проблемах с подключением к Redis после исчерпания попыток
        """
        async def op():
            # Получаем JSON со списком артикулов
            articles_json = await self._redis.hget(f"catalog:{tenant}:products", product_name)
            if not articles_json:
                return []
            
            # Декодируем JSON с обработкой ошибок формата данных
            try:
                articles = json.loads(articles_json.decode('utf-8'))
                return articles if isinstance(articles, list) else []
            except (json.JSONDecodeError, AttributeError) as e:
                # Ошибка в данных - логируем, но не падаем
                logger.warning(f"Invalid JSON data for product '{product_name}' (tenant: {tenant}): {e}")
                return []
        
        # Только ретраи для сетевых ошибок, ошибки данных не ретраим
        return await self._execute_with_retry(op)

    async def get_by_product(self, product_name: str, tenant: str) -> Optional[Dict[str, Any]]:
        """
        Получение обобщенных данных товара по названию.
        Если у товара несколько артикулов, возвращает обобщенное описание.
        
        Args:
            product_name: Название товара (очищенное)
            tenant: Идентификатор тенанта
            
        Returns:
            Словарь с обобщенными данными или None если не найден
        """
        async def op():
            # Получаем список артикулов для этого товара
            articles_json = await self._redis.hget(f"catalog:{tenant}:products", product_name)
            if not articles_json:
                return None
                
            articles = json.loads(articles_json.decode('utf-8'))
            if not articles:
                return None
                
            # Если только один артикул - возвращаем его данные
            if len(articles) == 1:
                compressed = await self._redis.hget(f"catalog:{tenant}:articles", articles[0])
                if not compressed:
                    return None
                return msgpack.unpackb(zlib.decompress(compressed))
            
            # Если же у товара несколько артикулов, формируем обобщенное описание
            return await self._create_generalized_description(product_name, articles, tenant)
        
        return await self._execute_with_retry(op)

    async def _create_generalized_description(
            self, product_name: str, 
            articles: List[str], 
            tenant: str
            ) -> Dict[str, Any]:
        """
        Создание обобщенного описания для товара с несколькими артикулами.
        
        Args:
            product_name: Название товара
            articles: Список артикулов
            tenant: Идентификатор тенанта
            
        Returns:
            Словарь с обобщенным описанием:
            {
                'name': str,
                'category': str,
                'articles': List[str],
                'common_characteristics': Dict[str, str],
                'different_characteristics': Dict[str, Dict[str, str]]
            }
        """
        # Собираем данные всех артикулов
        all_products = []
        for article in articles:
            compressed = await self._redis.hget(f"catalog:{tenant}:articles", article)
            if compressed:
                try:
                    product_data = msgpack.unpackb(zlib.decompress(compressed))
                    all_products.append(product_data)
                except Exception as e:
                    logger.warning(f"Error decompressing article {article}: {e}")
                    continue
        
        if not all_products:
            # Возвращаем минимальную структуру вместо сообщения об ошибке
            return {
                NAME_FIELD: product_name,
                CATEGORY_FIELD: '',
                ARTICLES_FIELD: articles,
                COMMON_CHARACTERISTICS_FIELD: {},
                DIFFERENT_CHARACTERISTICS_FIELD: {}
            }
        
        # Берем первый товар как базовый
        base_product = all_products[0]
        result = {
            NAME_FIELD: product_name,
            CATEGORY_FIELD: base_product.get(CATEGORY_FIELD, ''),
            ARTICLES_FIELD: articles,
            COMMON_CHARACTERISTICS_FIELD: {},
            DIFFERENT_CHARACTERISTICS_FIELD: {}
        }
        
        # Определяем общие и различные характеристики
        common_chars = {}
        diff_chars = {}
        
        # Проходим по всем характеристикам базового товара
        base_characteristics = base_product.get(CHARACTERISTICS_FIELD, {})
        if not base_characteristics:
            return result
        
        for char_name, base_value in base_characteristics.items():
            if not base_value:  # Пропускаем пустые значения
                continue
                
            values = {base_value}
            # Собираем значения этой характеристики у всех товаров
            for product in all_products[1:]:
                value = product.get(CHARACTERISTICS_FIELD, {}).get(char_name)
                if value:
                    values.add(value)
            
            # Если все значения одинаковые - общая характеристика
            if len(values) == 1:
                common_chars[char_name] = base_value
            else:
                # Различные значения по артикулам
                char_values_by_article = {}
                for article, product in zip(articles, all_products):
                    value = product.get(CHARACTERISTICS_FIELD, {}).get(char_name, "")
                    if value:
                        char_values_by_article[article] = value
                if char_values_by_article:
                    diff_chars[char_name] = char_values_by_article
        
        # Сохраняем в структурированном виде
        result[COMMON_CHARACTERISTICS_FIELD] = common_chars
        result[DIFFERENT_CHARACTERISTICS_FIELD] = diff_chars
        
        return result

    async def update_products_batch(self, tenant: str, products_by_article: Dict, 
                                product_index: Dict):
        """Пакетное обновление с атомарной ЗАМЕНОЙ каталога"""
        async def op():
            # Создаем временные ключи
            temp_articles_key = f"catalog:{tenant}:articles:temp"
            temp_products_key = f"catalog:{tenant}:products:temp"
            
            async with self._redis.pipeline(transaction=True) as pipe:
                # 1. Записываем во временные структуры
                for article, data in products_by_article.items():
                    compressed = zlib.compress(msgpack.packb(data))
                    pipe.hset(temp_articles_key, article, compressed)
                
                for product_name, articles in product_index.items():
                    pipe.hset(temp_products_key, product_name, json.dumps(articles))
                
                # 2. Атомарно заменяем старые данные новыми
                pipe.rename(temp_articles_key, f"catalog:{tenant}:articles")
                pipe.rename(temp_products_key, f"catalog:{tenant}:products")
                
                await pipe.execute()
        
        await self._execute_with_retry(op)

    async def delete_tenant_catalog(self, tenant: str):
        """
        Полное удаление каталога товаров для указанного тенанта.
        Удаляет обе структуры: articles и products.
        """
        async def op():
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.delete(f"catalog:{tenant}:articles")
                pipe.delete(f"catalog:{tenant}:products")
                await pipe.execute()
            logger.info(f"Каталог тенанта {tenant} удален из Redis")
        
        await self._execute_with_retry(op)

    async def tenant_exists(self, tenant: str) -> bool:
        """Проверка существования тенанта"""
        async def op():
            # Проверяем наличие любой из структур
            exists = await self._redis.exists(f"catalog:{tenant}:articles")
            return bool(exists)
        
        return await self._execute_with_retry(op)

    async def article_exists(self, article: str, tenant: str) -> bool:
        """Проверка существования артикула"""
        async def op():
            exists = await self._redis.hexists(f"catalog:{tenant}:articles", article)
            return bool(exists)
        
        return await self._execute_with_retry(op)

    async def product_name_exists(self, product_name: str, tenant: str) -> bool:
        """Проверка существования названия товара"""
        async def op():
            exists = await self._redis.hexists(f"catalog:{tenant}:products", product_name)
            return bool(exists)
        
        return await self._execute_with_retry(op)

    async def get_all_search_data(self, tenant: str) -> Dict[str, Dict]:
        """
        Получение данных для поиска в новом формате.
        
        Возвращает:
        - Для артикулов: данные из catalog:{tenant}:articles
        - Для названий товаров: ключи из catalog:{tenant}:products
        """
        async def op():
            search_data = {}
            
            # 1. Собираем данные по артикулам (только артикулы!)
            cursor = 0
            while True:
                cursor, items = await self._redis.hscan(
                    f"catalog:{tenant}:articles", 
                    cursor=cursor, 
                    count=100
                )
                
                for article_bytes, compressed in items.items():
                    try:
                        if compressed:
                            article = article_bytes.decode('utf-8') if isinstance(article_bytes, bytes) else str(article_bytes)
                            product_data = msgpack.unpackb(zlib.decompress(compressed))
                            
                            # ТОЛЬКО артикулы, без создания записей по названиям!
                            search_data[article] = {
                                'type': 'article',
                                NAME_FIELD: product_data.get(NAME_FIELD, ''),
                                ARTICLES_FIELD: [article],
                                CATEGORY_FIELD: product_data.get(CATEGORY_FIELD, '')
                            }
                    except Exception as e:
                        logger.warning(f"Error processing article {article_bytes}: {str(e)}")
                        continue
                
                if cursor == 0:
                    break
            
            # 2. Собираем данные по названиям товаров (ключи из products)
            cursor = 0
            while True:
                cursor, items = await self._redis.hscan(
                    f"catalog:{tenant}:products", 
                    cursor=cursor, 
                    count=100
                )
                
                for name_bytes, articles_json in items.items():
                    try:
                        product_name = name_bytes.decode('utf-8') if isinstance(name_bytes, bytes) else str(name_bytes)
                        articles = json.loads(articles_json.decode('utf-8'))
                        
                        if articles and product_name:
                            # Берем первый артикул для получения категории
                            first_article = articles[0]
                            compressed = await self._redis.hget(f"catalog:{tenant}:articles", first_article)
                            
                            category = ''
                            if compressed:
                                try:
                                    product_data = msgpack.unpackb(zlib.decompress(compressed))
                                    category = product_data.get(CATEGORY_FIELD, '')
                                except:
                                    pass
                            
                            # ТОЛЬКО названия товаров как отдельные записи
                            search_data[product_name] = {
                                'type': 'product',
                                NAME_FIELD: product_name,
                                ARTICLES_FIELD: articles,
                                CATEGORY_FIELD: category
                            }
                    except Exception as e:
                        logger.warning(f"Error processing product name {name_bytes}: {str(e)}")
                        continue
                
                if cursor == 0:
                    break
                            
            return search_data
        
        return await self._execute_with_retry(op)