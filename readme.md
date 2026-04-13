# Catalog Client

Асинхронный Redis-клиент для управления каталогом товаров с поддержкой тенантов.

## Особенности

- Асинхронная работа на базе `redis-py`
- Сжатие данных (zlib + MessagePack)
- Автоматические retry при сбоях соединения
- Поддержка тенантов
- Атомарное обновление каталога

## Установка

```bash
pip install "git+https://github.com/sidorov-works/redis_catalog_client.git@v0.1.1"
```

## Быстрый старт

```python
import asyncio
from catalog_client import CatalogClient

async def main():
    client = CatalogClient(redis_host="localhost")
    
    # Обновление каталога
    await client.update_products_batch(
        tenant="shop1",
        products_by_article={
            "ART001": {
                "name": "Смартфон X100",
                "category": "Электроника",
                "characteristics": {"color": "черный", "memory": "128GB"}
            }
        },
        product_index={"смартфон x100": ["ART001"]}
    )
    
    # Поиск по артикулу
    product = await client.get_by_article("ART001", "shop1")
    print(product["name"])  # Смартфон X100
    
    await client.close()

asyncio.run(main())
```

## API

### Конструктор

```python
CatalogClient(
    redis_host="localhost",
    redis_port=6379,
    redis_password=None,
    redis_db_number=0,
    max_execution_retries=3,
    # ... другие параметры подключения
)
```

### Основные методы

| Метод | Описание |
|-------|----------|
| `get_by_article(article, tenant)` | Получить товар по артикулу |
| `get_product_name_by_article(article, tenant)` | Получить только название |
| `get_articles_by_product(product_name, tenant)` | Список артикулов по названию |
| `get_by_product(product_name, tenant)` | Обобщенные данные товара |
| `update_products_batch(tenant, products_by_article, product_index)` | Атомарное обновление каталога |
| `delete_tenant_catalog(tenant)` | Удалить каталог тенанта |
| `article_exists(article, tenant)` | Проверка существования артикула |
| `tenant_exists(tenant)` | Проверка существования тенанта |

### Формат обобщенного описания

Для товаров с несколькими артикулами:

```python
{
    "name": "Смартфон X100",
    "category": "Электроника",
    "articles": ["ART001", "ART002"],
    "common_characteristics": {"memory": "128GB"},
    "different_characteristics": {
        "color": {"ART001": "черный", "ART002": "синий"}
    }
}
```

## Структура данных в Redis

- `catalog:{tenant}:articles` — Hash: артикул → сжатые данные товара
- `catalog:{tenant}:products` — Hash: название товара → JSON-список артикулов

## Требования

- Python >= 3.9
- Redis >= 6.0

## Лицензия

MIT