#!/usr/bin/env python3
"""
Скрипт для импорта продуктов из CSV/базы в БД
"""
import asyncio
import aiosqlite
import logging
import csv
import os
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

# Путь к БД (совпадает с botes.py)
if os.path.exists("/data"):
    DB_PATH = "/data/calories_bot.db"
else:
    DB_PATH = "calories_bot.db"

# Встроенный список продуктов для импорта (примеры)
DEFAULT_PRODUCTS = [
    ("Куриное филе", 165, 31.0, 3.6, 0.0),
    ("Яйцо куриное", 155, 13.0, 11.5, 0.7),
    ("Рис белый", 130, 2.7, 0.3, 28.0),
    ("Морковь сырая", 41, 0.9, 0.2, 9.6),
    ("Яблоко", 52, 0.3, 0.2, 13.8),
    ("Хлеб пшеничный", 265, 8.3, 3.2, 49.0),
    ("Молоко коровье", 64, 3.2, 3.6, 4.8),
    ("Творог 5% жирности", 121, 17.6, 5.0, 3.3),
    ("Банан", 89, 1.1, 0.3, 22.8),
    ("Помидор", 18, 0.9, 0.2, 3.9),
    ("Огурец", 15, 0.8, 0.1, 2.8),
    ("Кабачок", 23, 1.5, 0.2, 4.6),
    ("Говяжья котлета", 214, 18.0, 15.0, 0.0),
    ("Скумбрия", 181, 20.3, 10.7, 0.0),
    ("Форель", 199, 19.0, 12.0, 0.0),
    ("Салат айсберг", 15, 1.2, 0.1, 2.9),
    ("Оливковое масло", 884, 0.0, 100.0, 0.0),
    ("Миндаль", 579, 21.2, 50.0, 21.6),
    ("Орехи грецкие", 656, 13.4, 65.2, 14.3),
    ("Молочный шоколад", 540, 8.0, 34.0, 58.0),
]

async def import_from_csv(db: aiosqlite.Connection, csv_file: str):
    """Импортировать из CSV файла"""
    if not os.path.exists(csv_file):
        logger.warning(f"⚠️ Файл {csv_file} не найден")
        return
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                try:
                    await db.execute(
                        "INSERT OR IGNORE INTO products (name, calories, proteins, fats, carbs, is_approved) VALUES (?, ?, ?, ?, ?, 1)",
                        (
                            row['name'],
                            float(row['calories']),
                            float(row['proteins']),
                            float(row['fats']),
                            float(row['carbs'])
                        )
                    )
                    count += 1
                except (KeyError, ValueError) as e:
                    logger.warning(f"⚠️ Ошибка в строке CSV: {e}")
                    continue
            
            await db.commit()
            logger.info(f"✅ Импортировано из CSV: {count} продуктов")
    except Exception as e:
        logger.error(f"❌ Ошибка при импорте CSV: {e}")

async def import_default_products(db: aiosqlite.Connection):
    """Импортировать встроенные продукты"""
    count = 0
    for name, cal, prot, fat, carb in DEFAULT_PRODUCTS:
        try:
            await db.execute(
                "INSERT OR IGNORE INTO products (name, calories, proteins, fats, carbs, is_approved) VALUES (?, ?, ?, ?, ?, 1)",
                (name, cal, prot, fat, carb)
            )
            count += 1
        except Exception as e:
            logger.warning(f"⚠️ Ошибка при добавлении {name}: {e}")
    
    await db.commit()
    logger.info(f"✅ Импортировано встроенных продуктов: {count}")

async def main():
    """Главная функция импорта"""
    logger.info("📥 Импорт продуктов начат")
    
    try:
        db = await aiosqlite.connect(DB_PATH)
        
        # 1. Импортировать встроенные продукты
        await import_default_products(db)
        
        # 2. Попытаться импортировать из CSV если есть
        csv_files = ['products.csv', 'data.csv', 'items.csv']
        for csv_file in csv_files:
            if os.path.exists(csv_file):
                await import_from_csv(db, csv_file)
                break
        
        # Проверка
        result = await db.execute("SELECT COUNT(*) FROM products")
        count = (await result.fetchone())[0]
        logger.info(f"✅ Всего продуктов в БД: {count}")
        
        await db.close()
        logger.info("✅ Импорт завершен успешно")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка импорта: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())