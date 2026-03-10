import asyncio
import aiosqlite
import logging
from datetime import datetime, timedelta
from typing import Union, Optional, Tuple, List, Any
import os
import io
import re

# Библиотеки для графиков
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# Важно для работы на сервере без монитора
plt.switch_backend('Agg')

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, BotCommand, Message
)
from aiogram.exceptions import TelegramNetworkError, TelegramForbiddenError, TelegramBadRequest
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.default import DefaultBotProperties

# ================= ЛОГИРОВАНИЕ =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ================= КОНСТАНТЫ =================
MEALS = {
    "Breakfast": "Завтрак",
    "Lunch": "Обед",
    "Dinner": "Ужин",
    "Snack": "Перекус"
}

MODE_NAMES = {
    'loss': 'Похудение',
    'maintain': 'Поддержание',
    'gain': 'Набор'
}

MODE_FACTORS = {
    'loss': 0.85,      # -15%
    'maintain': 1.0,   # без изменений
    'gain': 1.15       # +15%
}

# ================= НАСТРОЙКИ =================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не установлен в переменных окружения!")

ADMIN_ID = os.getenv("ADMIN_ID")
if ADMIN_ID:
    try:
        ADMIN_ID = int(ADMIN_ID)
    except ValueError:
        logger.warning("⚠️  ADMIN_ID не корректен, админка отключена")
        ADMIN_ID = None
else:
    logger.info("ℹ️  ADMIN_ID не задан")

# Логика пути к базе данных для Amvera
plt.style.use('dark_background')
if os.path.exists("/data"):
    DB_PATH = "/data/calories_bot.db"
else:
    DB_PATH = "calories_bot.db"

# ================= RATE LIMIT (ЗАЩИТА ОТ СПАМА) =================

user_last_message: dict[int, float] = {}  # user_id -> timestamp
RATE_LIMIT_SECONDS = 0.5  # Минимум 0.5 сек между сообщениями

async def rate_limit_middleware(handler, event, data):
    """Middleware для защиты от спама"""
    user_id = event.from_user.id if hasattr(event, 'from_user') else None
    
    if user_id:
        now = asyncio.get_event_loop().time()
        last_time = user_last_message.get(user_id, 0)
        
        if now - last_time < RATE_LIMIT_SECONDS:
            logger.warning(f"⚠️ Rate limit для пользователя {user_id}")
            return  # Пропустить сообщение
        
        user_last_message[user_id] = now
    
    return await handler(event, data)

# ================= BOT =================

session = AiohttpSession(timeout=60)
bot = Bot(
    token=TELEGRAM_TOKEN,
    session=session,
    default=DefaultBotProperties(parse_mode="Markdown")
)
dp = Dispatcher()

# Добавить rate limit middleware
dp.message.middleware(rate_limit_middleware)
dp.callback_query.middleware(rate_limit_middleware)

# ================= GLOBAL DB CONNECTION POOL =================

db: Optional[aiosqlite.Connection] = None

async def init_connection_pool():
    """Инициализировать глобальное подключение к БД"""
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA temp_store=MEMORY;")
    await db.execute("PRAGMA cache_size=-64000;")
    logger.info("✅ Подключение к БД установлено")

async def close_connection_pool():
    """Закрыть глобальное подключение"""
    global db
    if db:
        await db.close()
        logger.info("⛔ Подключение к БД закрыто")

# ================= DATABASE (ASYNC) =================

async def init_db():
    """Инициализация БД с FTS5 для поиска и триггерами"""
    global db
    
    # Пользователи
    await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            join_date TEXT
        )
    """)
    
    # Продукты (с FTS5 для быстрого поиска)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            calories REAL,
            proteins REAL,
            fats REAL,
            carbs REAL,
            is_approved INTEGER DEFAULT 0
        )
    """)
    
    # FTS5 для полнотекстового поиска
    await db.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS products_fts USING fts5(
            name,
            content='products',
            content_rowid='id'
        )
    """)
    
    # Триггер: при INSERT нового продукта → обновить FTS5
    await db.execute("""
        CREATE TRIGGER IF NOT EXISTS products_ai AFTER INSERT ON products BEGIN
            INSERT INTO products_fts(rowid, name) VALUES (new.id, new.name);
        END;
    """)
    
    # Триггер: при UPDATE продукта → обновить FTS5
    await db.execute("""
        CREATE TRIGGER IF NOT EXISTS products_au AFTER UPDATE ON products BEGIN
            UPDATE products_fts SET name = new.name WHERE rowid = new.id;
        END;
    """)
    
    # Триггер: при DELETE продукта → удалить из FTS5
    await db.execute("""
        CREATE TRIGGER IF NOT EXISTS products_ad AFTER DELETE ON products BEGIN
            DELETE FROM products_fts WHERE rowid = old.id;
        END;
    """)
    
    # Потребление
    await db.execute("""
        CREATE TABLE IF NOT EXISTS consumption (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            product_id INTEGER,
            weight REAL,
            meal_type TEXT,
            date TEXT
        )
    """)
    
    # Настройки + РЕЖИМ
    await db.execute("""
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY,
            calorie_goal REAL,
            mode TEXT DEFAULT 'maintain'
        )
    """)
    
    # Избранное
    await db.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            user_id INTEGER,
            product_id INTEGER,
            PRIMARY KEY (user_id, product_id)
        )
    """)

    # Индексы
    await db.execute("CREATE INDEX IF NOT EXISTS idx_products_name ON products(name COLLATE NOCASE)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_consumption_user_date ON consumption(user_id, date)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_consumption_user ON consumption(user_id)")
    
    # МИГРАЦИИ
    migrations = [
        "ALTER TABLE products ADD COLUMN is_approved INTEGER DEFAULT 0",
        "ALTER TABLE user_settings ADD COLUMN mode TEXT DEFAULT 'maintain'",
        "ALTER TABLE consumption ADD COLUMN product_id INTEGER"
    ]
    
    for migration in migrations:
        try:
            await db.execute(migration)
        except aiosqlite.OperationalError:
            pass  # Уже применена
    
    await db.commit()
    logger.info("✅ БД инициализирована с триггерами FTS5")

# ================= DB HELPERS (ФАСАД ДЛЯ БД) =================

async def db_execute(query: str, params: tuple = ()) -> None:
    """Выполнить запрос БД без возврата результата"""
    if not db:
        raise RuntimeError("БД не инициализирована!")
    await db.execute(query, params)
    await db.commit()

async def db_fetchone(query: str, params: tuple = ()) -> Optional[tuple]:
    """Получить одну строку"""
    if not db:
        raise RuntimeError("БД не инициализирована!")
    cursor = await db.execute(query, params)
    result = await cursor.fetchone()
    await cursor.close()
    return result

async def db_fetchall(query: str, params: tuple = ()) -> List[tuple]:
    """Получить все строки"""
    if not db:
        raise RuntimeError("БД не инициализирована!")
    cursor = await db.execute(query, params)
    result = await cursor.fetchall()
    await cursor.close()
    return result

async def add_user_to_db(user: types.User):
    """Добавить пользователя в БД"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await db_execute(
        "INSERT OR IGNORE INTO users (user_id, username, join_date) VALUES (?, ?, ?)",
        (user.id, user.username, now)
    )

# ================= HELPERS =================

def validate_float(text: str) -> Optional[float]:
    """Безопасное преобразование в float"""
    try:
        if not text or not text.strip():
            return None
        return float(text.replace(',', '.'))
    except (ValueError, AttributeError):
        return None

async def get_user_settings(user_id: int) -> Tuple[float, str]:
    """Возвращает (цель_калорий, режим)"""
    result = await db_fetchone(
        'SELECT calorie_goal, mode FROM user_settings WHERE user_id = ?',
        (user_id,)
    )
    return (result[0], result[1]) if result else (2000.0, 'maintain')

async def get_day_summary(user_id: int, date_str: str) -> float:
    """Вычислить сумму калорий за день"""
    result = await db_fetchone("""
        SELECT SUM(p.calories * c.weight / 100)
        FROM consumption c
        JOIN products p ON p.id = c.product_id
        WHERE c.user_id = ? AND c.date = ?
    """, (user_id, date_str))
    return result[0] if result and result[0] else 0.0

async def get_product_by_id(prod_id: int) -> Optional[tuple]:
    """Получить информацию о продукте"""
    return await db_fetchone(
        "SELECT name, calories, proteins, fats, carbs, is_approved, id FROM products WHERE id = ?",
        (prod_id,)
    )

# ================= GRAPHICS HELPERS =================

async def get_stats_data(user_id: int, days: int) -> Tuple[List[str], List[float], List[float], List[float], List[float]]:
    """Получить статистику за N дней (оптимизированно - один запрос вместо цикла)"""
    dates, calories, proteins, fats, carbs = [], [], [], [], []
    end_date = datetime.now()
    start_date = (end_date - timedelta(days=days - 1)).strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # ОПТИМИЗАЦИЯ: Один запрос вместо N запросов в цикле
    rows = await db_fetchall("""
        SELECT 
            c.date,
            SUM(p.calories * c.weight / 100),
            SUM(p.proteins * c.weight / 100),
            SUM(p.fats * c.weight / 100),
            SUM(p.carbs * c.weight / 100)
        FROM consumption c
        JOIN products p ON p.id = c.product_id
        WHERE c.user_id = ? AND c.date BETWEEN ? AND ?
        GROUP BY c.date
        ORDER BY c.date
    """, (user_id, start_date, end_date_str))
    
    # Создать словарь для быстрого поиска
    data_by_date = {row[0]: row[1:] for row in rows}
    
    # Заполнить все дни (даже пустые)
    for i in range(days - 1, -1, -1):
        d = (end_date - timedelta(days=i)).strftime("%Y-%m-%d")
        dates.append(datetime.strptime(d, "%Y-%m-%d").strftime("%d.%m"))
        
        if d in data_by_date:
            data = data_by_date[d]
            calories.append(data[0] if data[0] else 0)
            proteins.append(data[1] if data[1] else 0)
            fats.append(data[2] if data[2] else 0)
            carbs.append(data[3] if data[3] else 0)
        else:
            calories.append(0)
            proteins.append(0)
            fats.append(0)
            carbs.append(0)
    
    return dates, calories, proteins, fats, carbs

def generate_plot(dates: List[str], calories: List[float], goal: float) -> io.BytesIO:
    """Генерировать график калорий"""
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(dates, calories, color='#4CAF50', alpha=0.7, label='Калории')
    ax.axhline(y=goal, color='#FF5252', linestyle='--', linewidth=2, label=f'Норма ({goal:.0f})')
    
    for bar, val in zip(bars, calories):
        if val > goal:
            bar.set_color('#FF9800')
    
    ax.set_title('Динамика калорий', fontsize=16, color='white', pad=20)
    ax.set_ylabel('ккал', fontsize=12)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    if len(dates) > 7:
        plt.xticks(rotation=45)
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)
    return buf

# ================= FSM =================

class TrackFood(StatesGroup):
    choosing_meal = State()
    search = State()
    weight = State()

class AddProduct(StatesGroup):
    calories = State()
    proteins = State()
    fats = State()
    carbs = State()

class Settings(StatesGroup):
    choosing_mode = State()
    base_calories = State()

class StatsState(StatesGroup):
    waiting_for_days = State()

class AdminEditState(StatesGroup):
    waiting_for_value = State()
    
class AdminBroadcast(StatesGroup):
    waiting_for_message = State()

# ================= KEYBOARDS =================

def main_kb(user_id=None):
    buttons = [
        [KeyboardButton(text="📝 Записать прием пищи"), KeyboardButton(text="📋 Сегодня")],
        [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="⚙️ Цель и Режим")]
    ]
    if user_id and ADMIN_ID and user_id == ADMIN_ID:
        buttons.append([KeyboardButton(text="👑 Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def admin_panel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика бота", callback_data="adm_stats_global")],
        [InlineKeyboardButton(text="📢 Рассылка всем", callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="🔍 Непроверенные продукты", callback_data="adm_check_pending")],
        [InlineKeyboardButton(text="🔄 Обновить БД", callback_data="adm_update_db")],
        [InlineKeyboardButton(text="🔙 Закрыть", callback_data="close_profile")]
    ])

def profile_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика (График)", callback_data="open_stats_menu")],
        [InlineKeyboardButton(text="🗑 Очистить день", callback_data="pre_clear_day")],
        [InlineKeyboardButton(text="🔒 Закрыть", callback_data="close_profile")]
    ])

def confirm_clear_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить всё", callback_data="perform_clear_day")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_profile")]
    ])

def meal_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🍳 Завтрак", callback_data="meal_Breakfast"),
            InlineKeyboardButton(text="🍲 Обед", callback_data="meal_Lunch")
        ],
        [
            InlineKeyboardButton(text="🍖 Ужин", callback_data="meal_Dinner"),
            InlineKeyboardButton(text="🍏 Перекус", callback_data="meal_Snack")
        ],
        [InlineKeyboardButton(text="⭐ Мои продукты", callback_data="show_favorites")],
        [InlineKeyboardButton(text="🏠 Отмена", callback_data="to_main")]
    ])

def mode_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📉 Похудение", callback_data="mode_loss")],
        [InlineKeyboardButton(text="⚖️ Поддержание", callback_data="mode_maintain")],
        [InlineKeyboardButton(text="📈 Набор массы", callback_data="mode_gain")]
    ])

def after_track_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить ещё", callback_data="add_more_food")],
        [InlineKeyboardButton(text="🏠 В меню", callback_data="to_main")]
    ])

def stats_period_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сегодня", callback_data="stats_1"), InlineKeyboardButton(text="3 дня", callback_data="stats_3")],
        [InlineKeyboardButton(text="Неделя", callback_data="stats_7"), InlineKeyboardButton(text="Месяц", callback_data="stats_30")],
        [InlineKeyboardButton(text="🔢 Свой период", callback_data="stats_custom")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_profile")]
    ])

# ================= SAFE SEND =================

async def safe_answer(obj: Union[Message, CallbackQuery], text: str, **kwargs):
    """Безопасный ответ с retry логикой для Telegram ошибок"""
    max_retries = 3
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            if isinstance(obj, CallbackQuery):
                try:
                    await obj.message.edit_text(text, **kwargs)
                except TelegramBadRequest:
                    # Если сообщение не изменилось, отправим новое
                    await obj.message.answer(text, **kwargs)
            else:
                await obj.answer(text, **kwargs)
            return  # Успешно!
        
        except (TelegramNetworkError, TimeoutError) as e:
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ Retry {attempt + 1}/{max_retries} после ошибки: {e}")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"❌ Ошибка после {max_retries} попыток: {e}")
                raise
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при отправке сообщения: {e}")
            break

# ================= START & MENU =================

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await add_user_to_db(message.from_user)
    await safe_answer(message, "🍏 *Бот готов к работе!*", reply_markup=main_kb(message.from_user.id))

@dp.message(F.text.in_(["📝 Записать прием пищи", "⚙️ Цель и Режим", "📋 Сегодня", "👤 Профиль", "👑 Админ-панель"]))
async def handle_menu_buttons(message: types.Message, state: FSMContext):
    await state.clear()
    txt = message.text
    if txt == "📝 Записать прием пищи":
        await track_food_start(message, state)
    elif txt == "📋 Сегодня":
        await show_today_history(message)
    elif txt == "⚙️ Цель и Режим":
        await set_goal_start_mode(message, state)
    elif txt == "👤 Профиль":
        await show_profile(message)
    elif txt == "👑 Админ-панель":
        if message.from_user.id == ADMIN_ID:
            await show_admin_panel(message)
        else:
            await message.answer("Нет доступа.")

# ================= GOAL & MODE (НАСТРОЙКИ) =================

async def set_goal_start_mode(message: types.Message, state: FSMContext):
    goal, mode = await get_user_settings(message.from_user.id)
    mode_name = MODE_NAMES.get(mode, 'Unknown')
    
    await safe_answer(
        message,
        f"⚙️ *Настройка цели*\nСейчас: {goal} ккал ({mode_name})\n\n"
        f"Выбер��те режим:",
        reply_markup=mode_kb()
    )
    await state.set_state(Settings.choosing_mode)

@dp.callback_query(Settings.choosing_mode, F.data.startswith("mode_"))
async def mode_chosen(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split("_")[1]
    await state.update_data(selected_mode=mode)
    
    prompts = {
        'loss': "📉 *Режим похудения*\nМы отнимем ~15% от вашей нормы.\nВведите вашу норму поддержки (сколько тратите в покое):",
        'maintain': "⚖️ *Поддержание*\nВведите вашу норму калорий:",
        'gain': "📈 *Набор массы*\nМы добавим +15% (сурплюс).\nВведите вашу норму поддержки:"
    }
    
    await callback.message.edit_text(prompts[mode])
    await state.set_state(Settings.base_calories)

@dp.message(Settings.base_calories)
async def calc_goal(message: types.Message, state: FSMContext):
    val = validate_float(message.text)
    if val is None or val <= 500:
        await message.answer("Введите адекватное число (например, 2000).")
        return
    
    data = await state.get_data()
    mode = data.get('selected_mode', 'maintain')
    
    # Применить множитель в зависимости от режима
    final_goal = val * MODE_FACTORS.get(mode, 1.0)
        
    await db_execute(
        'INSERT OR REPLACE INTO user_settings (user_id, calorie_goal, mode) VALUES (?, ?, ?)',
        (message.from_user.id, final_goal, mode)
    )
        
    await message.answer(
        f"✅ *Готово!*\nБаза: {val} ккал\nРежим: {mode}\n🎯 *Новая цель: {final_goal:.0f} ккал*",
        reply_markup=main_kb(message.from_user.id)
    )
    await state.clear()

# ================= ADMIN PANEL =================

async def show_admin_panel(message: types.Message):
    await message.answer("👑 *Панель администратора*", reply_markup=admin_panel_kb())

@dp.callback_query(F.data == "adm_stats_global")
async def admin_global_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    users_c = (await db_fetchone("SELECT COUNT(*) FROM users"))[0]
    prod_c = (await db_fetchone("SELECT COUNT(*) FROM products"))[0]
    pend_c = (await db_fetchone("SELECT COUNT(*) FROM products WHERE is_approved = 0"))[0]
    rec_c = (await db_fetchone("SELECT COUNT(*) FROM consumption"))[0]
    
    text = f"📊 Пользователей: {users_c}\n🍏 Продуктов: {prod_c}\n⏳ На проверке: {pend_c}\n📝 Записей: {rec_c}"
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="adm_back_to_panel")]]))

@dp.callback_query(F.data == "adm_back_to_panel")
async def back_to_adm(c: CallbackQuery):
    await c.message.edit_text("👑 Панель", reply_markup=admin_panel_kb())

@dp.callback_query(F.data == "adm_check_pending")
async def admin_check_pending(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    row = await db_fetchone("SELECT id, name, calories, proteins, fats, carbs FROM products WHERE is_approved = 0 LIMIT 1")
    if not row:
        await callback.answer("✅ Все проверено!", show_alert=True)
        return
    
    msg = f"🆕 *{row[1]}*\n🔥 {row[2]} ккал\nБ: {row[3]} Ж: {row[4]} У: {row[5]}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ ОК", callback_data=f"adm_approve_{row[0]}"), InlineKeyboardButton(text="❌ Удалить", callback_data=f"adm_reject_{row[0]}")],
        [InlineKeyboardButton(text="✏️ Изм", callback_data=f"adm_edit_menu_{row[0]}")]
    ])
    await callback.message.edit_text(msg, reply_markup=kb)

@dp.callback_query(F.data.startswith("adm_approve_"))
async def adm_appr(c: CallbackQuery):
    pid = c.data.split("_")[2]
    await db_execute("UPDATE products SET is_approved=1 WHERE id=?", (pid,))
    await c.message.edit_text("✅ Одобрено")
    await admin_check_pending(c)

@dp.callback_query(F.data.startswith("adm_reject_"))
async def adm_rej(c: CallbackQuery):
    pid = c.data.split("_")[2]
    await db_execute("DELETE FROM products WHERE id=?", (pid,))
    await c.message.edit_text("❌ Удалено")
    await admin_check_pending(c)

@dp.callback_query(F.data == "adm_update_db")
async def update_db(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    await callback.message.edit_text("⏳ Обновляю базу...")
    
    try:
        proc = await asyncio.create_subprocess_exec(
            "python3", "importer.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode == 0:
            await callback.message.edit_text("✅ База успешно обновлена", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="adm_back_to_panel")]]))
        else:
            error_msg = stderr.decode()[:100]
            await callback.message.edit_text(f"❌ Ошибка:\n{error_msg}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="adm_back_to_panel")]]))
    except Exception as e:
        logger.error(f"Ошибка при обновлении БД: {e}")
        await callback.message.edit_text(f"❌ Исключение:\n{str(e)[:100]}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="adm_back_to_panel")]]))

# ================= PROFILE & STATS =================

async def show_profile(message: types.Message):
    user_id = message.from_user.id
    goal, mode = await get_user_settings(user_id)
    mode_name = MODE_NAMES.get(mode, mode)
    await safe_answer(message, f"👤 *Кабинет*\n🎯 Цель: *{goal:.0f} ккал*\n📊 Режим: {mode_name}", reply_markup=profile_kb())

@dp.callback_query(F.data == "close_profile")
async def close_p(c: CallbackQuery):
    await c.message.delete()

@dp.callback_query(F.data == "open_stats_menu")
async def open_stats(c: CallbackQuery):
    await c.message.edit_text("📊 Период:", reply_markup=stats_period_kb())

@dp.callback_query(F.data.startswith("stats_"))
async def stats_handler(c: CallbackQuery, state: FSMContext):
    if "custom" in c.data:
        await c.message.edit_text("Введите число дней:")
        await state.set_state(StatsState.waiting_for_days)
        return
    days = int(c.data.split("_")[1])
    await show_stats(c.message, days)

@dp.message(StatsState.waiting_for_days)
async def stats_custom(m: Message, state: FSMContext):
    if m.text and m.text.isdigit():
        await show_stats(m, int(m.text))
    await state.clear()

async def show_stats(message: types.Message, days: int):
    user_id = message.chat.id
    goal, _ = await get_user_settings(user_id)
    dates, cals, prots, fats, car = await get_stats_data(user_id, days)
    
    total_c = sum(cals)
    if total_c == 0:
        await safe_answer(message, "Нет данных.")
        return

    # Генерация фото
    photo_buf = await asyncio.to_thread(generate_plot, dates, cals, goal)
    photo_file = types.BufferedInputFile(photo_buf.read(), filename="chart.png")
    
    # БЖУ %
    total_p, total_f, total_carb = sum(prots), sum(fats), sum(car)
    en = total_p*4 + total_f*9 + total_carb*4
    p_pct = (total_p*4/en*100) if en else 0
    f_pct = (total_f*9/en*100) if en else 0
    c_pct = (total_carb*4/en*100) if en else 0
    
    warns = []
    if p_pct < 15:
        warns.append("⚠️ Мало белка!")
    if f_pct > 40:
        warns.append("⚠️ Много жиров!")
    advice = "\n".join(warns) if warns else "✅ Баланс ОК"
    
    cap = (f"📊 *{days} дней*\n🔥 Среднее: {total_c/days:.0f}/{goal:.0f}\n"
           f"🥩 Б: {p_pct:.1f}% | 🥑 Ж: {f_pct:.1f}% | 🍞 У: {c_pct:.1f}%\n{advice}")
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="close_profile")]])
    await message.answer_photo(photo_file, caption=cap, reply_markup=kb)

# ================= FAVORITES =================

@dp.callback_query(F.data == "show_favorites")
async def show_favs(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    rows = await db_fetchall("""
        SELECT p.id, p.name, p.calories 
        FROM favorites f 
        JOIN products p ON f.product_id = p.id 
        WHERE f.user_id = ?
    """, (user_id,))
        
    if not rows:
        await callback.answer("⭐ Избранное пусто. Добавьте продукты при поиске!", show_alert=True)
        return

    kb_builder = []
    for r in rows:
        kb_builder.append([InlineKeyboardButton(text=f"⭐ {r[1]} ({r[2]} ккал)", callback_data=f"idx_{r[0]}")])
    kb_builder.append([InlineKeyboardButton(text="🔙 Назад", callback_data="add_more_food")])
    
    await callback.message.edit_text("⭐ *Ваше избранное:*", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_builder))

@dp.callback_query(F.data.startswith("fav_add_"))
async def add_fav(callback: CallbackQuery):
    pid = int(callback.data.split("_")[2])
    try:
        await db_execute(
            "INSERT INTO favorites (user_id, product_id) VALUES (?, ?)",
            (callback.from_user.id, pid)
        )
        await callback.answer("✅ Добавлено в избранное!", show_alert=True)
    except aiosqlite.IntegrityError:
        await callback.answer("⚠️ Уже в избранном", show_alert=True)

# ================= TRACKING =================

async def track_food_start(obj, state):
    await safe_answer(obj, "Прием пищи:", reply_markup=meal_kb())
    await state.set_state(TrackFood.choosing_meal)

@dp.callback_query(F.data == "add_more_food")
async def more_food(c: CallbackQuery, state: FSMContext):
    await track_food_start(c, state)
    await c.answer()

@dp.callback_query(F.data.startswith("meal_"))
async def meal_sel(c: CallbackQuery, state: FSMContext):
    meal_key = c.data.split("_")[1]
    meal_name = MEALS.get(meal_key, "Еда")
    await state.update_data(meal_type=meal_name)
    await c.message.edit_text(f"🍽 *{meal_name}*: Введите название продукта:")
    await state.set_state(TrackFood.search)

@dp.message(TrackFood.search)
async def search_p(message: types.Message, state: FSMContext):
    q = message.text.lower().strip()
    await state.update_data(product_name_query=q)
    
    # Используем FTS5 для полнотекстового поиска
    rows = await db_fetchall("""
        SELECT p.id, p.name, p.calories 
        FROM products_fts f
        JOIN products p ON p.id = f.rowid
        WHERE products_fts MATCH ? AND p.is_approved = 1
        ORDER BY bm25(products_fts)
        LIMIT 10
    """, (q + "*",))
    
    if not rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать", callback_data="add_product")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="add_more_food")]
        ])
        await message.answer("❌ Продукт не найден в базе.", reply_markup=kb)
        return

    kb = []
    for r in rows:
        kb.append([InlineKeyboardButton(text=f"{r[1]} ({r[2]} ккал)", callback_data=f"idx_{r[0]}")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="add_more_food")])
    await message.answer("Результаты поиска:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("idx_"))
async def prod_sel(c: CallbackQuery, state: FSMContext):
    pid = int(c.data.split("_")[1])
    row = await get_product_by_id(pid)
    
    if not row:
        await c.answer("❌ Продукт не найден или удален", show_alert=True)
        await state.clear()
        return
    
    await state.update_data(product_name=row[0], calories=row[1], prod_id=pid)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ В избранное", callback_data=f"fav_add_{pid}")],
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="add_more_food")]
    ])
    
    await c.message.edit_text(f"⚖️ *{row[0]}*\n{row[1]} ккал/100г\nВведите вес (г):", reply_markup=kb)
    await state.set_state(TrackFood.weight)

@dp.message(TrackFood.weight)
async def save_food(message: types.Message, state: FSMContext):
    w = validate_float(message.text)
    if not w:
        await message.answer("Введите число!")
        return
    
    if w <= 0 or w > 5000:
        await message.answer("⚠️ Введите вес 1–5000 граммов")
        return
    
    d = await state.get_data()
    uid = message.from_user.id
    today = datetime.now().strftime('%Y-%m-%d')
    kcal = d["calories"] * w / 100
    
    await db_execute(
        "INSERT INTO consumption (user_id, product_id, weight, meal_type, date) VALUES (?,?,?,?,?)",
        (uid, d["prod_id"], w, d["meal_type"], today)
    )
    
    summ = await get_day_summary(uid, today)
    goal, _ = await get_user_settings(uid)
    
    warning_text = ""
    if summ > goal:
        warning_text = "\n\n⚠️ *ПРЕВЫШЕНИЕ НОРМЫ!* 🛑"
    
    pct = (summ/goal)*100
    txt = f"✅ +{kcal:.0f} ккал\n📊 Итог: {summ:.0f} / {goal:.0f} ({pct:.0f}%){warning_text}"
    
    await message.answer(txt, reply_markup=after_track_kb())
    await state.clear()

# ================= ADD NEW PRODUCT =================

@dp.callback_query(F.data == "add_product")
async def add_p_start(c: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.update_data(new_name=d.get('product_name_query', 'New'))
    await c.message.edit_text("Ккал на 100г:")
    await state.set_state(AddProduct.calories)

@dp.message(AddProduct.calories)
async def add_cal(m: Message, state: FSMContext):
    val = validate_float(m.text)
    if val is None:
        await m.answer("Введите число")
        return
    await state.update_data(cal=val)
    await m.answer("Белки (г):")
    await state.set_state(AddProduct.proteins)

@dp.message(AddProduct.proteins)
async def add_prot(m: Message, state: FSMContext):
    val = validate_float(m.text)
    if val is None:
        await m.answer("Введите число")
        return
    await state.update_data(prot=val)
    await m.answer("Жиры (г):")
    await state.set_state(AddProduct.fats)

@dp.message(AddProduct.fats)
async def add_fat(m: Message, state: FSMContext):
    val = validate_float(m.text)
    if val is None:
        await m.answer("Введите число")
        return
    await state.update_data(fat=val)
    await m.answer("Углеводы (г):")
    await state.set_state(AddProduct.carbs)

@dp.message(AddProduct.carbs)
async def add_carb_fin(m: Message, state: FSMContext):
    val = validate_float(m.text)
    if val is None:
        await m.answer("Введите число")
        return
    
    d = await state.get_data()
    carb = val
    
    # Валидация БЖУ vs калории
    calc_kcal = d['prot'] * 4 + d['fat'] * 9 + carb * 4
    if abs(calc_kcal - d['cal']) > 50:
        await m.answer(f"⚠️ Калории не совпадают с БЖУ!\nУказано: {d['cal']} ккал\nПо БЖУ выходит: {calc_kcal:.0f} ккал")
        return
    
    if len(d['new_name']) > 60:
        await m.answer("⚠️ Имя продукта слишком длинное (макс 60 символов)")
        return
    
    try:
        cursor = await db.execute(
            "INSERT INTO products (name, calories, proteins, fats, carbs, is_approved) VALUES (?,?,?,?,?,0)",
            (d['new_name'], d['cal'], d['prot'], d['fat'], carb)
        )
        pid = cursor.lastrowid
        await db.commit()
        
        if ADMIN_ID:
            info = f"🆕 *{d['new_name']}*\n{d['cal']} ккал\nБ{d['prot']} Ж{d['fat']} У{carb}"
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅", callback_data=f"adm_approve_{pid}"), InlineKeyboardButton(text="❌", callback_data=f"adm_reject_{pid}")]])
            try:
                await bot.send_message(ADMIN_ID, info, reply_markup=kb)
            except:
                logger.error("Не удалось отправить админу уведомление")
            
        await state.update_data(product_name=d['new_name'], calories=d['cal'], prod_id=pid)
        await m.answer("✅ Продукт добавлен. Введите вес (г):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⭐ В избранное", callback_data=f"fav_add_{pid}")]]))
        await state.set_state(TrackFood.weight)
    except aiosqlite.IntegrityError:
        await m.answer("Такой продукт уже есть.")
        await state.clear()

async def show_today_history(m: Message):
    """Показать историю приема пищи за сегодня"""
    uid = m.from_user.id
    rows = await db_fetchall(
        "SELECT meal_type, p.name, weight, p.calories FROM consumption c JOIN products p ON c.product_id=p.id WHERE user_id=? AND date=?",
        (uid, datetime.now().strftime('%Y-%m-%d'))
    )
    
    if not rows:
        await m.answer("📋 Сегодня пусто.")
    else:
        t = 0
        lines = []
        for r in rows:
            kc = r[3]*r[2]/100
            t += kc
            lines.append(f"{r[0]}: {r[1]} {r[2]:.0f}г ({kc:.0f})")
        
        goal, _ = await get_user_settings(uid)
        pct = (t/goal)*100 if goal > 0 else 0
        lines.append(f"\n*Итого: {t:.0f} / {goal:.0f} ккал ({pct:.0f}%)*")
        await m.answer("\n".join(lines))

@dp.callback_query(F.data == "to_main")
async def to_main(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.delete()
    await c.message.answer("🏠 Меню", reply_markup=main_kb(c.from_user.id))

@dp.callback_query(F.data == "pre_clear_day")
async def pre_clear(c: CallbackQuery):
    await c.message.edit_text("⚠️ Это удалит ВСЕ записи за сегодня! Уверены?", reply_markup=confirm_clear_kb())

@dp.callback_query(F.data == "perform_clear_day")
async def perform_clear(c: CallbackQuery):
    today = datetime.now().strftime('%Y-%m-%d')
    await db_execute("DELETE FROM consumption WHERE user_id=? AND date=?", (c.from_user.id, today))
    await c.message.edit_text("✅ Записи за сегодня удалены", reply_markup=profile_kb())

@dp.callback_query(F.data == "back_to_profile")
async def back_to_prof(c: CallbackQuery):
    await show_profile(c.message)

# ================= MAIN =================

async def main():
    """Главная функция запуска бота"""
    await init_connection_pool()
    await init_db()
    
    logger.info("🤖 Бот запущен (polling)")
    
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await close_connection_pool()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ Бот остановлен пользователем")