import asyncio
import logging
import random
import os
import time
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import asyncpg
from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.utils.exceptions import (
    BotBlocked, UserDeactivated, ChatNotFound, RetryAfter,
    TelegramAPIError, MessageNotModified, MessageToEditNotFound,
    TerminatedByOtherGetUpdates, ChatAdminRequired
)
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.dispatcher.handler import CancelHandler

# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан в переменных окружения")

SUPER_ADMINS_STR = os.getenv("SUPER_ADMINS", "")
SUPER_ADMINS = [int(x.strip()) for x in SUPER_ADMINS_STR.split(",") if x.strip()]

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не задан. Создай PostgreSQL базу в Railway.")

# Значения по умолчанию для настроек
DEFAULT_SETTINGS = {
    "random_attack_cost": "0",
    "targeted_attack_cost": "50",
    "theft_cooldown_minutes": "30",
    "theft_success_chance": "40",
    "theft_defense_chance": "20",
    "theft_defense_penalty": "10",
    "casino_win_chance": "30",
    "min_theft_amount": "5",
    "max_theft_amount": "15",
    "dice_multiplier": "2",
    "guess_multiplier": "5",
    "guess_reputation": "1",
    "chat_notify_big_win": "1",
    "chat_notify_big_purchase": "1",
    "chat_notify_giveaway": "1",
    "gift_amount": "30",
    "gift_limit_per_day": "3",
    "referral_bonus": "50",
    "referral_reputation": "2",
}

# Константы
ITEMS_PER_PAGE = 10
BIG_WIN_THRESHOLD = 100
BIG_PURCHASE_THRESHOLD = 100

# ===== ИНИЦИАЛИЗАЦИЯ =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

db_pool = None
settings_cache = {}
last_settings_update = 0
channels_cache = []
last_channels_update = 0
chats_cache = []
last_chats_update = 0

async def before_start():
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Webhook удалён, пропущены старые обновления")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ===== МИДЛВАРЬ ДЛЯ ЗАЩИТЫ ОТ ФЛУДА =====
class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, rate_limit=1.0):
        self.rate_limit = rate_limit
        self.user_last_time = defaultdict(float)
        super().__init__()

    async def on_process_message(self, message: types.Message, data: dict):
        if message.chat.type != 'private' or await is_admin(message.from_user.id):
            return
        user_id = message.from_user.id
        now = time.time()
        if now - self.user_last_time[user_id] < self.rate_limit:
            await message.reply("⏳ Слишком много запросов. Подожди секунду.")
            raise CancelHandler()
        self.user_last_time[user_id] = now

dp.middleware.setup(ThrottlingMiddleware(rate_limit=0.5))

# ===== БЕЗОПАСНАЯ ОТПРАВКА СООБЩЕНИЙ =====
async def safe_send_message(user_id: int, text: str, **kwargs):
    try:
        await bot.send_message(user_id, text, **kwargs)
    except BotBlocked:
        logging.warning(f"Bot blocked by user {user_id}")
    except UserDeactivated:
        logging.warning(f"User {user_id} deactivated")
    except ChatNotFound:
        logging.warning(f"Chat {user_id} not found")
    except RetryAfter as e:
        logging.warning(f"Flood limit exceeded. Retry after {e.timeout} seconds")
        await asyncio.sleep(e.timeout)
        try:
            await bot.send_message(user_id, text, **kwargs)
        except Exception as ex:
            logging.warning(f"Still failed after retry: {ex}")
    except TelegramAPIError as e:
        logging.warning(f"Telegram API error for user {user_id}: {e}")
    except Exception as e:
        logging.warning(f"Failed to send message to {user_id}: {e}")

def safe_send_message_task(user_id: int, text: str, **kwargs):
    asyncio.create_task(safe_send_message(user_id, text, **kwargs))

async def safe_send_chat(chat_id: int, text: str, **kwargs):
    try:
        await bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        logging.error(f"Failed to send to chat {chat_id}: {e}")

# ===== ПОДКЛЮЧЕНИЕ К POSTGRESQL =====
async def create_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=5,
        max_size=20,
        command_timeout=60,
        max_queries=50000,
        max_inactive_connection_lifetime=300
    )
    logging.info("Подключение к PostgreSQL установлено")

async def init_db():
    async with db_pool.acquire() as conn:
        # Пользователи
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_date TEXT,
                balance INTEGER DEFAULT 0,
                reputation INTEGER DEFAULT 0,
                total_spent INTEGER DEFAULT 0,
                negative_balance INTEGER DEFAULT 0,
                last_bonus TEXT,
                last_theft_time TEXT,
                theft_attempts INTEGER DEFAULT 0,
                theft_success INTEGER DEFAULT 0,
                theft_failed INTEGER DEFAULT 0,
                theft_protected INTEGER DEFAULT 0,
                casino_wins INTEGER DEFAULT 0,
                casino_losses INTEGER DEFAULT 0,
                guess_wins INTEGER DEFAULT 0,
                guess_losses INTEGER DEFAULT 0
            )
        ''')

        # Каналы для подписки (обязательные)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,
                chat_id TEXT UNIQUE,
                title TEXT,
                invite_link TEXT
            )
        ''')

        # Чаты, куда добавлен бот (для уведомлений и подгонов)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                chat_id BIGINT PRIMARY KEY,
                title TEXT,
                type TEXT,
                joined_date TEXT,
                notify_enabled BOOLEAN DEFAULT TRUE,
                last_gift_date DATE,
                gift_count_today INTEGER DEFAULT 0
            )
        ''')

        # Реферальная система
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                id SERIAL PRIMARY KEY,
                referrer_id BIGINT,
                referred_id BIGINT UNIQUE,
                referred_date TEXT,
                reward_given BOOLEAN DEFAULT FALSE
            )
        ''')

        # Товары магазина
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS shop_items (
                id SERIAL PRIMARY KEY,
                name TEXT,
                description TEXT,
                price INTEGER,
                stock INTEGER DEFAULT -1
            )
        ''')

        # Покупки
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                item_id INTEGER,
                purchase_date TEXT,
                status TEXT DEFAULT 'pending',
                admin_comment TEXT
            )
        ''')

        # Промокоды
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                reward INTEGER,
                max_uses INTEGER,
                used_count INTEGER DEFAULT 0
            )
        ''')

        # Активации промокодов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS promo_activations (
                user_id BIGINT,
                promo_code TEXT,
                activated_at TEXT,
                PRIMARY KEY (user_id, promo_code)
            )
        ''')

        # Розыгрыши
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS giveaways (
                id SERIAL PRIMARY KEY,
                prize TEXT,
                description TEXT,
                end_date TEXT,
                media_file_id TEXT,
                media_type TEXT,
                status TEXT DEFAULT 'active',
                winner_id BIGINT,
                winners_count INTEGER DEFAULT 1,
                notified BOOLEAN DEFAULT FALSE
            )
        ''')

        # Участники розыгрышей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS participants (
                user_id BIGINT,
                giveaway_id INTEGER,
                PRIMARY KEY (user_id, giveaway_id)
            )
        ''')

        # Младшие админы
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                added_by BIGINT,
                added_date TEXT
            )
        ''')

        # Заблокированные пользователи
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id BIGINT PRIMARY KEY,
                banned_by BIGINT,
                banned_date TEXT,
                reason TEXT
            )
        ''')

        # Настройки игры
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        # Задания
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                name TEXT,
                description TEXT,
                task_type TEXT,
                target_id TEXT,
                reward_coins INTEGER DEFAULT 0,
                reward_reputation INTEGER DEFAULT 0,
                required_days INTEGER DEFAULT 0,
                penalty_days INTEGER DEFAULT 0,
                created_by BIGINT,
                created_at TEXT,
                active BOOLEAN DEFAULT TRUE
            )
        ''')

                # Выполненные задания
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_tasks (
                user_id BIGINT,
                task_id INTEGER,
                completed_at TEXT,
                expires_at TEXT,
                status TEXT DEFAULT 'completed',
                PRIMARY KEY (user_id, task_id)
            )
        ''')

        # Мультиплеерные игры
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS multiplayer_games (
                game_id TEXT PRIMARY KEY,
                host_id BIGINT,
                max_players INTEGER,
                bet_amount INTEGER,
                status TEXT DEFAULT 'waiting',
                deck TEXT,
                created_at TEXT
            )
        ''')

        await conn.execute('''
            CREATE TABLE IF NOT EXISTS game_players (
                game_id TEXT,
                user_id BIGINT,
                username TEXT,
                cards TEXT,
                value INTEGER DEFAULT 0,
                stopped BOOLEAN DEFAULT FALSE,
                joined_at TEXT,
                PRIMARY KEY (game_id, user_id)
            )
        ''')

        # Добавляем поле game_wins в users (если нет)
        await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS game_wins INTEGER DEFAULT 0')

        # Индексы
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_balance ON users(balance DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_reputation ON users(reputation DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_total_spent ON users(total_spent DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_user_id ON purchases(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_status ON purchases(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_giveaways_status ON giveaways(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_promo_activations_user ON promo_activations(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_tasks_expires ON user_tasks(expires_at)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_active ON tasks(active)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")

        # Индексы
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_balance ON users(balance DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_reputation ON users(reputation DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_total_spent ON users(total_spent DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_user_id ON purchases(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_status ON purchases(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_giveaways_status ON giveaways(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_promo_activations_user ON promo_activations(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_tasks_expires ON user_tasks(expires_at)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_active ON tasks(active)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")

    await create_default_items()
    await init_settings()
    logging.info("Таблицы в PostgreSQL созданы/проверены")

async def create_default_items():
    default_items = [
        ("🎁 Цветы", "Красивый букет", 50, 10),
        ("🎁 Конфеты", "Коробка шоколадных конфет", 30, 10),
        ("🎁 Игрушка", "Мягкая игрушка", 70, 5),
    ]
    async with db_pool.acquire() as conn:
        for name, desc, price, stock in default_items:
            exists = await conn.fetchval("SELECT id FROM shop_items WHERE name=$1", name)
            if not exists:
                await conn.execute(
                    "INSERT INTO shop_items (name, description, price, stock) VALUES ($1, $2, $3, $4)",
                    name, desc, price, stock
                )

async def init_settings():
    async with db_pool.acquire() as conn:
        for key, value in DEFAULT_SETTINGS.items():
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                key, value
            )

async def get_setting(key: str) -> str:
    global settings_cache, last_settings_update
    now = time.time()
    if now - last_settings_update > 60 or not settings_cache:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM settings")
            settings_cache = {row['key']: row['value'] for row in rows}
        last_settings_update = now
    return settings_cache.get(key, DEFAULT_SETTINGS[key])

async def set_setting(key: str, value: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE settings SET value=$1 WHERE key=$2", value, key)
    settings_cache[key] = value

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
async def is_super_admin(user_id: int) -> bool:
    return user_id in SUPER_ADMINS

async def is_junior_admin(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchval("SELECT user_id FROM admins WHERE user_id=$1", user_id)
    return row is not None

async def is_admin(user_id: int) -> bool:
    return await is_super_admin(user_id) or await is_junior_admin(user_id)

async def is_banned(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchval("SELECT user_id FROM banned_users WHERE user_id=$1", user_id)
    return row is not None

async def get_channels():
    global channels_cache, last_channels_update
    now = time.time()
    if now - last_channels_update > 300 or not channels_cache:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id, title, invite_link FROM channels")
            channels_cache = [(r['chat_id'], r['title'], r['invite_link']) for r in rows]
        last_channels_update = now
    return channels_cache

async def get_chats():
    global chats_cache, last_chats_update
    now = time.time()
    if now - last_chats_update > 300 or not chats_cache:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id, notify_enabled FROM chats")
            chats_cache = [(r['chat_id'], r['notify_enabled']) for r in rows]
        last_chats_update = now
    return chats_cache

async def check_subscription(user_id: int):
    channels = await get_channels()
    if not channels:
        return True, []
    not_subscribed = []
    for chat_id, title, link in channels:
        try:
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status in ['left', 'kicked']:
                not_subscribed.append((title, link))
        except Exception:
            not_subscribed.append((title, link))
    return len(not_subscribed) == 0, not_subscribed

async def get_user_balance(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
        return balance if balance is not None else 0

async def update_user_balance(user_id: int, delta: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT balance, negative_balance FROM users WHERE user_id=$1", user_id)
        if not row:
            return
        balance, negative = row['balance'], row['negative_balance']
        new_balance = balance + delta
        if new_balance < 0:
            negative += abs(new_balance)
            new_balance = 0
        await conn.execute(
            "UPDATE users SET balance=$1, negative_balance=$2 WHERE user_id=$3",
            new_balance, negative, user_id
        )

async def get_user_reputation(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        rep = await conn.fetchval("SELECT reputation FROM users WHERE user_id=$1", user_id)
        return rep if rep is not None else 0

async def update_user_reputation(user_id: int, delta: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET reputation = reputation + $1 WHERE user_id=$2", delta, user_id)

async def update_user_total_spent(user_id: int, amount: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET total_spent = total_spent + $1 WHERE user_id=$2", amount, user_id)

async def get_random_user(exclude_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT user_id FROM users 
            WHERE user_id != $1 AND user_id NOT IN (SELECT user_id FROM banned_users)
            ORDER BY RANDOM() LIMIT 1
        """, exclude_id)
        return row['user_id'] if row else None

async def find_user_by_input(input_str: str) -> Optional[Dict]:
    input_str = input_str.strip()
    try:
        uid = int(input_str)
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", uid)
            return dict(row) if row else None
    except ValueError:
        username = input_str.lower()
        if username.startswith('@'):
            username = username[1:]
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE LOWER(username)=$1", username)
            return dict(row) if row else None

async def notify_chats(message_text: str, importance: str = 'info'):
    chats = await get_chats()
    for chat_id, enabled in chats:
        if not enabled:
            continue
        await safe_send_chat(chat_id, message_text)

# ===== ВСТАВЬ СЮДА =====
import string

def generate_game_id():
    """Генерирует уникальный код комнаты из 6 символов"""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

def calculate_hand_value(cards):
    """Вычисляет сумму очков для списка карт (карты в формате '10♠', 'A♥' и т.д.)"""
    value = 0
    aces = 0
    for card in cards:
        rank = card[:-1]
        if rank in ['J', 'Q', 'K']:
            value += 10
        elif rank == 'A':
            aces += 1
            value += 11
        else:
            value += int(rank)
    while value > 21 and aces:
        value -= 10
        aces -= 1
    return value

def create_deck():
    """Создаёт перемешанную колоду из 52 карт"""
    suits = ['♠', '♥', '♦', '♣']
    ranks = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
    deck = [f"{rank}{suit}" for suit in suits for rank in ranks]
    random.shuffle(deck)
    return deck
# ===== КОНЕЦ ВСТАВКИ =====

# ===== СОСТОЯНИЯ FSM =====
class CreateGiveaway(StatesGroup):
    prize = State()
    description = State()
    end_date = State()
    media = State()

class AddChannel(StatesGroup):
    chat_id = State()
    title = State()
    invite_link = State()

class RemoveChannel(StatesGroup):
    chat_id = State()

class AddShopItem(StatesGroup):
    name = State()
    description = State()
    price = State()
    stock = State()

class RemoveShopItem(StatesGroup):
    item_id = State()

class EditShopItem(StatesGroup):
    item_id = State()
    field = State()
    value = State()

class CreatePromocode(StatesGroup):
    code = State()
    reward = State()
    max_uses = State()

class Broadcast(StatesGroup):
    media = State()

class AddBalance(StatesGroup):
    user_id = State()
    amount = State()

class RemoveBalance(StatesGroup):
    user_id = State()
    amount = State()

class CasinoBet(StatesGroup):
    amount = State()

class DiceBet(StatesGroup):
    amount = State()

class GuessBet(StatesGroup):
    amount = State()
    number = State()

class PromoActivate(StatesGroup):
    code = State()

class TheftTarget(StatesGroup):
    target = State()

class FindUser(StatesGroup):
    query = State()

class AddJuniorAdmin(StatesGroup):
    user_id = State()

class RemoveJuniorAdmin(StatesGroup):
    user_id = State()

class CompleteGiveaway(StatesGroup):
    giveaway_id = State()
    winners_count = State()

class BlockUser(StatesGroup):
    user_id = State()
    reason = State()

class UnblockUser(StatesGroup):
    user_id = State()

class EditSettings(StatesGroup):
    key = State()
    value = State()

class CreateTask(StatesGroup):
    name = State()
    description = State()
    task_type = State()
    target_id = State()
    reward_coins = State()
    reward_reputation = State()
    required_days = State()
    penalty_days = State()

class TakeTask(StatesGroup):
    task_id = State()

class DeleteTask(StatesGroup):
    task_id = State()
class MultiplayerGame(StatesGroup):
    create_max_players = State()
    create_bet = State()
    join_code = State()

class RoomChat(StatesGroup):
    message = State()
# ===== КЛАВИАТУРЫ =====
def subscription_inline(not_subscribed):
    kb = []
    for title, link in not_subscribed:
        if link:
            kb.append([InlineKeyboardButton(text=f"📢 {title}", url=link)])
        else:
            kb.append([InlineKeyboardButton(text=f"📢 {title}", callback_data="no_link")])
    kb.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(row_width=1, inline_keyboard=kb)

def user_main_keyboard(is_admin_user=False):
    buttons = [
        [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="🎁 Бонус")],
        [KeyboardButton(text="🛒 Магазин подарков"), KeyboardButton(text="🎰 Казино")],
        [KeyboardButton(text="🎟 Промокод"), KeyboardButton(text="🏆 Топ игроков")],
        [KeyboardButton(text="💰 Мои покупки"), KeyboardButton(text="🔫 Ограбить")],
        [KeyboardButton(text="🎲 Игры"), KeyboardButton(text="⭐️ Репутация")],
        [KeyboardButton(text="📋 Задания"), KeyboardButton(text="🔗 Рефералка")],
    ]
    if is_admin_user:
        buttons.append([KeyboardButton(text="⚙️ Админ панель")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def theft_choice_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎲 Случайная цель")],
        [KeyboardButton(text="👤 Выбрать пользователя")],
        [KeyboardButton(text="◀️ Назад")]
    ], resize_keyboard=True)

# ===== НОВЫЕ КЛАВИАТУРЫ ДЛЯ МУЛЬТИПЛЕЕРА =====
def room_menu_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📋 Список комнат")],
        [KeyboardButton(text="🎮 Создать комнату")],
        [KeyboardButton(text="ℹ️ Правила игры")],
        [KeyboardButton(text="🏆 Топ игроков")],
        [KeyboardButton(text="◀️ Назад в игры")]
    ], resize_keyboard=True)

def room_control_keyboard(game_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Начать игру", callback_data=f"start_game_{game_id}")],
        [InlineKeyboardButton(text="❌ Закрыть комнату", callback_data=f"close_room_{game_id}")]
    ])

def room_action_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Ещё", callback_data="room_hit"),
         InlineKeyboardButton(text="🛑 Хватит", callback_data="room_stand")],
        [InlineKeyboardButton(text="🏳️ Сдаться", callback_data="room_surrender")]
    ])

def leave_room_keyboard(game_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚪 Выйти из комнаты", callback_data=f"leave_room_{game_id}")]
    ])

def admin_main_keyboard(is_super):
    buttons = [
        [KeyboardButton(text="🎁 Управление розыгрышами")],
        [KeyboardButton(text="📢 Рассылка"), KeyboardButton(text="💰 Начислить монеты")],
        [KeyboardButton(text="📺 Управление каналами")],
        [KeyboardButton(text="🛒 Управление магазином")],
        [KeyboardButton(text="🎫 Управление промокодами")],
        [KeyboardButton(text="📋 Управление заданиями")],
        [KeyboardButton(text="⚙️ Настройки игры")],
        [KeyboardButton(text="🧹 Очистить старые записи")],
        [KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="👥 Найти пользователя")],
        [KeyboardButton(text="🛍️ Список покупок")],
        [KeyboardButton(text="🔨 Заблокировать пользователя")],
        [KeyboardButton(text="🔓 Разблокировать пользователя")],
        [KeyboardButton(text="💸 Списать монеты")],
    ]
    if is_super:
        buttons.append([KeyboardButton(text="➕ Добавить админа")])
        buttons.append([KeyboardButton(text="➖ Удалить админа")])
        buttons.append([KeyboardButton(text="🔄 Сброс статистики")])
    buttons.append([KeyboardButton(text="◀️ Назад в главное меню")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def task_admin_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Создать задание")],
        [KeyboardButton(text="📋 Список заданий")],
        [KeyboardButton(text="❌ Удалить задание")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def settings_reply_keyboard():
    buttons = [
        [KeyboardButton(text="💰 Стоимость случайной кражи")],
        [KeyboardButton(text="👤 Стоимость кражи по username")],
        [KeyboardButton(text="⏱ Кулдаун (минут)")],
        [KeyboardButton(text="🎲 Шанс успеха %")],
        [KeyboardButton(text="🛡 Шанс защиты %")],
        [KeyboardButton(text="💥 Штраф при защите")],
        [KeyboardButton(text="🎰 Шанс казино %")],
        [KeyboardButton(text="💰 Мин. сумма кражи")],
        [KeyboardButton(text="💰 Макс. сумма кражи")],
        [KeyboardButton(text="🎲 Множитель костей")],
        [KeyboardButton(text="🔢 Множитель угадайки")],
        [KeyboardButton(text="⭐️ Репутация за угадайку")],
        [KeyboardButton(text="📢 Уведомления в чатах")],
        [KeyboardButton(text="💰 Сумма подарка в чате")],
        [KeyboardButton(text="📊 Лимит подарков в день")],
        [KeyboardButton(text="👥 Реферальный бонус (монеты)")],
        [KeyboardButton(text="⭐️ Реферальный бонус (репутация)")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def giveaway_admin_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Создать розыгрыш")],
        [KeyboardButton(text="📋 Активные розыгрыши")],
        [KeyboardButton(text="✅ Завершить розыгрыш")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def channel_admin_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить канал")],
        [KeyboardButton(text="➖ Удалить канал")],
        [KeyboardButton(text="📋 Список каналов")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def shop_admin_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить товар")],
        [KeyboardButton(text="➖ Удалить товар")],
        [KeyboardButton(text="✏️ Редактировать товар")],
        [KeyboardButton(text="📋 Список товаров")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def promo_admin_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Создать промокод")],
        [KeyboardButton(text="📋 Список промокодов")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def back_keyboard():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="◀️ Назад")]], resize_keyboard=True)

def purchase_action_keyboard(purchase_id):
    return InlineKeyboardMarkup(row_width=2, inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выполнено", callback_data=f"purchase_done_{purchase_id}"),
         InlineKeyboardButton(text="❌ Отказ", callback_data=f"purchase_reject_{purchase_id}")]
    ])

# ===== ТЕКСТОВЫЕ ФРАЗЫ =====
BONUS_PHRASES = [
    "🎉 Красава, лови +{bonus} монет!",
    "💰 Зашкварно богатенький стал! +{bonus}",
    "🌟 Хайпанули? +{bonus} монет в карман!",
    "🍀 Удача крашеная, держи +{bonus}",
    "🎁 Ты в тренде, +{bonus} монет!"
]

CASINO_WIN_PHRASES = [
    "🎰 Краш! Ты выиграл {win} монет (чистыми {profit})!",
    "🍒 Хайповая комбинация! +{profit} монет!",
    "💫 Фортуна крашеная, твой выигрыш: {win} монет!",
    "🎲 Изи-катка, {profit} монет твои!",
    "✨ Ты красавчик, обыграл казино! +{profit} монет!"
]

CASINO_LOSE_PHRASES = [
    "😢 Обидно, потерял {loss} монет.",
    "💔 Зашкварно, минус {loss}.",
    "📉 Не фортануло, -{loss} монет.",
    "🍂 В следующий раз краш будет твоим, а пока -{loss}.",
    "⚡️ Лузернулся на {loss} монет."
]

PURCHASE_PHRASES = [
    "✅ Купил! Админ скоро в личку прилетит.",
    "🛒 Товар твой! Жди админа, бро.",
    "🎁 Крутая покупка! Админ уже в курсе.",
    "💎 Ты краш! Админ свяжется."
]

THEFT_CHOICE_PHRASES = [
    "🔫 Выбери, как хочешь напасть:",
    "💢 Кого будем грабить?",
    "😈 Куда направим бандитские лапы?"
]

THEFT_COOLDOWN_PHRASES = [
    "⏳ Ты ещё не остыл после прошлого налёта. Подожди {minutes} мин.",
    "🕐 Полегче, ковбой! Отдохни {minutes} минут.",
    "😴 Грабить так часто – плохая примета. Возвращайся через {minutes} мин."
]

THEFT_NO_MONEY_PHRASES = [
    "😕 У тебя нет монет даже на подготовку к краже!",
    "💸 Сначала заработай, потом грабить будешь.",
    "💰 Пустой карман – не до криминала."
]

THEFT_SUCCESS_PHRASES = [
    "🔫 Красава! Ты украл {amount} монет у {target}!",
    "💰 Хайпанул, {amount} монет у {target} теперь твои!",
    "🦹‍♂️ Удачная кража! +{amount} от {target}",
    "😈 Ты краш, {target} даже не понял! +{amount}"
]

THEFT_FAIL_PHRASES = [
    "😢 Облом, тебя спалили! Ничего не украл.",
    "🚨 Треск, {target} оказался слишком бдительным!",
    "👮‍♂️ Пришлось сваливать, 0 монет.",
    "💔 Не фортануло, {target} слишком крутой."
]

THEFT_DEFENSE_PHRASES = [
    "🛡️ {target} отразил атаку! Ты потерял {penalty} монет.",
    "💥 Бабах! {target} выставил защиту, и ты лишился {penalty} монет.",
    "😱 Засада! Ты напоролся на защиту и потерял {penalty} монет."
]

THEFT_VICTIM_DEFENSE_PHRASES = [
    "🛡️ Твоя защита сработала! {attacker} ничего не украл и потерял {penalty} монет.",
    "💪 Ты краш! Отбил атаку {attacker} и получил {penalty} монет.",
    "😎 Ха! {attacker} думал поживиться, а сам потерял {penalty} монет."
]

DICE_WIN_PHRASES = [
    "🎲 {dice1} + {dice2} = {total} — Победа! +{profit} монет!",
    "🎲 Круто! {dice1}+{dice2}={total}, ты выиграл {profit}!",
    "🎲 Хайп! {total} очков, твой выигрыш: {profit}!"
]

DICE_LOSE_PHRASES = [
    "🎲 {dice1} + {dice2} = {total} — Проигрыш. -{loss} монет.",
    "🎲 Эх, {total} очков, не повезло. -{loss}.",
    "🎲 В этот раз не зашло, -{loss} монет."
]

GUESS_WIN_PHRASES = [
    "🔢 Ты угадал! Было {secret}. Выигрыш: +{profit} монет и +{rep} репутации!",
    "🔢 Красава! Число {secret}, твой выигрыш {profit} монет!",
    "🔢 Хайпанул! +{profit} монет, репутация +{rep}!"
]

GUESS_LOSE_PHRASES = [
    "🔢 Не угадал. Было {secret}. -{loss} монет.",
    "🔢 Увы, загадано {secret}. Теряешь {loss} монет.",
    "🔢 Не фортануло, правильный ответ {secret}. -{loss}."
]

CHAT_WIN_PHRASES = [
    "🔥 {name} только что выиграл {amount} монет в казино!",
    "💰 Удача на стороне {name}: +{amount} монет!",
    "🎰 {name} сорвал куш — {amount} монет!"
]

CHAT_PURCHASE_PHRASES = [
    "🛒 {name} купил {item} за {price} монет!",
    "🎁 {name} приобрёл {item}! Админ уже в пути.",
    "💎 {name} потратил {price} монет на {item}!"
]

CHAT_GIVEAWAY_PHRASES = [
    "🎁 Не пропусти розыгрыш! Осталось {time}",
    "⏰ Напоминание: розыгрыш {prize} заканчивается через {time}",
    "🔥 Участвуй в розыгрыше {prize}! Осталось {time}"
]
# ===== КОМАНДА HELP =====
@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    text = (
        "🤖 <b>Malboro GAME</b> – помощь:\n\n"
        "• 👤 Профиль – баланс и статистика\n"
        "• 🎁 Бонус – ежедневная награда\n"
        "• 🛒 Магазин подарков – покупка подарков\n"
        "• 🎰 Казино – испытай удачу\n"
        "• 🎟 Промокод – активация промокодов\n"
        "• 🏆 Топ игроков – лучшие по балансу\n"
        "• 💰 Мои покупки – история заказов\n"
        "• 🔫 Ограбить – укради монеты у другого\n"
        "• 🎲 Игры – кости и угадай число\n"
        "• ⭐️ Репутация – твой авторитет\n"
        "• 📋 Задания – выполняй и получай награды\n"
        "• 🔗 Рефералка – приглашай друзей и получай бонусы\n\n"
        "Администраторы имеют дополнительные функции в панели."
    )
    await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))

# ===== СТАРТ =====
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    if message.chat.type != 'private':
        chat = message.chat
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO chats (chat_id, title, type, joined_date) VALUES ($1, $2, $3, $4) ON CONFLICT (chat_id) DO NOTHING",
                chat.id, chat.title, chat.type, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        await message.answer("✅ Бот активирован в этом чате! Теперь я буду присылать уведомления о крупных событиях. Также можно использовать команду '🎁 Подгон' (до 3 раз в день) для случайного подарка участнику.")
        return

    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await message.answer("⛔ Вы заблокированы.")
        return

    args = message.get_args()
    if args and args.startswith('ref'):
        try:
            referrer_id = int(args[3:])
            if referrer_id != user_id and not await is_banned(referrer_id):
                async with db_pool.acquire() as conn:
                    existing = await conn.fetchval("SELECT 1 FROM referrals WHERE referred_id=$1", user_id)
                    if not existing:
                        await conn.execute(
                            "INSERT INTO referrals (referrer_id, referred_id, referred_date, reward_given) VALUES ($1, $2, $3, $4)",
                            referrer_id, user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), False
                        )
                        await safe_send_message(referrer_id, f"🔗 Новый пользователь {message.from_user.first_name} зарегистрировался по вашей ссылке! Награда будет выдана после того, как он совершит 15 успешных ограблений.")
        except:
            pass

    username = message.from_user.username
    first_name = message.from_user.first_name
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (user_id, username, first_name, joined_date, balance, reputation, total_spent, negative_balance) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (user_id) DO NOTHING",
                user_id, username, first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 0, 0, 0, 0
            )
    except Exception as e:
        logging.error(f"DB error in start: {e}")
        await message.answer("❌ Ошибка базы данных. Попробуй позже.")
        return

    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer(
            "❗️ Для доступа к боту нужно подписаться на наши каналы.\nПосле подписки нажми кнопку ниже.",
            reply_markup=subscription_inline(not_subscribed)
        )
        return
    admin_flag = await is_admin(user_id)
    await message.answer(
        f"Привет, {first_name}!\n"
        f"Добро пожаловать в <b>Malboro GAME</b>! 🚬\n"
        f"Тут ты найдёшь: казино, розыгрыши, магазин с подарками.\n"
        f"А ещё можешь грабить других (раз в 30 мин) – случайно или по username!\n\n"
        f"Канал: @lllMALBOROlll (подпишись, чтобы быть в теме)",
        reply_markup=user_main_keyboard(admin_flag)
    )

# ===== ПРОВЕРКА ПОДПИСКИ =====
@dp.callback_query_handler(lambda c: c.data == "check_sub")
async def check_sub_callback(callback: types.CallbackQuery):
    if callback.message.chat.type != 'private':
        await callback.answer("Эта функция работает только в личке", show_alert=True)
        return
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    ok, not_subscribed = await check_subscription(callback.from_user.id)
    if ok:
        admin_flag = await is_admin(callback.from_user.id)
        await callback.message.edit_text("✅ Подписка подтверждена! Добро пожаловать.")
        await callback.message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))
    else:
        await callback.answer("❌ Ты ещё не подписался на все каналы!", show_alert=True)
        await callback.message.edit_reply_markup(reply_markup=subscription_inline(not_subscribed))

@dp.callback_query_handler(lambda c: c.data == "no_link")
async def no_link(callback: types.CallbackQuery):
    await callback.answer("Ссылка временно недоступна, найди канал вручную", show_alert=True)

# ===== ПРОФИЛЬ =====
@dp.message_handler(lambda message: message.text == "👤 Профиль")
async def profile_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT balance, reputation, total_spent, negative_balance, joined_date, theft_attempts, theft_success, theft_failed, theft_protected, casino_wins, casino_losses, guess_wins, guess_losses FROM users WHERE user_id=$1",
                user_id
            )
        if row:
            balance, rep, spent, neg, joined, attempts, success, failed, protected, cw, cl, gw, gl = row['balance'], row['reputation'], row['total_spent'], row['negative_balance'], row['joined_date'], row['theft_attempts'], row['theft_success'], row['theft_failed'], row['theft_protected'], row['casino_wins'], row['casino_losses'], row['guess_wins'], row['guess_losses']
            neg_text = f" (долг: {neg})" if neg > 0 else ""
            text = (
                f"👤 Твой профиль:\n"
                f"💰 Баланс: {balance} монет{neg_text}\n"
                f"⭐️ Репутация: {rep}\n"
                f"💸 Всего потрачено: {spent} монет\n"
                f"📅 Зарегистрирован: {joined}\n"
                f"🔫 Ограблений: {attempts} (успешно: {success}, провал: {failed})\n"
                f"⚔️ Отбито атак: {protected}\n"
                f"🎰 Казино: побед {cw}, поражений {cl}\n"
                f"🔢 Угадайка: побед {gw}, поражений {gl}"
            )
        else:
            text = "Профиль не найден"
    except Exception as e:
        logging.error(f"Profile error: {e}")
        text = "❌ Ошибка загрузки профиля."
    await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))

# ===== РЕПУТАЦИЯ =====
@dp.message_handler(lambda message: message.text == "⭐️ Репутация")
async def reputation_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    rep = await get_user_reputation(user_id)
    await message.answer(f"⭐️ Твоя репутация: {rep}\n\nРепутация даёт статус, но не влияет на баланс. Зарабатывай её в играх!", reply_markup=user_main_keyboard(await is_admin(user_id)))

# ===== БОНУС =====
@dp.message_handler(lambda message: message.text == "🎁 Бонус")
async def bonus_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with db_pool.acquire() as conn:
            last_bonus_str = await conn.fetchval("SELECT last_bonus FROM users WHERE user_id=$1", user_id)

        now = datetime.now()
        if last_bonus_str:
            last_bonus = datetime.strptime(last_bonus_str, "%Y-%m-%d %H:%M:%S")
            if now - last_bonus < timedelta(days=1):
                remaining = timedelta(days=1) - (now - last_bonus)
                hours = remaining.seconds // 3600
                minutes = (remaining.seconds // 60) % 60
                await message.answer(f"⏳ Бонус можно будет получить через {hours} ч {minutes} мин")
                return

        bonus = random.randint(5, 15)
        phrase = random.choice(BONUS_PHRASES).format(bonus=bonus)

        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance + $1, last_bonus = $2 WHERE user_id=$3",
                bonus, now.strftime("%Y-%m-%d %H:%M:%S"), user_id
            )
        await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        logging.error(f"Bonus error: {e}")
        await message.answer("❌ Ошибка при получении бонуса.")

# ===== ТОП ИГРОКОВ =====
@dp.message_handler(lambda message: message.text == "🏆 Топ игроков")
async def leaderboard_menu(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="💰 Самые богатые")],
        [KeyboardButton(text="💸 Транжиры")],
        [KeyboardButton(text="🔫 Крадуны")],
        [KeyboardButton(text="⭐️ По репутации")],
        [KeyboardButton(text="◀️ Назад")]
    ], resize_keyboard=True)
    await message.answer("Выбери категорию топа:", reply_markup=kb)

@dp.message_handler(lambda message: message.text.lower() == "malboro top" and message.chat.type != 'private')
async def chat_top_handler(message: types.Message):
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT first_name, balance FROM users ORDER BY balance DESC LIMIT 10")
        if not rows:
            await message.answer("Пока нет данных для топа.")
            return
        text = "🏆 <b>Топ 10 богачей:</b>\n\n"
        for idx, row in enumerate(rows, 1):
            text += f"{idx}. {row['first_name']} – {row['balance']} монет\n"
        await message.answer(text)
    except Exception as e:
        logging.error(f"Chat top error: {e}")
        await message.answer("❌ Ошибка загрузки топа.")

@dp.message_handler(lambda message: message.text == "💰 Самые богатые")
async def top_rich_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "balance", "💰 Самые богатые")

@dp.message_handler(lambda message: message.text == "💸 Транжиры")
async def top_spenders_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "total_spent", "💸 Транжиры (по потраченным монетам)")

@dp.message_handler(lambda message: message.text == "🔫 Крадуны")
async def top_thieves_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "theft_success", "🔫 Крадуны (успешные ограбления)")

@dp.message_handler(lambda message: message.text == "⭐️ По репутации")
async def top_reputation_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "reputation", "⭐️ По репутации")

async def show_top(message: types.Message, order_field: str, title: str):
    user_id = message.from_user.id
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval(f"SELECT COUNT(*) FROM users")
            rows = await conn.fetch(
                f"SELECT first_name, {order_field} FROM users ORDER BY {order_field} DESC LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("Нет данных.")
            return
        text = f"{title} (страница {page}):\n\n"
        for idx, row in enumerate(rows, start=offset+1):
            value = row[order_field]
            text += f"{idx}. {row['first_name']} – {value}\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"top_{order_field}_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"top_{order_field}_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text)
    except Exception as e:
        logging.error(f"Top error: {e}")
        await message.answer("❌ Ошибка загрузки топа.")

@dp.callback_query_handler(lambda c: c.data.startswith("top_"))
async def top_page_callback(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    field = parts[1]
    page = int(parts[2])
    titles = {
        "balance": "💰 Самые богатые",
        "total_spent": "💸 Транжиры",
        "theft_success": "🔫 Крадуны",
        "reputation": "⭐️ По репутации"
    }
    title = titles.get(field, "Топ")
    callback.message.text = f"{title} {page}"
    await show_top(callback.message, field, title)
    await callback.answer()

# ===== МАГАЗИН ПОДАРКОВ =====
@dp.message_handler(lambda message: message.text == "🛒 Магазин подарков")
async def shop_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM shop_items")
            rows = await conn.fetch(
                "SELECT id, name, description, price, stock FROM shop_items ORDER BY id LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("🎁 В магазине пока нет подарков.")
            return
        text = f"🎁 Подарки (страница {page}):\n\n"
        kb = []
        for row in rows:
            item_id = row['id']
            name = row['name']
            desc = row['description']
            price = row['price']
            stock = row['stock']
            stock_info = f" (в наличии: {stock})" if stock != -1 else ""
            text += f"🔹 {name}\n{desc}\n💰 {price} монет{stock_info}\n\n"
            kb.append([InlineKeyboardButton(text=f"Купить {name}", callback_data=f"buy_{item_id}")])
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"shop_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"shop_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e:
        logging.error(f"Shop error: {e}")
        await message.answer("❌ Ошибка загрузки магазина.")

@dp.callback_query_handler(lambda c: c.data.startswith("shop_page_"))
async def shop_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"🛒 Магазин подарков {page}"
    await shop_handler(callback.message)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("buy_"))
async def buy_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await callback.message.edit_text("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    item_id = int(callback.data.split("_")[1])
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT name, price, stock FROM shop_items WHERE id=$1", item_id)
            if not row:
                await callback.answer("Товар не найден", show_alert=True)
                return
            name, price, stock = row['name'], row['price'], row['stock']
            if stock != -1 and stock <= 0:
                await callback.answer("Товара нет в наличии!", show_alert=True)
                return
            balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
            if balance is None:
                await callback.answer("Пользователь не найден", show_alert=True)
                return
            if balance < price:
                await callback.answer("Не хватает монет!", show_alert=True)
                return
            async with conn.transaction():
                await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", price, user_id)
                await conn.execute("UPDATE users SET total_spent = total_spent + $1 WHERE user_id=$2", price, user_id)
                await conn.execute(
                    "INSERT INTO purchases (user_id, item_id, purchase_date) VALUES ($1, $2, $3)",
                    user_id, item_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
                if stock != -1:
                    await conn.execute("UPDATE shop_items SET stock = stock - 1 WHERE id=$1", item_id)

        phrase = random.choice(PURCHASE_PHRASES)
        await callback.answer(f"✅ Ты купил {name}! {phrase}", show_alert=True)

        if price >= BIG_PURCHASE_THRESHOLD:
            user = callback.from_user
            chat_phrase = random.choice(CHAT_PURCHASE_PHRASES).format(name=user.first_name, item=name, price=price)
            await notify_chats(chat_phrase, 'purchase')

        asyncio.create_task(notify_admins_about_purchase(callback.from_user, name, price))
        try:
            await callback.message.edit_text(f"✅ Покупка совершена!")
        except (MessageNotModified, MessageToEditNotFound):
            pass
        await callback.message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        logging.error(f"Purchase error: {e}")
        await callback.answer("❌ Ошибка при покупке. Попробуй позже.", show_alert=True)

async def notify_admins_about_purchase(user: types.User, item_name: str, price: int):
    admins = SUPER_ADMINS.copy()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM admins")
        for row in rows:
            admins.append(row['user_id'])
    for admin_id in admins:
        await safe_send_message(admin_id,
            f"🛒 Покупка: пользователь {user.full_name} (@{user.username})\n"
            f"<a href=\"tg://user?id={user.id}\">Ссылка</a> купил {item_name} за {price} монет."
        )

# ===== МОИ ПОКУПКИ =====
@dp.message_handler(lambda message: message.text == "💰 Мои покупки")
async def my_purchases(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM purchases WHERE user_id=$1", user_id)
            rows = await conn.fetch(
                "SELECT p.id, s.name, p.purchase_date, p.status, p.admin_comment FROM purchases p "
                "JOIN shop_items s ON p.item_id = s.id WHERE p.user_id=$1 ORDER BY p.purchase_date DESC LIMIT $2 OFFSET $3",
                user_id, ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("У тебя пока нет покупок.", reply_markup=user_main_keyboard(await is_admin(user_id)))
            return
        text = f"📦 Твои покупки (страница {page}):\n"
        for row in rows:
            pid, name, date, status, comment = row['id'], row['name'], row['purchase_date'], row['status'], row['admin_comment']
            status_emoji = "⏳" if status == 'pending' else "✅" if status == 'completed' else "❌"
            text += f"{status_emoji} {name} от {date}\n"
            if comment:
                text += f"   Комментарий: {comment}\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"mypurchases_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"mypurchases_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        logging.error(f"My purchases error: {e}")
        await message.answer("❌ Ошибка загрузки покупок.")

@dp.callback_query_handler(lambda c: c.data.startswith("mypurchases_page_"))
async def mypurchases_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"💰 Мои покупки {page}"
    await my_purchases(callback.message)
    await callback.answer()

# ===== КАЗИНО =====
@dp.message_handler(lambda message: message.text == "🎰 Казино")
async def casino_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("🎰 Введи сумму ставки (целое число):", reply_markup=back_keyboard())
    await CasinoBet.amount.set()

@dp.message_handler(state=CasinoBet.amount)
async def casino_bet_amount(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(message.from_user.id)))
        return
    try:
        amount = int(message.text)
    except ValueError:
        await message.answer("❌ Введите целое число.")
        return
    if amount <= 0:
        await message.answer("❌ Ставка должна быть положительной.")
        return
    user_id = message.from_user.id
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        await state.finish()
        return
    try:
        win_chance = int(await get_setting("casino_win_chance")) / 100
        async with db_pool.acquire() as conn:
            balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
            if amount > balance:
                await message.answer("❌ Недостаточно монет.")
                await state.finish()
                return
            win = random.random() < win_chance
            if win:
                await conn.execute("UPDATE users SET balance = balance + $1, casino_wins = casino_wins + 1 WHERE user_id=$2", amount, user_id)
                profit = amount
                win_amount = amount * 2
                phrase = random.choice(CASINO_WIN_PHRASES).format(win=win_amount, profit=profit)
                if amount >= BIG_WIN_THRESHOLD:
                    user = message.from_user
                    chat_phrase = random.choice(CHAT_WIN_PHRASES).format(name=user.first_name, amount=amount*2)
                    await notify_chats(chat_phrase, 'win')
            else:
                await conn.execute("UPDATE users SET balance = balance - $1, casino_losses = casino_losses + 1 WHERE user_id=$2", amount, user_id)
                phrase = random.choice(CASINO_LOSE_PHRASES).format(loss=amount)
            new_balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
        await message.answer(
            f"{phrase}\n💰 Текущий баланс: {new_balance}",
            reply_markup=user_main_keyboard(await is_admin(user_id))
        )
    except Exception as e:
        logging.error(f"Casino error: {e}")
        await message.answer("❌ Ошибка в казино.")
    await state.finish()

# ===== ИГРЫ =====
@dp.message_handler(lambda message: message.text == "🎲 Игры")
async def games_menu(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("Выбери игру:", reply_markup=games_keyboard())

@dp.message_handler(lambda message: message.text == "🎲 Кости")
async def dice_game(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("🎲 Введи сумму ставки (целое число):", reply_markup=back_keyboard())
    await DiceBet.amount.set()

@dp.message_handler(state=DiceBet.amount)
async def dice_bet_amount(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await games_menu(message)
        return
    try:
        amount = int(message.text)
    except ValueError:
        await message.answer("❌ Введите целое число.")
        return
    if amount <= 0:
        await message.answer("❌ Ставка должна быть положительной.")
        return
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    if amount > balance:
        await message.answer("❌ Недостаточно монет.")
        await state.finish()
        return

    dice1 = random.randint(1, 6)
    dice2 = random.randint(1, 6)
    total = dice1 + dice2
    multiplier = int(await get_setting("dice_multiplier"))

    if total > 7:
        profit = amount * multiplier
        await update_user_balance(user_id, profit)
        phrase = random.choice(DICE_WIN_PHRASES).format(dice1=dice1, dice2=dice2, total=total, profit=profit)
    else:
        await update_user_balance(user_id, -amount)
        phrase = random.choice(DICE_LOSE_PHRASES).format(dice1=dice1, dice2=dice2, total=total, loss=amount)

    new_balance = await get_user_balance(user_id)
    await message.answer(f"{phrase}\n💰 Баланс: {new_balance}")
    await state.finish()

@dp.message_handler(lambda message: message.text == "🔢 Угадай число")
async def guess_game(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("🔢 Введи сумму ставки (целое число):", reply_markup=back_keyboard())
    await GuessBet.amount.set()

@dp.message_handler(state=GuessBet.amount)
async def guess_bet_amount(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await games_menu(message)
        return
    try:
        amount = int(message.text)
    except ValueError:
        await message.answer("❌ Введите целое число.")
        return
    if amount <= 0:
        await message.answer("❌ Ставка должна быть положительной.")
        return
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    if amount > balance:
        await message.answer("❌ Недостаточно монет.")
        await state.finish()
        return
    await state.update_data(amount=amount)
    await message.answer("🔢 Загадай число от 1 до 5:", reply_markup=back_keyboard())
    await GuessBet.number.set()

@dp.message_handler(state=GuessBet.number)
async def guess_bet_number(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await games_menu(message)
        return
    try:
        guess = int(message.text)
        if guess < 1 or guess > 5:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите число от 1 до 5.")
        return
    data = await state.get_data()
    amount = data['amount']
    user_id = message.from_user.id

    secret = random.randint(1, 5)
    multiplier = int(await get_setting("guess_multiplier"))
    rep_reward = int(await get_setting("guess_reputation"))

    if guess == secret:
        profit = amount * multiplier
        await update_user_balance(user_id, profit)
        await update_user_reputation(user_id, rep_reward)
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET guess_wins = guess_wins + 1 WHERE user_id=$1", user_id)
        phrase = random.choice(GUESS_WIN_PHRASES).format(secret=secret, profit=profit, rep=rep_reward)
    else:
        await update_user_balance(user_id, -amount)
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET guess_losses = guess_losses + 1 WHERE user_id=$1", user_id)
        phrase = random.choice(GUESS_LOSE_PHRASES).format(secret=secret, loss=amount)

    new_balance = await get_user_balance(user_id)
    new_rep = await get_user_reputation(user_id)
    await message.answer(f"{phrase}\n💰 Баланс: {new_balance}\n⭐️ Репутация: {new_rep}")
        await state.finish()

# ========== НАЧАЛО БЛОКА МУЛЬТИПЛЕЕРНОЙ ИГРЫ ==========
# ===== МУЛЬТИПЛЕЕРНАЯ ИГРА "21" (ФИНАЛЬНАЯ ВЕРСИЯ) =====

# Константы
MAX_ROOMS = 20
MIN_PLAYERS = 2
MAX_PLAYERS = 5
MIN_BET = 3
DEALER_WIN_RATE = 3  # Каждая 3-я игра – выигрыш дилера

# Хранилище активных комнат (для быстрого доступа)
active_rooms = {}

@dp.message_handler(lambda message: message.text == "👥 Комнатная игра 21")
async def multiplayer_main(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("🎮 Мультиплеер 21 – выбери действие:", reply_markup=room_menu_keyboard())

@dp.message_handler(lambda message: message.text == "ℹ️ Правила игры")
async def game_rules(message: types.Message):
    rules = """
🎯 **Правила игры "21" (мультиплеер):**
• Каждый игрок делает ставку (от 3 монет).
• Цель – набрать сумму очков как можно ближе к 21, но не больше.
• Карты: 2–10 по номиналу, J/Q/K – 10 очков, Туз – 11 или 1.
• Игроки ходят по очереди: можно взять ещё карту ("Ещё") или остановиться ("Хватит").
• Дилер добирает до 17 очков.
• Победитель забирает банк за вычетом комиссии (1 монета с игрока).
• В случае ничьей ставка возвращается.
• Создатель комнаты может начать игру при наличии от 2 до 5 игроков.
• До начала игры можно выйти без потери монет.
• Во время игры выход или сдача приводят к проигрышу ставки.
    """
    await message.answer(rules)

@dp.message_handler(lambda message: message.text == "🏆 Топ игроков")
async def game_top(message: types.Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT first_name, game_wins FROM users WHERE game_wins > 0 ORDER BY game_wins DESC LIMIT 10")
    if not rows:
        await message.answer("🏆 Топ пока пуст.")
        return
    text = "🏆 **Лучшие игроки в 21:**\n\n"
    for i, row in enumerate(rows, 1):
        text += f"{i}. {row['first_name']} – {row['game_wins']} побед\n"
    await message.answer(text)

@dp.message_handler(lambda message: message.text == "📋 Список комнат")
async def list_rooms(message: types.Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT game_id, host_id, max_players, bet_amount, 
                   (SELECT COUNT(*) FROM game_players WHERE game_id = g.game_id) as player_count
            FROM multiplayer_games g
            WHERE status = 'waiting'
            ORDER BY created_at
        """)
    if not rows:
        await message.answer("📭 Нет открытых комнат. Создай свою!")
        return
    text = "📋 **Открытые комнаты:**\n\n"
    kb = []
    for row in rows:
        game_id = row['game_id']
        max_pl = row['max_players']
        cur_pl = row['player_count']
        bet = row['bet_amount']
        text += f"🆔 `{game_id}` | {cur_pl}/{max_pl} игр. | 💰 {bet} монет\n"
        kb.append([InlineKeyboardButton(text=f"Присоединиться к {game_id}", callback_data=f"join_room_{game_id}")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query_handler(lambda c: c.data.startswith("join_room_"))
async def join_room_callback(callback: types.CallbackQuery):
    game_id = callback.data.replace("join_room_", "")
    user_id = callback.from_user.id
    username = callback.from_user.username or "NoName"
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1 AND status='waiting'", game_id)
        if not game:
            await callback.answer("❌ Комната не найдена или игра уже началась.", show_alert=True)
            return
        players = await conn.fetch("SELECT user_id FROM game_players WHERE game_id=$1", game_id)
        if len(players) >= game['max_players']:
            await callback.answer("❌ Комната уже заполнена.", show_alert=True)
            return
        existing = await conn.fetchval("SELECT 1 FROM game_players WHERE game_id=$1 AND user_id=$2", game_id, user_id)
        if existing:
            await callback.answer("❌ Ты уже в этой комнате.", show_alert=True)
            return
        balance = await get_user_balance(user_id)
        bet = game['bet_amount']
        if balance < bet:
            await callback.answer(f"❌ Недостаточно монет. Нужно {bet}", show_alert=True)
            return
        # Вступаем
        await conn.execute(
            "INSERT INTO game_players (game_id, user_id, username, cards, value, stopped, joined_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            game_id, user_id, username, '', 0, False, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        # Уведомление создателю
        host_id = game['host_id']
        if host_id != user_id:
            await safe_send_message(host_id, f"✅ @{username} присоединился к твоей комнате `{game_id}`.")
    await callback.message.edit_text(f"✅ Ты присоединился к комнате `{game_id}`. Ожидаем остальных...")
    await callback.message.answer("Ты в комнате. Можешь выйти в любой момент до начала игры.", reply_markup=leave_room_keyboard(game_id))
    await callback.answer()

def leave_room_keyboard(game_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚪 Выйти из комнаты", callback_data=f"leave_room_{game_id}")]
    ])

@dp.callback_query_handler(lambda c: c.data.startswith("leave_room_"))
async def leave_room_callback(callback: types.CallbackQuery):
    game_id = callback.data.replace("leave_room_", "")
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1", game_id)
        if not game:
            await callback.answer("❌ Комната не найдена.", show_alert=True)
            return
        bet = game['bet_amount']
        if game['status'] == 'waiting':
            # Выход до начала игры – возвращаем ставку
            await update_user_balance(user_id, bet)
            await conn.execute("DELETE FROM game_players WHERE game_id=$1 AND user_id=$2", game_id, user_id)
            # Если это был создатель, передаём права следующему
            if game['host_id'] == user_id:
                next_host = await conn.fetchval("SELECT user_id FROM game_players WHERE game_id=$1 ORDER BY joined_at LIMIT 1", game_id)
                if next_host:
                    await conn.execute("UPDATE multiplayer_games SET host_id=$1 WHERE game_id=$2", next_host, game_id)
                    await safe_send_message(next_host, f"🎮 Ты стал создателем комнаты `{game_id}`.")
                else:
                    # Комната пуста – удаляем
                    await conn.execute("DELETE FROM multiplayer_games WHERE game_id=$1", game_id)
            await callback.message.edit_text("❌ Ты покинул комнату. Ставка возвращена.")
        else:
            # Выход во время игры – штраф (списываем ставку)
            await update_user_balance(user_id, -bet)
            await conn.execute("UPDATE game_players SET stopped=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
            await callback.message.edit_text(f"❌ Ты покинул игру и потерял {bet} монет.")
            # Проверяем, не закончилась ли игра
            players_left = await conn.fetchval("SELECT COUNT(*) FROM game_players WHERE game_id=$1 AND user_id != 0 AND stopped=FALSE", game_id)
            if players_left == 0:
                # Все вышли – удаляем комнату
                await conn.execute("DELETE FROM game_players WHERE game_id=$1", game_id)
                await conn.execute("DELETE FROM multiplayer_games WHERE game_id=$1", game_id)
    await callback.answer()

@dp.message_handler(lambda message: message.text == "🎮 Создать комнату")
async def create_room_start(message: types.Message):
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM multiplayer_games WHERE status='waiting'")
    if count >= MAX_ROOMS:
        await message.answer(f"❌ Достигнут лимит активных комнат ({MAX_ROOMS}). Попробуй позже.")
        return
    await message.answer("Введи количество игроков (2–5):", reply_markup=back_keyboard())
    await MultiplayerGame.create_max_players.set()

@dp.message_handler(state=MultiplayerGame.create_max_players)
async def create_room_max_players(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await multiplayer_main(message)
        return
    try:
        max_players = int(message.text)
        if max_players < MIN_PLAYERS or max_players > MAX_PLAYERS:
            raise ValueError
    except:
        await message.answer(f"❌ Введи число от {MIN_PLAYERS} до {MAX_PLAYERS}.")
        return
    await state.update_data(max_players=max_players)
    await message.answer(f"Введи ставку (целое число, не меньше {MIN_BET}):")
    await MultiplayerGame.create_bet.set()

@dp.message_handler(state=MultiplayerGame.create_bet)
async def create_room_bet(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await multiplayer_main(message)
        return
    try:
        bet = int(message.text)
        if bet < MIN_BET:
            raise ValueError
    except:
        await message.answer(f"❌ Введи целое число не меньше {MIN_BET}.")
        return
    data = await state.get_data()
    max_players = data['max_players']
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    if balance < bet:
        await message.answer(f"❌ У тебя недостаточно монет. Нужно {bet}")
        await state.finish()
        return
    game_id = generate_game_id()
    async with db_pool.acquire() as conn:
        existing = await conn.fetchval("SELECT game_id FROM multiplayer_games WHERE game_id=$1", game_id)
        while existing:
            game_id = generate_game_id()
            existing = await conn.fetchval("SELECT game_id FROM multiplayer_games WHERE game_id=$1", game_id)
        await conn.execute(
            "INSERT INTO multiplayer_games (game_id, host_id, max_players, bet_amount, status, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
            game_id, user_id, max_players, bet, 'waiting', datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        await conn.execute(
            "INSERT INTO game_players (game_id, user_id, username, cards, value, stopped, joined_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            game_id, user_id, message.from_user.username or "NoName", '', 0, False, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    await state.finish()
    await message.answer(
        f"✅ Комната `{game_id}` создана!\n"
        f"👥 Игроков: 1/{max_players}\n"
        f"💰 Ставка: {bet} монет\n\n"
        f"Ты можешь запустить игру, когда наберётся не менее {MIN_PLAYERS} игроков.",
        reply_markup=room_control_keyboard(game_id)
    )

def room_control_keyboard(game_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Начать игру", callback_data=f"start_game_{game_id}")],
        [InlineKeyboardButton(text="❌ Закрыть комнату", callback_data=f"close_room_{game_id}")]
    ])

@dp.callback_query_handler(lambda c: c.data.startswith("close_room_"))
async def close_room_callback(callback: types.CallbackQuery):
    game_id = callback.data.replace("close_room_", "")
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1 AND status='waiting'", game_id)
        if not game:
            await callback.answer("❌ Комната не найдена или игра уже началась.", show_alert=True)
            return
        if game['host_id'] != user_id:
            await callback.answer("❌ Только создатель может закрыть комнату.", show_alert=True)
            return
        bet = game['bet_amount']
        # Возвращаем ставки всем игрокам
        players = await conn.fetch("SELECT user_id FROM game_players WHERE game_id=$1", game_id)
        for player in players:
            await update_user_balance(player['user_id'], bet)
        # Удаляем игроков и комнату
        await conn.execute("DELETE FROM game_players WHERE game_id=$1", game_id)
        await conn.execute("DELETE FROM multiplayer_games WHERE game_id=$1", game_id)
    await callback.message.edit_text("🏁 Комната закрыта. Ставки возвращены.")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("start_game_"))
async def start_game_callback(callback: types.CallbackQuery):
    game_id = callback.data.replace("start_game_", "")
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1 AND status='waiting'", game_id)
        if not game:
            await callback.answer("❌ Комната не найдена или игра уже началась.", show_alert=True)
            return
        if game['host_id'] != user_id:
            await callback.answer("❌ Только создатель комнаты может начать игру.", show_alert=True)
            return
        players = await conn.fetch("SELECT user_id FROM game_players WHERE game_id=$1", game_id)
        if len(players) < MIN_PLAYERS:
            await callback.answer(f"❌ Недостаточно игроков. Нужно минимум {MIN_PLAYERS}.", show_alert=True)
            return
        await conn.execute("UPDATE multiplayer_games SET status='playing' WHERE game_id=$1", game_id)
        deck = create_deck()
        for player in players:
            cards = [deck.pop(), deck.pop()]
            cards_str = ','.join(cards)
            value = calculate_hand_value(cards)
            await conn.execute(
                "UPDATE game_players SET cards=$1, value=$2 WHERE game_id=$3 AND user_id=$4",
                cards_str, value, game_id, player['user_id']
            )
        await conn.execute(
            "INSERT INTO game_players (game_id, user_id, username, cards, value, stopped, joined_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            game_id, 0, 'Дилер', '', 0, False, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        await conn.execute("UPDATE multiplayer_games SET deck=$1 WHERE game_id=$2", ','.join(deck), game_id)
    for player in players:
        await safe_send_message(player['user_id'], f"🎮 Игра в комнате `{game_id}` началась! Твой ход.")
    await process_next_turn(game_id, 0)

async def process_next_turn(game_id: str, player_index: int):
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1", game_id)
        if not game or game['status'] != 'playing':
            return
        players = await conn.fetch("SELECT * FROM game_players WHERE game_id=$1 AND user_id != 0 ORDER BY joined_at", game_id)
        if player_index >= len(players):
            await dealer_turn(game_id)
            return
        current_player = players[player_index]
        cards = current_player['cards'].split(',') if current_player['cards'] else []
        value = calculate_hand_value(cards)
        # Сохраняем контекст
        async with dp.current_state(chat=current_player['user_id'], user=current_player['user_id']).proxy() as data:
            data['game_id'] = game_id
            data['player_index'] = player_index
        # Клавиатура с действиями
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎯 Ещё", callback_data="room_hit"),
             InlineKeyboardButton(text="🛑 Хватит", callback_data="room_stand")],
            [InlineKeyboardButton(text="🏳️ Сдаться", callback_data="room_surrender")]
        ])
        await safe_send_message(
            current_player['user_id'],
            f"🎮 Твой ход!\nТвои карты: {', '.join(cards)} (очков: {value})\n\nВыбери действие:",
            reply_markup=kb
        )

@dp.callback_query_handler(lambda c: c.data in ["room_hit", "room_stand", "room_surrender"])
async def room_action_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    async with dp.current_state(chat=user_id, user=user_id).proxy() as data:
        game_id = data.get('game_id')
        player_index = data.get('player_index')
    if not game_id:
        await callback.answer("❌ Игра не найдена.", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1", game_id)
        if not game or game['status'] != 'playing':
            await callback.answer("❌ Игра уже завершена.", show_alert=True)
            return
        players = await conn.fetch("SELECT * FROM game_players WHERE game_id=$1 AND user_id != 0 ORDER BY joined_at", game_id)
        if player_index >= len(players) or players[player_index]['user_id'] != user_id:
            await callback.answer("❌ Сейчас не твой ход.", show_alert=True)
            return
        deck = game['deck'].split(',') if game['deck'] else []
        current_player = players[player_index]
        cards = current_player['cards'].split(',') if current_player['cards'] else []
        value = calculate_hand_value(cards)

        if callback.data == "room_hit":
            if not deck:
                await callback.answer("Колода кончилась, передаём ход...", show_alert=True)
                await conn.execute("UPDATE game_players SET stopped=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
                await callback.answer()
                await process_next_turn(game_id, player_index + 1)
                return
            new_card = deck.pop()
            cards.append(new_card)
            value = calculate_hand_value(cards)
            await conn.execute(
                "UPDATE game_players SET cards=$1, value=$2 WHERE game_id=$3 AND user_id=$4",
                ','.join(cards), value, game_id, user_id
            )
            await conn.execute("UPDATE multiplayer_games SET deck=$1 WHERE game_id=$2", ','.join(deck), game_id)
            if value > 21:
                await conn.execute("UPDATE game_players SET stopped=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
                await callback.message.edit_text(f"💥 Перебор! Твои карты: {', '.join(cards)} (очков: {value})\nТы проиграл свою ставку.")
                await callback.answer()
                await process_next_turn(game_id, player_index + 1)
            else:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🎯 Ещё", callback_data="room_hit"),
                     InlineKeyboardButton(text="🛑 Хватит", callback_data="room_stand")],
                    [InlineKeyboardButton(text="🏳️ Сдаться", callback_data="room_surrender")]
                ])
                await callback.message.edit_text(
                    f"Твои карты: {', '.join(cards)} (очков: {value})\nВыбери действие:",
                    reply_markup=kb
                )
                await callback.answer()
            return

        elif callback.data == "room_stand":
            await conn.execute("UPDATE game_players SET stopped=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
            await callback.message.edit_text(f"✅ Ты остановился на {value} очках.")
            await callback.answer()
            await process_next_turn(game_id, player_index + 1)
            return

        elif callback.data == "room_surrender":
            bet = game['bet_amount']
            await update_user_balance(user_id, -bet)
            await conn.execute("UPDATE game_players SET stopped=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
            await callback.message.edit_text(f"🏳️ Ты сдался и потерял {bet} монет.")
            await callback.answer()
            await process_next_turn(game_id, player_index + 1)
            return

async def dealer_turn(game_id: str):
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1", game_id)
        if not game or game['status'] != 'playing':
            return
        deck = game['deck'].split(',') if game['deck'] else []
        dealer = await conn.fetchrow("SELECT * FROM game_players WHERE game_id=$1 AND user_id=0", game_id)
        if dealer:
            dealer_cards = dealer['cards'].split(',') if dealer['cards'] else []
            dealer_value = dealer['value']
        else:
            dealer_cards = []
            dealer_value = 0
        while dealer_value < 17 and deck:
            new_card = deck.pop()
            dealer_cards.append(new_card)
            dealer_value = calculate_hand_value(dealer_cards)
            await conn.execute(
                "UPDATE game_players SET cards=$1, value=$2 WHERE game_id=$3 AND user_id=0",
                ','.join(dealer_cards), dealer_value, game_id
            )
            await conn.execute("UPDATE multiplayer_games SET deck=$1 WHERE game_id=$2", ','.join(deck), game_id)
        # Принудительный выигрыш дилера (каждая DEALER_WIN_RATE игра)
        dealer_forced_win = (random.randint(1, DEALER_WIN_RATE) == 1)
        players = await conn.fetch("SELECT * FROM game_players WHERE game_id=$1 AND user_id != 0", game_id)
        bet = game['bet_amount']
        results = []
        for player in players:
            player_value = player['value']
            if player_value > 21:
                results.append((player['user_id'], f"❌ Проигрыш (перебор) -{bet}"))
                await update_user_balance(player['user_id'], -bet)
            elif dealer_forced_win:
                results.append((player['user_id'], f"❌ Проигрыш (дилер силён) -{bet}"))
                await update_user_balance(player['user_id'], -bet)
            elif dealer_value > 21:
                win = bet - 1  # комиссия 1 монета
                results.append((player['user_id'], f"✅ Выигрыш +{win}"))
                await update_user_balance(player['user_id'], win)
                await conn.execute("UPDATE users SET game_wins = game_wins + 1 WHERE user_id=$1", player['user_id'])
            elif player_value > dealer_value:
                win = bet - 1
                results.append((player['user_id'], f"✅ Выигрыш +{win}"))
                await update_user_balance(player['user_id'], win)
                await conn.execute("UPDATE users SET game_wins = game_wins + 1 WHERE user_id=$1", player['user_id'])
            elif player_value < dealer_value:
                results.append((player['user_id'], f"❌ Проигрыш -{bet}"))
                await update_user_balance(player['user_id'], -bet)
            else:
                results.append((player['user_id'], f"🤝 Ничья 0"))
        dealer_cards_str = ', '.join(dealer_cards) if dealer_cards else 'нет карт'
        for user_id, res in results:
            await safe_send_message(user_id,
                f"🎮 Итоги игры в комнате `{game_id}`:\n"
                f"Карты дилера: {dealer_cards_str} (очков: {dealer_value})\n"
                f"Результат: {res}"
            )
        await conn.execute("DELETE FROM game_players WHERE game_id=$1", game_id)
        await conn.execute("DELETE FROM multiplayer_games WHERE game_id=$1", game_id)

# ========== КОНЕЦ БЛОКА МУЛЬТИПЛЕЕРНОЙ ИГРЫ ==========

# ===== ПРОМОКОД =====
@dp.message_handler(lambda message: message.text == "🎟 Промокод")
async def promo_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    # ... остальной код промокода (у вас уже есть)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("Введи промокод:", reply_markup=back_keyboard())
    await PromoActivate.code.set()

@dp.message_handler(state=PromoActivate.code)
async def promo_activate(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(message.from_user.id)))
        return
    code = message.text.strip().upper()
    user_id = message.from_user.id
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        await state.finish()
        return
    try:
        async with db_pool.acquire() as conn:
            already_used = await conn.fetchval(
                "SELECT 1 FROM promo_activations WHERE user_id=$1 AND promo_code=$2",
                user_id, code
            )
            if already_used:
                await message.answer("❌ Ты уже активировал этот промокод.")
                await state.finish()
                return
            row = await conn.fetchrow("SELECT reward, max_uses, used_count FROM promocodes WHERE code=$1", code)
            if not row:
                await message.answer("❌ Промокод не найден.")
                await state.finish()
                return
            reward, max_uses, used = row['reward'], row['max_uses'], row['used_count']
            if used >= max_uses:
                await message.answer("❌ Промокод уже использован максимальное количество раз.")
                await state.finish()
                return
            async with conn.transaction():
                await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", reward, user_id)
                await conn.execute("UPDATE promocodes SET used_count = used_count + 1 WHERE code=$1", code)
                await conn.execute(
                    "INSERT INTO promo_activations (user_id, promo_code, activated_at) VALUES ($1, $2, $3)",
                    user_id, code, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
        await message.answer(
            f"✅ Промокод активирован! Ты получил {reward} монет.",
            reply_markup=user_main_keyboard(await is_admin(user_id))
        )
    except Exception as e:
        logging.error(f"Promo error: {e}")
        await message.answer("❌ Ошибка активации промокода.")
    await state.finish()

# ===== РОЗЫГРЫШИ =====
@dp.message_handler(lambda message: message.text == "🎲 Розыгрыши")
async def giveaways_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM giveaways WHERE status='active'")
            rows = await conn.fetch(
                "SELECT id, prize, end_date FROM giveaways WHERE status='active' ORDER BY end_date LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer(
                "Сейчас нет активных розыгрышей.",
                reply_markup=user_main_keyboard(await is_admin(user_id))
            )
            return
        text = f"🎁 Активные розыгрыши (страница {page}):\n\n"
        kb = []
        for row in rows:
            gid, prize, end = row['id'], row['prize'], row['end_date']
            async with db_pool.acquire() as conn2:
                count = await conn2.fetchval("SELECT COUNT(*) FROM participants WHERE giveaway_id=$1", gid)
            text += f"ID: {gid} | {prize} | до {end} | 👥 {count} участников\n"
            kb.append([InlineKeyboardButton(text=f"🔍 Подробнее о {prize}", callback_data=f"detail_{gid}")])
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"giveaways_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"giveaways_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        kb.append([InlineKeyboardButton(text="« Назад", callback_data="back_main")])
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e:
        logging.error(f"Giveaways list error: {e}")
        await message.answer("❌ Ошибка загрузки розыгрышей.")

@dp.callback_query_handler(lambda c: c.data.startswith("giveaways_page_"))
async def giveaways_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"🎲 Розыгрыши {page}"
    await giveaways_handler(callback.message)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("detail_"))
async def giveaway_detail(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    giveaway_id = int(callback.data.split("_")[1])
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT prize, description, end_date, media_file_id, media_type FROM giveaways WHERE id=$1 AND status='active'",
                giveaway_id
            )
            participants_count = await conn.fetchval("SELECT COUNT(*) FROM participants WHERE giveaway_id=$1", giveaway_id)
        if not row:
            await callback.answer("Розыгрыш не найден или завершён.", show_alert=True)
            return
        prize, desc, end_date, media_file_id, media_type = row['prize'], row['description'], row['end_date'], row['media_file_id'], row['media_type']
        caption = f"🎁 Розыгрыш: {prize}\n📝 {desc}\n📅 Окончание: {end_date}\n👥 Участников: {participants_count}\n\nЖелаешь участвовать?"
        confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, участвую", callback_data=f"confirm_part_{giveaway_id}")],
            [InlineKeyboardButton(text="❌ Нет", callback_data="cancel_detail")]
        ])
        if media_file_id and media_type:
            if media_type == 'photo':
                await callback.message.answer_photo(photo=media_file_id, caption=caption, reply_markup=confirm_kb)
            elif media_type == 'video':
                await callback.message.answer_video(video=media_file_id, caption=caption, reply_markup=confirm_kb)
            elif media_type == 'document':
                await callback.message.answer_document(document=media_file_id, caption=caption, reply_markup=confirm_kb)
        else:
            await callback.message.answer(caption, reply_markup=confirm_kb)
        await callback.answer()
    except Exception as e:
        logging.error(f"Giveaway detail error: {e}")
        await callback.answer("Ошибка загрузки деталей.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith("confirm_part_"))
async def confirm_participation(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    giveaway_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await callback.message.edit_text("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with db_pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM giveaways WHERE id=$1", giveaway_id)
            if not status or status != 'active':
                await callback.answer("Розыгрыш не активен", show_alert=True)
                return
            await conn.execute("INSERT INTO participants (user_id, giveaway_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", user_id, giveaway_id)
        await callback.answer("✅ Ты участвуешь в розыгрыше!", show_alert=True)
        await giveaways_handler(callback.message)
    except Exception as e:
        logging.error(f"Participation error: {e}")
        await callback.answer("Ошибка при участии.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "cancel_detail")
async def cancel_detail(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        return
    await callback.message.delete()
    await giveaways_handler(callback.message)

@dp.callback_query_handler(lambda c: c.data == "back_main")
async def back_main_callback(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        return
    admin_flag = await is_admin(callback.from_user.id)
    await callback.message.delete()
    await callback.message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))

# ===== ОГРАБЛЕНИЕ =====
@dp.message_handler(lambda message: message.text == "🔫 Ограбить")
async def theft_menu(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    phrase = random.choice(THEFT_CHOICE_PHRASES)
    await message.answer(phrase, reply_markup=theft_choice_keyboard())

@dp.message_handler(lambda message: message.text == "🎲 Случайная цель")
async def theft_random(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    cooldown_minutes = int(await get_setting("theft_cooldown_minutes"))
    async with db_pool.acquire() as conn:
        last_time_str = await conn.fetchval("SELECT last_theft_time FROM users WHERE user_id=$1", user_id)
        if last_time_str:
            last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
            diff = datetime.now() - last_time
            if diff < timedelta(minutes=cooldown_minutes):
                remaining = cooldown_minutes - int(diff.total_seconds() // 60)
                phrase = random.choice(THEFT_COOLDOWN_PHRASES).format(minutes=remaining)
                await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(user_id)))
                return
    target_id = await get_random_user(user_id)
    if not target_id:
        await message.answer("😕 В игре пока нет других игроков.", reply_markup=user_main_keyboard(await is_admin(user_id)))
        return
    cost = int(await get_setting("random_attack_cost"))
    if cost > 0:
        balance = await get_user_balance(user_id)
        if balance < cost:
            await message.answer(random.choice(THEFT_NO_MONEY_PHRASES), reply_markup=user_main_keyboard(await is_admin(user_id)))
            return
        await update_user_balance(user_id, -cost)
    await perform_theft(message, user_id, target_id)

@dp.message_handler(lambda message: message.text == "👤 Выбрать пользователя")
async def theft_choose_user(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    cooldown_minutes = int(await get_setting("theft_cooldown_minutes"))
    async with db_pool.acquire() as conn:
        last_time_str = await conn.fetchval("SELECT last_theft_time FROM users WHERE user_id=$1", user_id)
        if last_time_str:
            last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
            diff = datetime.now() - last_time
            if diff < timedelta(minutes=cooldown_minutes):
                remaining = cooldown_minutes - int(diff.total_seconds() // 60)
                phrase = random.choice(THEFT_COOLDOWN_PHRASES).format(minutes=remaining)
                await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(user_id)))
                return
    await message.answer("Введи @username или ID того, кого хочешь ограбить:", reply_markup=back_keyboard())
    await TheftTarget.target.set()

@dp.message_handler(state=TheftTarget.target)
async def theft_target_entered(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(message.from_user.id)))
        return
    target_input = message.text.strip()
    robber_id = message.from_user.id

    target_data = await find_user_by_input(target_input)
    if not target_data:
        await message.answer("❌ Пользователь не найден. Проверь username или ID.")
        return
    target_id = target_data['user_id']

    if target_id == robber_id:
        await message.answer("Сам себя не ограбишь, бро! 😆")
        await state.finish()
        return

    if await is_banned(target_id):
        await message.answer("❌ Этот пользователь заблокирован и не может быть целью.")
        await state.finish()
        return

    cost = int(await get_setting("targeted_attack_cost"))
    if cost > 0:
        balance = await get_user_balance(robber_id)
        if balance < cost:
            await message.answer(random.choice(THEFT_NO_MONEY_PHRASES), reply_markup=user_main_keyboard(await is_admin(robber_id)))
            await state.finish()
            return
        await update_user_balance(robber_id, -cost)

    await perform_theft(message, robber_id, target_id)
    await state.finish()

async def perform_theft(message: types.Message, robber_id: int, victim_id: int):
    # Получаем настройки заранее
    success_chance = int(await get_setting("theft_success_chance"))
    defense_chance = int(await get_setting("theft_defense_chance"))
    defense_penalty = int(await get_setting("theft_defense_penalty"))
    min_amount = int(await get_setting("min_theft_amount"))
    max_amount = int(await get_setting("max_theft_amount"))

    # Фразы уже должны быть определены глобально
    # Импортируем их для уверенности (если вдруг)
    global THEFT_DEFENSE_PHRASES, THEFT_VICTIM_DEFENSE_PHRASES, THEFT_SUCCESS_PHRASES, THEFT_FAIL_PHRASES

    try:
        async with db_pool.acquire() as conn:
            victim_balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", victim_id)
            if victim_balance is None:
                await message.answer("❌ Цель не найдена в базе.")
                return

            victim_info = await conn.fetchrow("SELECT username, first_name FROM users WHERE user_id=$1", victim_id)
            victim_name = victim_info['first_name'] if victim_info else str(victim_id)

            defense_triggered = random.randint(1, 100) <= defense_chance
            if defense_triggered:
                penalty = defense_penalty
                robber_balance = await get_user_balance(robber_id)
                if penalty > robber_balance:
                    penalty = robber_balance
                if penalty > 0:
                    await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", penalty, robber_id)
                    await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", penalty, victim_id)
                await conn.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_failed = theft_failed + 1 WHERE user_id=$1", robber_id)
                await conn.execute("UPDATE users SET theft_protected = theft_protected + 1 WHERE user_id=$1", victim_id)
                await conn.execute("UPDATE users SET last_theft_time = $1 WHERE user_id=$2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), robber_id)

                robber_phrase = random.choice(THEFT_DEFENSE_PHRASES).format(target=victim_name, penalty=penalty)
                victim_phrase = random.choice(THEFT_VICTIM_DEFENSE_PHRASES).format(attacker=message.from_user.first_name, penalty=penalty)
                await message.answer(robber_phrase, reply_markup=user_main_keyboard(await is_admin(robber_id)))
                await safe_send_message(victim_id, victim_phrase)
                return

            success = random.randint(1, 100) <= success_chance
            if success and victim_balance > 0:
                steal_amount = random.randint(min_amount, min(max_amount, victim_balance))
                await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", steal_amount, victim_id)
                await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", steal_amount, robber_id)
                await conn.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_success = theft_success + 1 WHERE user_id=$1", robber_id)

                # Проверяем, не достиг ли грабитель 15 успешных ограблений (для реферальной награды)
                new_success = await conn.fetchval("SELECT theft_success FROM users WHERE user_id=$1", robber_id)
                if new_success == 15:
                    ref = await conn.fetchrow("SELECT referrer_id FROM referrals WHERE referred_id=$1 AND reward_given=FALSE", robber_id)
                    if ref:
                        referrer_id = ref['referrer_id']
                        bonus_coins = int(await get_setting("referral_bonus"))
                        bonus_rep = int(await get_setting("referral_reputation"))
                        await update_user_balance(referrer_id, bonus_coins)
                        await update_user_reputation(referrer_id, bonus_rep)
                        await conn.execute("UPDATE referrals SET reward_given=TRUE WHERE referred_id=$1", robber_id)
                        await safe_send_message(referrer_id, f"🎉 Ваш реферал совершил 15 успешных ограблений! Вы получили {bonus_coins} монет и {bonus_rep} репутации.")

                phrase = random.choice(THEFT_SUCCESS_PHRASES).format(amount=steal_amount, target=victim_name)
                await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(robber_id)))
                await safe_send_message(victim_id, f"🔫 Вас ограбили! {message.from_user.first_name} украл {steal_amount} монет.")
            else:
                await conn.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_failed = theft_failed + 1 WHERE user_id=$1", robber_id)
                phrase = random.choice(THEFT_FAIL_PHRASES).format(target=victim_name)
                await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(robber_id)))

            await conn.execute("UPDATE users SET last_theft_time = $1 WHERE user_id=$2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), robber_id)

    except Exception as e:
        logging.error(f"Theft error: {e}")
        await message.answer("❌ Ошибка при ограблении.")

# ===== РЕФЕРАЛЬНАЯ ССЫЛКА =====
@dp.message_handler(lambda message: message.text == "🔗 Рефералка")
async def referral_link(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    bot_username = (await bot.me).username
    link = f"https://t.me/{bot_username}?start=ref{user_id}"
    bonus_coins = await get_setting("referral_bonus")
    bonus_rep = await get_setting("referral_reputation")
    await message.answer(
        f"🔗 Твоя реферальная ссылка:\n{link}\n\n"
        f"Приведи друга и получи {bonus_coins} монет и {bonus_rep} репутации, когда он совершит 15 успешных ограблений!"
    )

# ===== ПОДГОН В ЧАТАХ =====
@dp.message_handler(lambda message: message.chat.type != 'private' and message.text == "🎁 Подгон")
async def chat_gift(message: types.Message):
    chat_id = message.chat.id
    user_id = message.from_user.id

    if await is_banned(user_id):
        return

    gift_amount = int(await get_setting("gift_amount"))
    gift_limit = int(await get_setting("gift_limit_per_day"))
    today_date = date.today()

    async with db_pool.acquire() as conn:
        chat = await conn.fetchrow("SELECT * FROM chats WHERE chat_id=$1", chat_id)
        if not chat:
            return

        last_date = chat['last_gift_date']
        gift_count = chat['gift_count_today'] if last_date == today_date.isoformat() else 0

        if gift_count >= gift_limit:
            await message.reply(f"❌ Сегодня уже использовано {gift_count} из {gift_limit} подгонов. Попробуйте завтра.")
            return

        try:
            admins = await bot.get_chat_administrators(chat_id)
            eligible = [a.user for a in admins if a.user.id != user_id and not await is_banned(a.user.id)]
            if not eligible:
                await message.reply("❌ Нет подходящих получателей для подарка.")
                return
            recipient = random.choice(eligible)
        except Exception as e:
            logging.error(f"Gift error: {e}")
            await message.reply("❌ Не удалось выбрать получателя.")
            return

        await update_user_balance(recipient.id, gift_amount)

        if last_date == today_date.isoformat():
            await conn.execute("UPDATE chats SET gift_count_today = gift_count_today + 1 WHERE chat_id=$1", chat_id)
        else:
            await conn.execute("UPDATE chats SET last_gift_date=$1, gift_count_today=1 WHERE chat_id=$2", today_date.isoformat(), chat_id)

    await message.answer(
        f"🎁 {message.from_user.first_name} активировал подгон!\n"
        f"Счастливчик: {recipient.first_name} получает {gift_amount} монет! 🎉"
    )

# ===== ЗАДАНИЯ =====
@dp.message_handler(lambda message: message.text == "📋 Задания")
async def tasks_menu(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, description, reward_coins, reward_reputation FROM tasks WHERE active=TRUE")
    if not rows:
        await message.answer("📋 Пока нет доступных заданий.", reply_markup=user_main_keyboard(await is_admin(user_id)))
        return

    text = "📋 Доступные задания:\n\n"
    kb = []
    for row in rows:
        text += f"🔹 {row['name']}\n{row['description']}\nНаграда: {row['reward_coins']} монет, {row['reward_reputation']} репутации\n\n"
        kb.append([InlineKeyboardButton(text=f"Выполнить {row['name']}", callback_data=f"task_{row['id']}")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query_handler(lambda c: c.data.startswith("task_"))
async def take_task(callback: types.CallbackQuery):
    task_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

    async with db_pool.acquire() as conn:
        existing = await conn.fetchval("SELECT 1 FROM user_tasks WHERE user_id=$1 AND task_id=$2", user_id, task_id)
        if existing:
            await callback.answer("Ты уже выполнял это задание!", show_alert=True)
            return

        task = await conn.fetchrow("SELECT * FROM tasks WHERE id=$1 AND active=TRUE", task_id)
        if not task:
            await callback.answer("Задание не найдено или неактивно.", show_alert=True)
            return

        if task['task_type'] == 'subscribe':
            channel_id = task['target_id']
            try:
                member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
                if member.status in ['left', 'kicked']:
                    await callback.answer("❌ Ты не подписан на этот канал!", show_alert=True)
                    return
            except Exception as e:
                logging.error(f"Task subscribe check error: {e}")
                await callback.answer("❌ Не удалось проверить подписку. Возможно, бот не админ канала.", show_alert=True)
                return

            async with conn.transaction():
                await conn.execute("UPDATE users SET balance = balance + $1, reputation = reputation + $2 WHERE user_id=$3",
                                   task['reward_coins'], task['reward_reputation'], user_id)
                expires_at = (datetime.now() + timedelta(days=task['required_days'])).strftime("%Y-%m-%d %H:%M:%S") if task['required_days'] > 0 else None
                await conn.execute(
                    "INSERT INTO user_tasks (user_id, task_id, completed_at, expires_at, status) VALUES ($1, $2, $3, $4, $5)",
                    user_id, task_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), expires_at, 'completed'
                )

            await callback.answer(f"✅ Задание выполнено! +{task['reward_coins']} монет, +{task['reward_reputation']} репутации", show_alert=True)
            await callback.message.delete()
        else:
            await callback.answer("Этот тип заданий пока не поддерживается.", show_alert=True)

# ===== АДМИН ПАНЕЛЬ =====
@dp.message_handler(lambda message: message.text == "⚙️ Админ панель")
async def admin_panel(message: types.Message):
    if message.chat.type != 'private':
        return
    if not await is_admin(message.from_user.id):
        await message.answer("У тебя нет прав администратора.")
        return
    super_admin = await is_super_admin(message.from_user.id)
    await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
@dp.message_handler(lambda message: message.text == "🧹 Очистить старые записи")
async def admin_cleanup(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может это делать.")
        return

    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, чистить", callback_data="cleanup_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cleanup_cancel")]
    ])
    await message.answer("⚠️ Удалить все использованные промокоды и неактивные задания?", reply_markup=confirm_kb)

@dp.callback_query_handler(lambda c: c.data == "cleanup_confirm")
async def cleanup_confirm(callback: types.CallbackQuery):
    if not await is_super_admin(callback.from_user.id):
        await callback.answer("Недостаточно прав", show_alert=True)
        return

    async with db_pool.acquire() as conn:
        # Удаляем промокоды
        promo_result = await conn.execute("DELETE FROM promocodes WHERE used_count >= max_uses")
        promo_deleted = promo_result.split()[1]  # результат в asyncpg — строка типа "DELETE 5"

        # Удаляем неактивные задания
        tasks_result = await conn.execute("DELETE FROM tasks WHERE active = FALSE")
        tasks_deleted = tasks_result.split()[1]

    await callback.message.edit_text(
        f"✅ Очистка завершена!\n"
        f"Удалено промокодов: {promo_deleted}\n"
        f"Удалено заданий: {tasks_deleted}"
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "cleanup_cancel")
async def cleanup_cancel(callback: types.CallbackQuery):
    await callback.message.edit_text("❌ Очистка отменена")
    await callback.answer()
# ===== УПРАВЛЕНИЕ ЗАДАНИЯМИ =====
@dp.message_handler(lambda message: message.text == "📋 Управление заданиями")
async def admin_tasks_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Управление заданиями:", reply_markup=task_admin_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Создать задание")
async def create_task_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи название задания:", reply_markup=back_keyboard())
    await CreateTask.name.set()

@dp.message_handler(state=CreateTask.name)
async def create_task_name(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    await state.update_data(name=message.text)
    await message.answer("Введи описание задания:")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.description)
async def create_task_description(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи тип задания (subscribe):")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.task_type)
async def create_task_type(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    task_type = message.text.lower()
    if task_type not in ['subscribe']:
        await message.answer("Поддерживается только 'subscribe'")
        return
    await state.update_data(task_type=task_type)
    await message.answer("Введи ID канала (с -100) для подписки:")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.target_id)
async def create_task_target(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    await state.update_data(target_id=message.text.strip())
    await message.answer("Введи награду (монеты):")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.reward_coins)
async def create_task_reward_coins(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    try:
        coins = int(message.text)
    except:
        await message.answer("Введи целое число.")
        return
    await state.update_data(reward_coins=coins)
    await message.answer("Введи награду (репутация):")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.reward_reputation)
async def create_task_reward_rep(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    try:
        rep = int(message.text)
    except:
        await message.answer("Введи целое число.")
        return
    await state.update_data(reward_reputation=rep)
    await message.answer("Сколько дней нужно быть подписанным? (0 - не проверять):")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.required_days)
async def create_task_required_days(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    try:
        days = int(message.text)
        if days < 0:
            raise ValueError
    except:
        await message.answer("Введи неотрицательное целое число.")
        return
    await state.update_data(required_days=days)
    await message.answer("Штрафных дней (если отписался раньше, 0 - нет штрафа):")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.penalty_days)
async def create_task_penalty_days(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    try:
        days = int(message.text)
        if days < 0:
            raise ValueError
    except:
        await message.answer("Введи неотрицательное целое число.")
        return
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO tasks (name, description, task_type, target_id, reward_coins, reward_reputation, required_days, penalty_days, created_by, created_at, active) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE)",
                data['name'], data['description'], data['task_type'], data['target_id'], data['reward_coins'], data['reward_reputation'], data['required_days'], days, message.from_user.id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        await message.answer("✅ Задание создано!", reply_markup=task_admin_keyboard())
    except Exception as e:
        logging.error(f"Create task error: {e}")
        await message.answer("❌ Ошибка при создании задания.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список заданий")
async def list_tasks(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, active FROM tasks ORDER BY id")
    if not rows:
        await message.answer("Нет заданий.")
        return
    text = "📋 Задания:\n"
    for row in rows:
        text += f"ID {row['id']}: {row['name']} ({'активно' if row['active'] else 'неактивно'})\n"
    await message.answer(text, reply_markup=task_admin_keyboard())

@dp.message_handler(lambda message: message.text == "❌ Удалить задание")
async def delete_task_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID задания для удаления (деактивации):", reply_markup=back_keyboard())
    await DeleteTask.task_id.set()  # используем отдельное состояние

@dp.message_handler(state=DeleteTask.task_id)
async def delete_task_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    try:
        task_id = int(message.text)
    except:
        await message.answer("Введи число.")
        return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE tasks SET active=FALSE WHERE id=$1", task_id)
    await message.answer("✅ Задание деактивировано.", reply_markup=task_admin_keyboard())
    await state.finish()

# ===== УПРАВЛЕНИЕ РОЗЫГРЫШАМИ =====
@dp.message_handler(lambda message: message.text == "🎁 Управление розыгрышами")
async def admin_giveaway_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Управление розыгрышами:", reply_markup=giveaway_admin_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Создать розыгрыш")
async def create_giveaway_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи название приза:", reply_markup=back_keyboard())
    await CreateGiveaway.prize.set()

@dp.message_handler(state=CreateGiveaway.prize)
async def create_giveaway_prize(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    await state.update_data(prize=message.text)
    await message.answer("Введи описание розыгрыша:")
    await CreateGiveaway.description.set()

@dp.message_handler(state=CreateGiveaway.description)
async def create_giveaway_description(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи дату окончания в формате ДД.ММ.ГГГГ ЧЧ:ММ (например, 31.12.2025 23:59):")
    await CreateGiveaway.end_date.set()

@dp.message_handler(state=CreateGiveaway.end_date)
async def create_giveaway_end_date(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    try:
        end_date = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
        if end_date <= datetime.now():
            await message.answer("Дата окончания должна быть в будущем.")
            return
        await state.update_data(end_date=end_date.strftime("%Y-%m-%d %H:%M:%S"))
    except ValueError:
        await message.answer("Неверный формат. Используй ДД.ММ.ГГГГ ЧЧ:ММ")
        return
    await message.answer("Отправь медиа (фото, видео или документ) для розыгрыша или отправь 'пропустить':")
    await CreateGiveaway.media.set()

@dp.message_handler(state=CreateGiveaway.media, content_types=['text', 'photo', 'video', 'document'])
async def create_giveaway_media(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    data = await state.get_data()
    media_file_id = None
    media_type = None
    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = 'photo'
    elif message.video:
        media_file_id = message.video.file_id
        media_type = 'video'
    elif message.document:
        media_file_id = message.document.file_id
        media_type = 'document'
    elif message.text and message.text.lower() == 'пропустить':
        pass
    else:
        await message.answer("Пожалуйста, отправь фото, видео, документ или 'пропустить'.")
        return

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO giveaways (prize, description, end_date, media_file_id, media_type) VALUES ($1, $2, $3, $4, $5)",
                data['prize'], data['description'], data['end_date'], media_file_id, media_type
            )
        await message.answer("✅ Розыгрыш создан!", reply_markup=giveaway_admin_keyboard())
    except Exception as e:
        logging.error(f"Create giveaway error: {e}")
        await message.answer("❌ Ошибка при создании розыгрыша.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Активные розыгрыши")
async def list_active_giveaways(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM giveaways WHERE status='active'")
            rows = await conn.fetch(
                "SELECT id, prize, end_date, description FROM giveaways WHERE status='active' ORDER BY end_date LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("Нет активных розыгрышей.")
            return
        text = f"Активные розыгрыши (страница {page}):\n"
        for row in rows:
            gid, prize, end, desc = row['id'], row['prize'], row['end_date'], row['description']
            async with db_pool.acquire() as conn2:
                count = await conn2.fetchval("SELECT COUNT(*) FROM participants WHERE giveaway_id=$1", gid)
            text += f"ID: {gid} | {prize} | до {end} | 👥 {count} участников\n{desc}\n\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"activegiveaways_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"activegiveaways_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text, reply_markup=giveaway_admin_keyboard())
    except Exception as e:
        logging.error(f"List giveaways error: {e}")
        await message.answer("❌ Ошибка.")

@dp.callback_query_handler(lambda c: c.data.startswith("activegiveaways_page_"))
async def activegiveaways_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"📋 Активные розыгрыши {page}"
    await list_active_giveaways(callback.message)
    await callback.answer()

@dp.message_handler(lambda message: message.text == "✅ Завершить розыгрыш")
async def finish_giveaway_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID розыгрыша, который нужно завершить:", reply_markup=back_keyboard())
    await CompleteGiveaway.giveaway_id.set()

@dp.message_handler(state=CompleteGiveaway.giveaway_id)
async def finish_giveaway(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    try:
        gid = int(message.text)
    except ValueError:
        await message.answer("❌ Введи число.")
        return
    await state.update_data(giveaway_id=gid)
    await message.answer("Введи количество победителей (целое число):")
    await CompleteGiveaway.winners_count.set()

@dp.message_handler(state=CompleteGiveaway.winners_count)
async def finish_giveaway_winners(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    try:
        winners_count = int(message.text)
        if winners_count < 1:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    gid = data['giveaway_id']
    try:
        async with db_pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM giveaways WHERE id=$1", gid)
            if not status or status != 'active':
                await message.answer("Розыгрыш не активен или не существует.")
                await state.finish()
                return
            participants = await conn.fetch("SELECT user_id FROM participants WHERE giveaway_id=$1", gid)
            participants = [r['user_id'] for r in participants]
            if not participants:
                await message.answer("В этом розыгрыше нет участников.")
                await state.finish()
                return
            if winners_count > len(participants):
                winners_count = len(participants)
            winners = random.sample(participants, winners_count)
            await conn.execute("UPDATE giveaways SET status='completed', winner_id=$1 WHERE id=$2", winners[0], gid)
            for wid in winners:
                safe_send_message_task(wid, f"🎉 Поздравляем! Ты выиграл в розыгрыше! Свяжись с админом.")
        await message.answer(f"🏆 Победители выбраны! ({len(winners)})", reply_markup=giveaway_admin_keyboard())
    except Exception as e:
        logging.error(f"Finish giveaway error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== УПРАВЛЕНИЕ КАНАЛАМИ =====
@dp.message_handler(lambda message: message.text == "📺 Управление каналами")
async def admin_channel_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Управление каналами:", reply_markup=channel_admin_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Добавить канал")
async def add_channel_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи chat_id канала (можно получить у @username_to_id_bot):", reply_markup=back_keyboard())
    await AddChannel.chat_id.set()

@dp.message_handler(state=AddChannel.chat_id)
async def add_channel_chat_id(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    await state.update_data(chat_id=message.text.strip())
    await message.answer("Введи название канала:")
    await AddChannel.next()

@dp.message_handler(state=AddChannel.title)
async def add_channel_title(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    await state.update_data(title=message.text)
    await message.answer("Введи invite-ссылку (или отправь 'нет'):")
    await AddChannel.next()

@dp.message_handler(state=AddChannel.invite_link)
async def add_channel_link(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    link = None if message.text.lower() == 'нет' else message.text.strip()
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO channels (chat_id, title, invite_link) VALUES ($1, $2, $3)",
                data['chat_id'], data['title'], link
            )
        await message.answer("✅ Канал добавлен!", reply_markup=channel_admin_keyboard())
    except asyncpg.UniqueViolationError:
        await message.answer("❌ Канал с таким chat_id уже существует.")
    except Exception as e:
        logging.error(f"Add channel error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "➖ Удалить канал")
async def remove_channel_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи chat_id канала для удаления:", reply_markup=back_keyboard())
    await RemoveChannel.chat_id.set()

@dp.message_handler(state=RemoveChannel.chat_id)
async def remove_channel(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    chat_id = message.text.strip()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM channels WHERE chat_id=$1", chat_id)
        await message.answer("✅ Канал удалён, если существовал.", reply_markup=channel_admin_keyboard())
    except Exception as e:
        logging.error(f"Remove channel error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список каналов")
async def list_channels(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    channels = await get_channels()
    if not channels:
        await message.answer("Нет добавленных каналов.")
        return
    text = "📺 Каналы для подписки:\n"
    for chat_id, title, link in channels:
        text += f"• {title} (chat_id: {chat_id})\n  Ссылка: {link or 'нет'}\n"
    await message.answer(text, reply_markup=channel_admin_keyboard())

# ===== УПРАВЛЕНИЕ МАГАЗИНОМ =====
@dp.message_handler(lambda message: message.text == "🛒 Управление магазином")
async def admin_shop_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Управление магазином:", reply_markup=shop_admin_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Добавить товар")
async def add_shop_item_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи название товара:", reply_markup=back_keyboard())
    await AddShopItem.name.set()

@dp.message_handler(state=AddShopItem.name)
async def add_shop_item_name(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    await state.update_data(name=message.text)
    await message.answer("Введи описание товара:")
    await AddShopItem.next()

@dp.message_handler(state=AddShopItem.description)
async def add_shop_item_description(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи цену (целое число):")
    await AddShopItem.next()

@dp.message_handler(state=AddShopItem.price)
async def add_shop_item_price(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        price = int(message.text)
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Цена должна быть положительным целым числом.")
        return
    await state.update_data(price=price)
    await message.answer("Введи количество товара (целое число, -1 для бесконечного):")
    await AddShopItem.stock.set()

@dp.message_handler(state=AddShopItem.stock)
async def add_shop_item_stock(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        stock = int(message.text)
    except ValueError:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO shop_items (name, description, price, stock) VALUES ($1, $2, $3, $4)",
                data['name'], data['description'], data['price'], stock
            )
        await message.answer("✅ Товар добавлен!", reply_markup=shop_admin_keyboard())
    except Exception as e:
        logging.error(f"Add shop item error: {e}")
        await message.answer("❌ Ошибка при добавлении товара.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "➖ Удалить товар")
async def remove_shop_item_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    try:
        async with db_pool.acquire() as conn:
            items = await conn.fetch("SELECT id, name FROM shop_items ORDER BY id")
        if not items:
            await message.answer("В магазине нет товаров.")
            return
        text = "Товары:\n" + "\n".join([f"ID {i['id']}: {i['name']}" for i in items])
        await message.answer(text + "\n\nВведи ID товара для удаления:", reply_markup=back_keyboard())
    except Exception as e:
        logging.error(f"List items for remove error: {e}")
        await message.answer("❌ Ошибка.")
        return
    await RemoveShopItem.item_id.set()

@dp.message_handler(state=RemoveShopItem.item_id)
async def remove_shop_item(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        item_id = int(message.text)
    except ValueError:
        await message.answer("❌ Введи число.")
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM shop_items WHERE id=$1", item_id)
        await message.answer("✅ Товар удалён, если существовал.", reply_markup=shop_admin_keyboard())
    except Exception as e:
        logging.error(f"Remove shop item error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список товаров")
async def list_shop_items(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM shop_items")
            items = await conn.fetch(
                "SELECT id, name, description, price, stock FROM shop_items ORDER BY id LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not items:
            await message.answer("В магазине нет товаров.")
            return
        text = f"📦 Товары (страница {page}):\n"
        for item in items:
            text += f"\nID {item['id']} | {item['name']}\n{item['description']}\n💰 {item['price']} | наличие: {item['stock'] if item['stock']!=-1 else '∞'}\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"shopitems_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"shopitems_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text, reply_markup=shop_admin_keyboard())
    except Exception as e:
        logging.error(f"List shop items error: {e}")
        await message.answer("❌ Ошибка.")

@dp.callback_query_handler(lambda c: c.data.startswith("shopitems_page_"))
async def shopitems_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"📋 Список товаров {page}"
    await list_shop_items(callback.message)
    await callback.answer()

@dp.message_handler(lambda message: message.text == "✏️ Редактировать товар")
async def edit_shop_item_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID товара для редактирования:", reply_markup=back_keyboard())
    await EditShopItem.item_id.set()

@dp.message_handler(state=EditShopItem.item_id)
async def edit_shop_item_field(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        item_id = int(message.text)
    except ValueError:
        await message.answer("❌ Введи число.")
        return
    await state.update_data(item_id=item_id)
    await message.answer("Что хочешь изменить? (price/stock)", reply_markup=back_keyboard())
    await EditShopItem.field.set()

@dp.message_handler(state=EditShopItem.field)
async def edit_shop_item_value(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    field = message.text.lower()
    if field not in ['price', 'stock']:
        await message.answer("❌ Можно изменить только price или stock.")
        return
    await state.update_data(field=field)
    await message.answer(f"Введи новое значение для {field}:")
    await EditShopItem.value.set()

@dp.message_handler(state=EditShopItem.value)
async def edit_shop_item_final(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        value = int(message.text)
    except ValueError:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    item_id = data['item_id']
    field = data['field']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(f"UPDATE shop_items SET {field}=$1 WHERE id=$2", value, item_id)
        await message.answer("✅ Товар обновлён.", reply_markup=shop_admin_keyboard())
    except Exception as e:
        logging.error(f"Edit shop item error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== УПРАВЛЕНИЕ ПРОМОКОДАМИ =====
@dp.message_handler(lambda message: message.text == "🎫 Управление промокодами")
async def admin_promo_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Управление промокодами:", reply_markup=promo_admin_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Создать промокод")
async def create_promo_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи код промокода (латиница, цифры):", reply_markup=back_keyboard())
    await CreatePromocode.code.set()

@dp.message_handler(state=CreatePromocode.code)
async def create_promo_code(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_promo_menu(message)
        return
    code = message.text.strip().upper()
    await state.update_data(code=code)
    await message.answer("Введи количество монет, которые даёт промокод:")
    await CreatePromocode.next()

@dp.message_handler(state=CreatePromocode.reward)
async def create_promo_reward(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_promo_menu(message)
        return
    try:
        reward = int(message.text)
        if reward <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    await state.update_data(reward=reward)
    await message.answer("Введи максимальное количество использований:")
    await CreatePromocode.next()

@dp.message_handler(state=CreatePromocode.max_uses)
async def create_promo_max_uses(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_promo_menu(message)
        return
    try:
        max_uses = int(message.text)
        if max_uses <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO promocodes (code, reward, max_uses) VALUES ($1, $2, $3)",
                data['code'], data['reward'], max_uses
            )
        await message.answer("✅ Промокод создан!", reply_markup=promo_admin_keyboard())
    except asyncpg.UniqueViolationError:
        await message.answer("❌ Промокод с таким кодом уже существует.")
    except Exception as e:
        logging.error(f"Create promo error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список промокодов")
async def list_promos(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM promocodes")
            rows = await conn.fetch(
                "SELECT code, reward, max_uses, used_count FROM promocodes LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("Нет промокодов.")
            return
        text = f"🎫 Промокоды (страница {page}):\n"
        for row in rows:
            text += f"• {row['code']}: {row['reward']} монет, использовано {row['used_count']}/{row['max_uses']}\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"promos_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"promos_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text, reply_markup=promo_admin_keyboard())
    except Exception as e:
        logging.error(f"List promos error: {e}")
        await message.answer("❌ Ошибка.")

@dp.callback_query_handler(lambda c: c.data.startswith("promos_page_"))
async def promos_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"📋 Список промокодов {page}"
    await list_promos(callback.message)
    await callback.answer()

# ===== НАСТРОЙКИ ИГРЫ =====
@dp.message_handler(lambda message: message.text == "⚙️ Настройки игры")
async def settings_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    settings = {}
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM settings")
        for row in rows:
            settings[row['key']] = row['value']
    text = "⚙️ <b>Текущие настройки игры:</b>\n\n"
    text += f"💰 Стоимость случайной кражи: {settings.get('random_attack_cost', '0')} монет\n"
    text += f"👤 Стоимость кражи по username: {settings.get('targeted_attack_cost', '50')} монет\n"
    text += f"⏱ Кулдаун между кражами: {settings.get('theft_cooldown_minutes', '30')} мин\n"
    text += f"🎲 Шанс успеха кражи: {settings.get('theft_success_chance', '40')}%\n"
    text += f"🛡 Шанс защиты жертвы: {settings.get('theft_defense_chance', '20')}%\n"
    text += f"💥 Штраф при защите: {settings.get('theft_defense_penalty', '10')} монет\n"
    text += f"🎰 Шанс выигрыша в казино: {settings.get('casino_win_chance', '30')}%\n"
    text += f"💰 Мин. сумма кражи: {settings.get('min_theft_amount', '5')}\n"
    text += f"💰 Макс. сумма кражи: {settings.get('max_theft_amount', '15')}\n"
    text += f"🎲 Множитель костей: {settings.get('dice_multiplier', '2')}\n"
    text += f"🔢 Множитель угадайки: {settings.get('guess_multiplier', '5')}\n"
    text += f"⭐️ Репутация за угадайку: {settings.get('guess_reputation', '1')}\n"
    text += f"📢 Уведомления в чатах: {settings.get('chat_notify_big_win', '1')} (1-вкл, 0-выкл)\n"
    text += f"💰 Сумма подарка в чате: {settings.get('gift_amount', '30')}\n"
    text += f"📊 Лимит подарков в день: {settings.get('gift_limit_per_day', '3')}\n"
    text += f"👥 Реферальный бонус (монеты): {settings.get('referral_bonus', '50')}\n"
    text += f"⭐️ Реферальный бонус (репутация): {settings.get('referral_reputation', '2')}\n\n"
    text += "Выбери параметр для изменения (нажми на кнопку):"
    await message.answer(text, reply_markup=settings_reply_keyboard())

@dp.message_handler(lambda message: message.text in [
    "💰 Стоимость случайной кражи",
    "👤 Стоимость кражи по username",
    "⏱ Кулдаун (минут)",
    "🎲 Шанс успеха %",
    "🛡 Шанс защиты %",
    "💥 Штраф при защите",
    "🎰 Шанс казино %",
    "💰 Мин. сумма кражи",
    "💰 Макс. сумма кражи",
    "🎲 Множитель костей",
    "🔢 Множитель угадайки",
    "⭐️ Репутация за угадайку",
    "📢 Уведомления в чатах",
    "💰 Сумма подарка в чате",
    "📊 Лимит подарков в день",
    "👥 Реферальный бонус (монеты)",
    "⭐️ Реферальный бонус (репутация)"
])
async def settings_edit_start(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    key_map = {
        "💰 Стоимость случайной кражи": "random_attack_cost",
        "👤 Стоимость кражи по username": "targeted_attack_cost",
        "⏱ Кулдаун (минут)": "theft_cooldown_minutes",
        "🎲 Шанс успеха %": "theft_success_chance",
        "🛡 Шанс защиты %": "theft_defense_chance",
        "💥 Штраф при защите": "theft_defense_penalty",
        "🎰 Шанс казино %": "casino_win_chance",
        "💰 Мин. сумма кражи": "min_theft_amount",
        "💰 Макс. сумма кражи": "max_theft_amount",
        "🎲 Множитель костей": "dice_multiplier",
        "🔢 Множитель угадайки": "guess_multiplier",
        "⭐️ Репутация за угадайку": "guess_reputation",
        "📢 Уведомления в чатах": "chat_notify_big_win",
        "💰 Сумма подарка в чате": "gift_amount",
        "📊 Лимит подарков в день": "gift_limit_per_day",
        "👥 Реферальный бонус (монеты)": "referral_bonus",
        "⭐️ Реферальный бонус (репутация)": "referral_reputation",
    }
    key = key_map.get(message.text)
    if not key:
        return
    await state.update_data(setting_key=key)
    await message.answer(f"Введи новое значение для параметра (целое число):", reply_markup=back_keyboard())
    await EditSettings.key.set()

@dp.message_handler(state=EditSettings.key)
async def set_setting_value(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await settings_menu(message)
        return
    try:
        value = int(message.text)
    except ValueError:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    key = data['setting_key']
    await set_setting(key, str(value))
    await message.answer(f"✅ Параметр обновлён.")
    await state.finish()
    await settings_menu(message)

# ===== СТАТИСТИКА =====
@dp.message_handler(lambda message: message.text == "📊 Статистика")
async def stats_handler(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    try:
        async with db_pool.acquire() as conn:
            users = await conn.fetchval("SELECT COUNT(*) FROM users")
            total_balance = await conn.fetchval("SELECT SUM(balance) FROM users") or 0
            total_reputation = await conn.fetchval("SELECT SUM(reputation) FROM users") or 0
            total_spent = await conn.fetchval("SELECT SUM(total_spent) FROM users") or 0
            active_giveaways = await conn.fetchval("SELECT COUNT(*) FROM giveaways WHERE status='active'") or 0
            shop_items = await conn.fetchval("SELECT COUNT(*) FROM shop_items") or 0
            purchases_pending = await conn.fetchval("SELECT COUNT(*) FROM purchases WHERE status='pending'") or 0
            purchases_completed = await conn.fetchval("SELECT COUNT(*) FROM purchases WHERE status='completed'") or 0
            total_thefts = await conn.fetchval("SELECT SUM(theft_attempts) FROM users") or 0
            total_thefts_success = await conn.fetchval("SELECT SUM(theft_success) FROM users") or 0
            promos = await conn.fetchval("SELECT COUNT(*) FROM promocodes") or 0
            banned = await conn.fetchval("SELECT COUNT(*) FROM banned_users") or 0
        text = (
            f"📊 Статистика:\n"
            f"👥 Пользователей: {users}\n"
            f"💰 Всего монет: {total_balance}\n"
            f"⭐️ Всего репутации: {total_reputation}\n"
            f"💸 Всего потрачено: {total_spent}\n"
            f"🎁 Активных розыгрышей: {active_giveaways}\n"
            f"🛒 Товаров в магазине: {shop_items}\n"
            f"🛍️ Ожидающих покупок: {purchases_pending}\n"
            f"✅ Выполненных покупок: {purchases_completed}\n"
            f"🔫 Всего ограблений: {total_thefts} (успешно: {total_thefts_success})\n"
            f"🎫 Промокодов создано: {promos}\n"
            f"⛔ Заблокировано: {banned}"
        )
        await message.answer(text, reply_markup=admin_main_keyboard(await is_super_admin(message.from_user.id)))
    except Exception as e:
        logging.error(f"Stats error: {e}")
        await message.answer("❌ Ошибка получения статистики.")

# ===== НАЙТИ ПОЛЬЗОВАТЕЛЯ =====
@dp.message_handler(lambda message: message.text == "👥 Найти пользователя")
async def find_user_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await FindUser.query.set()

@dp.message_handler(state=FindUser.query)
async def find_user_result(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    name = user_data['first_name']
    bal = user_data['balance']
    rep = user_data['reputation']
    spent = user_data['total_spent']
    joined = user_data['joined_date']
    attempts = user_data['theft_attempts']
    success = user_data['theft_success']
    failed = user_data['theft_failed']
    protected = user_data['theft_protected']
    banned = await is_banned(uid)
    ban_status = "⛔ Заблокирован" if banned else "✅ Активен"
    text = (
        f"👤 Пользователь: {name} (ID: {uid})\n"
        f"💰 Баланс: {bal}\n"
        f"⭐️ Репутация: {rep}\n"
        f"💸 Потрачено: {spent}\n"
        f"📅 Регистрация: {joined}\n"
        f"🔫 Ограблений: {attempts} (успешно: {success}, провал: {failed})\n"
        f"⚔️ Отбито атак: {protected}\n"
        f"Статус: {ban_status}"
    )
    await message.answer(text)
    await state.finish()

# ===== СПИСОК ПОКУПОК (АДМИН) =====
@dp.message_handler(lambda message: message.text == "🛍️ Список покупок")
async def admin_purchases(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT p.id, u.user_id, u.username, s.name, p.purchase_date, p.status FROM purchases p "
                "JOIN users u ON p.user_id = u.user_id JOIN shop_items s ON p.item_id = s.id "
                "WHERE p.status='pending' ORDER BY p.purchase_date"
            )
        if not rows:
            await message.answer("Нет необработанных покупок.")
            return
        for row in rows:
            pid, uid, username, item_name, date, status = row['id'], row['user_id'], row['username'], row['name'], row['purchase_date'], row['status']
            text = f"🆔 {pid}\nПользователь: {uid} (@{username})\nТовар: {item_name}\nДата: {date}"
            await message.answer(text, reply_markup=purchase_action_keyboard(pid))
    except Exception as e:
        logging.error(f"Admin purchases error: {e}")
        await message.answer("❌ Ошибка загрузки покупок.")

@dp.callback_query_handler(lambda c: c.data.startswith("purchase_done_"))
async def purchase_done(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    purchase_id = int(callback.data.split("_")[2])
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE purchases SET status='completed' WHERE id=$1", purchase_id)
            user_id = await conn.fetchval("SELECT user_id FROM purchases WHERE id=$1", purchase_id)
            if user_id:
                safe_send_message_task(user_id, "✅ Твоя покупка обработана! Админ выслал подарок.")
        await callback.answer("Покупка отмечена как выполненная")
        await callback.message.delete()
    except Exception as e:
        logging.error(f"Purchase done error: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith("purchase_reject_"))
async def purchase_reject(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    purchase_id = int(callback.data.split("_")[2])
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE purchases SET status='rejected' WHERE id=$1", purchase_id)
            user_id = await conn.fetchval("SELECT user_id FROM purchases WHERE id=$1", purchase_id)
            if user_id:
                safe_send_message_task(user_id, "❌ К сожалению, твоя покупка не может быть выполнена. Свяжись с админом.")
        await callback.answer("Покупка отклонена")
        await callback.message.delete()
    except Exception as e:
        logging.error(f"Purchase reject error: {e}")
        await callback.answer("Ошибка", show_alert=True)

# ===== ДОБАВЛЕНИЕ МЛАДШЕГО АДМИНА =====
@dp.message_handler(lambda message: message.text == "➕ Добавить админа")
async def add_admin_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("Только суперадмин может добавлять админов.")
        return
    await message.answer("Введи ID или @username пользователя, которого хочешь сделать младшим админом:", reply_markup=back_keyboard())
    await AddJuniorAdmin.user_id.set()

@dp.message_handler(state=AddJuniorAdmin.user_id)
async def add_admin_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO admins (user_id, added_by, added_date) VALUES ($1, $2, $3)",
                uid, message.from_user.id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        await message.answer(f"✅ Пользователь {uid} теперь младший админ.")
    except asyncpg.UniqueViolationError:
        await message.answer("❌ Этот пользователь уже админ.")
    except Exception as e:
        logging.error(f"Add admin error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== УДАЛЕНИЕ МЛАДШЕГО АДМИНА =====
@dp.message_handler(lambda message: message.text == "➖ Удалить админа")
async def remove_admin_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("Только суперадмин может удалять админов.")
        return
    await message.answer("Введи ID или @username пользователя, которого хочешь лишить прав админа:", reply_markup=back_keyboard())
    await RemoveJuniorAdmin.user_id.set()

@dp.message_handler(state=RemoveJuniorAdmin.user_id)
async def remove_admin_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM admins WHERE user_id=$1", uid)
        await message.answer(f"✅ Пользователь {uid} больше не админ, если был им.")
    except Exception as e:
        logging.error(f"Remove admin error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== БЛОКИРОВКА ПОЛЬЗОВАТЕЛЯ =====
@dp.message_handler(lambda message: message.text == "🔨 Заблокировать пользователя")
async def block_user_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID или @username пользователя для блокировки:", reply_markup=back_keyboard())
    await BlockUser.user_id.set()

@dp.message_handler(state=BlockUser.user_id)
async def block_user_id(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    if await is_admin(uid):
        await message.answer("❌ Нельзя заблокировать администратора.")
        await state.finish()
        return
    await state.update_data(user_id=uid)
    await message.answer("Введи причину блокировки (можно отправить 'нет'):")
    await BlockUser.reason.set()

@dp.message_handler(state=BlockUser.reason)
async def block_user_reason(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    reason = None if message.text.lower() == 'нет' else message.text
    data = await state.get_data()
    uid = data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO banned_users (user_id, banned_by, banned_date, reason) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO NOTHING",
                uid, message.from_user.id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), reason
            )
        await message.answer(f"✅ Пользователь {uid} заблокирован.")
        safe_send_message_task(uid, f"⛔ Вы заблокированы в боте. Причина: {reason if reason else 'не указана'}")
    except Exception as e:
        logging.error(f"Block user error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== РАЗБЛОКИРОВКА =====
@dp.message_handler(lambda message: message.text == "🔓 Разблокировать пользователя")
async def unblock_user_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID или @username пользователя для разблокировки:", reply_markup=back_keyboard())
    await UnblockUser.user_id.set()

@dp.message_handler(state=UnblockUser.user_id)
async def unblock_user_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM banned_users WHERE user_id=$1", uid)
        await message.answer(f"✅ Пользователь {uid} разблокирован.")
        safe_send_message_task(uid, "🔓 Вы разблокированы в боте.")
    except Exception as e:
        logging.error(f"Unblock user error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== СПИСАНИЕ МОНЕТ =====
@dp.message_handler(lambda message: message.text == "💸 Списать монеты")
async def remove_balance_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await RemoveBalance.user_id.set()

@dp.message_handler(state=RemoveBalance.user_id)
async def remove_balance_user(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи сумму списания (целое положительное число):")
    await RemoveBalance.amount.set()

@dp.message_handler(state=RemoveBalance.amount)
async def remove_balance_amount(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await update_user_balance(uid, -amount)
        await message.answer(f"✅ У пользователя {uid} списано {amount} монет.")
        safe_send_message_task(uid, f"💸 У тебя списано {amount} монет администратором.")
    except Exception as e:
        logging.error(f"Remove balance error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== НАЧИСЛЕНИЕ МОНЕТ =====
@dp.message_handler(lambda message: message.text == "💰 Начислить монеты")
async def add_balance_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await AddBalance.user_id.set()

@dp.message_handler(state=AddBalance.user_id)
async def add_balance_user(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи сумму начисления (целое положительное число):")
    await AddBalance.amount.set()

@dp.message_handler(state=AddBalance.amount)
async def add_balance_amount(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await update_user_balance(uid, amount)
        await message.answer(f"✅ Пользователю {uid} начислено {amount} монет.")
        safe_send_message_task(uid, f"💰 Вам начислено {amount} монет администратором.")
    except Exception as e:
        logging.error(f"Add balance error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== СБРОС СТАТИСТИКИ =====
@dp.message_handler(lambda message: message.text == "🔄 Сброс статистики")
async def reset_stats(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, сбросить всё", callback_data="reset_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="reset_cancel")]
    ])
    await message.answer("⚠️ Ты уверен? Это действие безвозвратно обнулит балансы, репутацию, потраченные монеты, покупки и статистику всех пользователей.", reply_markup=confirm_kb)

@dp.callback_query_handler(lambda c: c.data == "reset_confirm")
async def reset_confirm(callback: types.CallbackQuery):
    if not await is_super_admin(callback.from_user.id):
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET balance=0, reputation=0, total_spent=0, theft_attempts=0, theft_success=0, theft_failed=0, theft_protected=0, last_theft_time=NULL, negative_balance=0")
            await conn.execute("DELETE FROM purchases")
            await conn.execute("DELETE FROM user_tasks")
            await conn.execute("DELETE FROM referrals")
        await callback.message.edit_text("✅ Статистика сброшена.")
    except Exception as e:
        logging.error(f"Reset error: {e}")
        await callback.message.edit_text("❌ Ошибка при сбросе.")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "reset_cancel")
async def reset_cancel(callback: types.CallbackQuery):
    await callback.message.edit_text("Сброс отменён.")
    await callback.answer()

# ===== РАССЫЛКА =====
@dp.message_handler(lambda message: message.text == "📢 Рассылка")
async def broadcast_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Отправь сообщение для рассылки (текст, фото, видео или документ).", reply_markup=back_keyboard())
    await Broadcast.media.set()

@dp.message_handler(state=Broadcast.media, content_types=['text', 'photo', 'video', 'document'])
async def broadcast_media(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return

    content = {}
    if message.text:
        content['type'] = 'text'
        content['text'] = message.text
    elif message.photo:
        content['type'] = 'photo'
        content['file_id'] = message.photo[-1].file_id
        content['caption'] = message.caption or ""
    elif message.video:
        content['type'] = 'video'
        content['file_id'] = message.video.file_id
        content['caption'] = message.caption or ""
    elif message.document:
        content['type'] = 'document'
        content['file_id'] = message.document.file_id
        content['caption'] = message.caption or ""
    else:
        await message.answer("Неподдерживаемый тип.")
        return

    await state.finish()

    status_msg = await message.answer("⏳ Рассылка начата... Это может занять некоторое время.")

    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id FROM users")
        users = [r['user_id'] for r in users]

    sent = 0
    failed = 0
    total = len(users)

    for i, uid in enumerate(users):
        if await is_banned(uid):
            continue
        try:
            if content['type'] == 'text':
                await bot.send_message(uid, content['text'])
            elif content['type'] == 'photo':
                await bot.send_photo(uid, content['file_id'], caption=content['caption'])
            elif content['type'] == 'video':
                await bot.send_video(uid, content['file_id'], caption=content['caption'])
            elif content['type'] == 'document':
                await bot.send_document(uid, content['file_id'], caption=content['caption'])
            sent += 1
        except (BotBlocked, UserDeactivated, ChatNotFound):
            failed += 1
        except RetryAfter as e:
            logging.warning(f"Flood limit, waiting {e.timeout} seconds")
            await asyncio.sleep(e.timeout)
            try:
                if content['type'] == 'text':
                    await bot.send_message(uid, content['text'])
                else:
                    if content['type'] == 'photo':
                        await bot.send_photo(uid, content['file_id'], caption=content['caption'])
                    elif content['type'] == 'video':
                        await bot.send_video(uid, content['file_id'], caption=content['caption'])
                    elif content['type'] == 'document':
                        await bot.send_document(uid, content['file_id'], caption=content['caption'])
                sent += 1
            except:
                failed += 1
        except Exception as e:
            failed += 1
            logging.warning(f"Failed to send to {uid}: {e}")

        if (i + 1) % 10 == 0:
            try:
                await status_msg.edit_text(f"⏳ Прогресс: {i+1}/{total}\n✅ Отправлено: {sent}\n❌ Ошибок: {failed}")
            except:
                pass

        await asyncio.sleep(0.05)

    await status_msg.edit_text(f"✅ Рассылка завершена!\n📊 Отправлено: {sent}\n❌ Ошибок: {failed}\n👥 Всего: {total}")

# ===== НАЗАД В ГЛАВНОЕ МЕНЮ =====
@dp.message_handler(lambda message: message.text == "◀️ Назад в главное меню")
async def back_to_main_from_admin(message: types.Message):
    if message.chat.type != 'private':
        return
    admin_flag = await is_admin(message.from_user.id)
    await message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))

@dp.message_handler(lambda message: message.text == "◀️ Назад")
async def back_from_submenu(message: types.Message):
    if message.chat.type != 'private':
        return
    admin_flag = await is_admin(message.from_user.id)
    await message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))

# ===== ОБРАБОТКА НЕИЗВЕСТНЫХ СООБЩЕНИЙ =====
@dp.message_handler()
async def unknown_message(message: types.Message):
    if message.chat.type != 'private':
        return
    if await is_banned(message.from_user.id) and not await is_admin(message.from_user.id):
        return
    admin_flag = await is_admin(message.from_user.id)
    await message.answer("Я не понимаю эту команду. Используй кнопки меню.", reply_markup=user_main_keyboard(admin_flag))

# ===== ВЕБ-СЕРВЕР =====
async def handle(request):
    return web.Response(text="Bot is running")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Web server started on port {port}")

# ===== ФОНОВЫЕ ЗАДАЧИ =====
async def check_expired_giveaways():
    while True:
        await asyncio.sleep(600)
        try:
            async with db_pool.acquire() as conn:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                await conn.execute("UPDATE giveaways SET status='completed' WHERE status='active' AND end_date < $1", now)
                if await get_setting("chat_notify_giveaway") == "1":
                    soon = (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
                    rows = await conn.fetch("SELECT id, prize, end_date FROM giveaways WHERE status='active' AND end_date < $1 AND end_date > $2 AND notified=FALSE",
                                            soon, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    for row in rows:
                        time_left = (datetime.strptime(row['end_date'], "%Y-%m-%d %H:%M:%S") - datetime.now()).seconds // 60
                        msg = random.choice(CHAT_GIVEAWAY_PHRASES).format(prize=row['prize'], time=f"{time_left} мин")
                        await notify_chats(msg, 'giveaway')
                        await conn.execute("UPDATE giveaways SET notified=TRUE WHERE id=$1", row['id'])
        except Exception as e:
            logging.error(f"Expired giveaways check error: {e}")

async def check_task_expirations():
    while True:
        await asyncio.sleep(600)
        try:
            async with db_pool.acquire() as conn:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                rows = await conn.fetch(
                    "SELECT ut.user_id, ut.task_id, t.penalty_days, t.reward_coins, t.reward_reputation, t.target_id "
                    "FROM user_tasks ut JOIN tasks t ON ut.task_id = t.id "
                    "WHERE ut.expires_at < $1 AND ut.status='completed' AND t.penalty_days > 0",
                    now
                )
                for row in rows:
                    user_id = row['user_id']
                    task_id = row['task_id']
                    penalty_days = row['penalty_days']
                    reward_coins = row['reward_coins']
                    reward_reputation = row['reward_reputation']
                    target_id = row['target_id']

                    try:
                        member = await bot.get_chat_member(chat_id=target_id, user_id=user_id)
                        if member.status in ['left', 'kicked']:
                            async with conn.transaction():
                                await update_user_balance(user_id, -reward_coins)
                                await update_user_reputation(user_id, -reward_reputation)
                                await conn.execute("UPDATE user_tasks SET status='penalty' WHERE user_id=$1 AND task_id=$2", user_id, task_id)
                            await safe_send_message(user_id, f"⚠️ Ты отписался от канала, поэтому награда за задание списана. Текущий баланс: {await get_user_balance(user_id)}")
                    except Exception as e:
                        logging.error(f"Task penalty check error: {e}")
        except Exception as e:
            logging.error(f"Task expiration check error: {e}")

# ===== ЗАПУСК =====
async def on_startup(dp):
    await before_start()
    await create_db_pool()
    await init_db()
    asyncio.create_task(check_expired_giveaways())
    asyncio.create_task(check_task_expirations())
    asyncio.create_task(start_web_server())
    logging.info("🤖 Бот запущен и готов к работе!")
    logging.info(f"👑 Суперадмины: {SUPER_ADMINS}")
    logging.info(f"🗄 База данных: PostgreSQL")

async def on_shutdown(dp):
    await db_pool.close()
    await storage.close()
    await dp.storage.close()
    await bot.close()
    logging.info("Бот остановлен")

if __name__ == "__main__":
    while True:
        try:
            executor.start_polling(dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown)
        except TerminatedByOtherGetUpdates:
            logging.error("Конфликт с другим экземпляром. Жду 5 сек...")
            time.sleep(5)
            continue
        except Exception as e:
            logging.error(f"Критическая ошибка: {e}")
            time.sleep(5)
            continue
