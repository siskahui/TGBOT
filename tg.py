import asyncio
import re
import aiohttp
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.types import BotCommand, BotCommandScopeDefault
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv
from collections import deque
import tempfile
import shutil
import datetime
import json
import os
import logging
import pickle
import time  # used for cache timestamps
from collections import deque, OrderedDict, Counter, defaultdict

# ----------------- CONFIG -----------------
load_dotenv("/root/TGBOT/.env") # файл .env с записанным токеном, путь к файлу
TOKEN = os.getenv("RELEASE_TOKEN") # внутри .env RELEASE_TOKEN=токен бота
BASE_URL = "https://lk.ks.psuti.ru/?mn=2&obj=218"
BOT_OWNER_ID = int(os.getenv("OWNERID")) #ID ТГ Аккаунта
USER_FILE = "users.json" #Куда сохраняются пользователи
GROUPS_FILE = "groups.json" #Где хранится список групп + курсы
SELECTION_FILE = "selections.json" #Закешированный выбор юзеров
CURRENT_WK_CACHE: dict = {"wk": 323, "ts": 0.0}
callback_cooldown = {}
CALLBACK_DELAY = 1.0  # Настройка антифлуда на кнопки
forward_queue = deque()
user_message_cooldown = {}
USER_DELAY = 1.0  # Антифлуд на сообщения от одного юзера

# CACHE config
CACHE_TTL_SECONDS = 300  # TTL Кеша
CACHE_FILE = "page_cache.pkl"  # Бэкап
FETCH_SEMAPHORE_LIMIT = 10
MAX_CACHE_AGE_DAYS = 1 #Время хранения кеша в бекапе

# LOCKS LRU config
LOCKS_CACHE_MAX = 1000  # URL локи

#-------------------АНТИФЛУД------------------------------
def is_flood(user_id: int) -> bool:
    now = time.time()
    last = callback_cooldown.get(user_id, 0)

    if now - last < CALLBACK_DELAY:
        return True

    callback_cooldown[user_id] = now
    return False

class CallbackAntiFloodMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if hasattr(event, "from_user"):
            user_id = event.from_user.id

            if is_flood(user_id):
                if hasattr(event, "answer"):
                    await event.answer("Лелеле тише ковбой")
                return

        try:
            return await handler(event, data)
        except TelegramBadRequest as e:
            if "query is too old" in str(e).lower() or "response timeout expired" in str(e).lower():
                logger.warning(f"⚠️ Старый callback от юзера {user_id} (игнорируем)")
                return
            raise

def is_user_spamming(user_id: int) -> bool:
    now = time.time()
    last = user_message_cooldown.get(user_id, 0)

    if now - last < USER_DELAY:
        return True

    user_message_cooldown[user_id] = now
    return False

# ----------------- LOGGING -----------------
from logging.handlers import RotatingFileHandler

logger = logging.getLogger("TGBot")
logger.setLevel(logging.INFO)

# Формат: Дата | Уровень | Файл:Строка | Сообщение
formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s')

# 1. Хэндлер для записи в файл (макс 5 МБ, храним 5 старых копий)
file_handler = RotatingFileHandler('bot_log.log', maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# 2. Хэндлер для вывода в консоль
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ----------------- STATE ----------------------
bot = Bot(TOKEN)
dp = Dispatcher()
dp.callback_query.middleware(CallbackAntiFloodMiddleware())
START_TIME = time.time()
TOTAL_REQUESTS = 0

# ----------------- БОТ МЕНЮ -------------------
async def set_bot_commands():
    commands = [
        BotCommand(command="start",     description="Начать / выбрать группу"),
        BotCommand(command="today",  description="Расписание на сегодня"),
        BotCommand(command="week",      description="Расписание на эту неделю"),
    ]

    try:
        await bot.set_my_commands(
            commands=commands,
            scope=BotCommandScopeDefault()
        )
        logger.info("✅ Команды бота успешно установлены")
    except Exception as e:
        logger.error(f"Не удалось установить команды бота: {e}")

class UserActivityMiddleware:
    async def __call__(self, handler, event, data):
        global TOTAL_REQUESTS
        TOTAL_REQUESTS += 1
        logger.info(f"👤 Юзер {event.from_user.id} (@{event.from_user.username}) -> действие: {type(event).__name__}")
        if hasattr(event, "from_user") and event.from_user is not None:
            update_user_activity(
                event.from_user.id,
                event.from_user.username
            )
        return await handler(event, data)

dp.message.middleware(UserActivityMiddleware())
dp.callback_query.middleware(UserActivityMiddleware())

current_wk_per_chat: dict[int, int] = {}
last_msg_per_chat: dict[int, int] = {}
last_text_per_chat: dict[int, str] = {}
forward_queue: asyncio.Queue = asyncio.Queue()

locks: dict[int, asyncio.Lock] = {}

selected_course_per_chat: dict[int, str] = {}
selected_group_per_chat: dict[int, str] = {}

waiting_for_schedule_time: set[int] = set()
waiting_for_broadcast: set[int] = set()
waiting_for_supp_id: set[int] = set()
admin_panel_msg_id: dict[int, int] = {}

last_sent_today: dict[int, str] = {}

# Global aiohttp session (new)
_shared_session: aiohttp.ClientSession | None = None

# ----------------- STORAGE -----------------
def load_json_file(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_json_file(path: str, data: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_users():
    """Загружает всех пользователей из файла целиком"""
    try:
        raw = load_json_file(USER_FILE)
        # Убеждаемся, что все ключи — строки для консистентности JSON
        return {str(k): v for k, v in raw.items()}
    except Exception as e:
        logger.error(f"Ошибка загрузки пользователей: {e}")
        return {}

def save_users(users):
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as tmp:
            json.dump(users, tmp, ensure_ascii=False, indent=2)
            tmp.flush()
            os.fsync(tmp.fileno())
        
        # Атомарная замена
        shutil.move(tmp.name, USER_FILE)
    except Exception as e:
        logger.error(f"Ошибка атомарного сохранения users: {e}")
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)

# Инициализация хранилища
user_store = load_users()
user_store = {str(k): v for k, v in user_store.items()}

def update_user_activity(user_id: int, username: str | None):
    uid = str(user_id)

    if uid not in user_store:
        user_store[uid] = {}

    # Обновляем только конкретные поля, не трогая schedule_time
    user_store[uid]["username"] = username or user_store[uid].get("username", "без ника")
    user_store[uid]["last_activity"] = time.time()

def add_user(uid, username):
    if uid not in user_store:
        user_store[uid] = username or "без ника"
        save_users(user_store)

def register_user_from_message(msg):
    try:
        user = msg.from_user
        update_user_activity(user.id, user.username)
    except:
        pass

groups: dict[str, dict] = load_json_file(GROUPS_FILE)

async def periodic_save():
    while True:
        await asyncio.sleep(1200) # 20 минут
        try:
            save_users(user_store)
            logger.info("💾 Автосохранение пользователей выполнено")
        except Exception as e:
            logger.error(f"❌ Ошибка при плановом сохранении: {e}")
# ----------------- SELECTION STORAGE -----------------
def _load_cache_file():
    global _cache, CURRENT_WK_CACHE
    if not os.path.exists(CACHE_FILE):
        _cache = {}
        return
    try:
        with open(CACHE_FILE, "rb") as f:
            data = pickle.load(f)
        _cache = data.get("page_cache", {})
        CURRENT_WK_CACHE = data.get("wk_cache", CURRENT_WK_CACHE)

        cutoff = time.time() - (MAX_CACHE_AGE_DAYS * 86400)
        old = [k for k, (ts, _) in _cache.items() if ts < cutoff]
        for k in old:
            del _cache[k]
        if old:
            logger.info(f"🧹 Кеш: удалено {len(old)} старых записей (> {MAX_CACHE_AGE_DAYS} дней)")
    except Exception as e:
        logger.warning(f"⚠️ Кеш не загрузился: {e}")
        _cache = {}

def _save_cache_file():
    try:
        data = {"page_cache": _cache, "wk_cache": CURRENT_WK_CACHE}
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as tmp:
            pickle.dump(data, tmp)
            tmp.flush()
            os.fsync(tmp.fileno())
        shutil.move(tmp.name, CACHE_FILE)
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения кеша: {e}")

def load_selections():
    global selected_course_per_chat, selected_group_per_chat
    data = load_json_file(SELECTION_FILE)

    selected_course_per_chat = {
        int(k): v for k, v in data.get("course", {}).items()
    }

    selected_group_per_chat = {
        int(k): v for k, v in data.get("group", {}).items()
    }

def save_selections():
    data = {
        "course": {str(k): v for k, v in selected_course_per_chat.items()},
        "group": {str(k): v for k, v in selected_group_per_chat.items()},
    }
    save_json_file(SELECTION_FILE, data)

if os.path.exists(SELECTION_FILE):
    load_selections()

# ----------------- HELPERS -----------------
def courses_list():
    vals = {str(info.get("course", "")) for info in groups.values()}
    return sorted(v for v in vals if v)

def groups_for_course(course):
    return sorted(name for name, info in groups.items() if str(info.get("course")) == str(course))

def build_url_for_wk(wk: int | None, chat_id: int) -> str:
    """Формирует URL с учётом выбранной группы и недели"""
    base = BASE_URL

    grp = selected_group_per_chat.get(chat_id)
    if grp and grp in groups:
        base = f"https://lk.ks.psuti.ru/?mn=2&obj={groups[grp]['obj']}"

    if wk is None:
        return base  # без wk — сайт сам покажет текущую

    return f"{base}&wk={wk}"

def get_current_monday_ts() -> float:
    now = datetime.datetime.now()
    days_to_monday = now.weekday()          # 0 = понедельник
    monday = now - datetime.timedelta(days=days_to_monday)
    monday_midnight = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    return monday_midnight.timestamp()

def get_stats_text() -> str:
    uptime_seconds = int(time.time() - START_TIME)
    uptime_str = str(datetime.timedelta(seconds=uptime_seconds))
    cache_size = len(_cache)
    total_users = len(user_store)
    if selected_group_per_chat:
        most_popular_group, group_count = Counter(selected_group_per_chat.values()).most_common(1)[0]
    else:
        most_popular_group, group_count = "Нет данных", 0
    total_reqs = TOTAL_REQUESTS
    last_user_name = "Нет данных"
    last_time = 0
    for uid_str, info in user_store.items():
        if int(uid_str) == BOT_OWNER_ID:
            continue
        user_time = info.get("last_activity", 0)
        if user_time > last_time:
            last_time = user_time
            last_user_name = info.get("username", "без_ника")
    last_use_text = f"@{last_user_name} ({datetime.datetime.fromtimestamp(last_time).strftime('%d.%m.%Y %H:%M:%S')})" if last_time > 0 else "Никто еще не пользовался"
    active_schedules = sum(1 for info in user_store.values() if "schedule_time" in info)
    today_iso = datetime.date.today().isoformat()
    today_sent = sum(1 for info in user_store.values() if info.get("last_sent_date") == today_iso)
    last_sent_info = None
    for uid_str, info in user_store.items():
        if info.get("last_sent_date") == today_iso and "last_sent_time" in info:
            if last_sent_info is None or info["last_sent_time"] > last_sent_info[1]:
                last_sent_info = (info.get("username", "без ника"), info["last_sent_time"], uid_str)
    last_sent_text = f"@{last_sent_info[0]} в {last_sent_info[1]}" if last_sent_info else "Сегодня ещё не было"
    return (
        f"📊 <b>Статистика бота</b>\n\n"
        f"⏱ <b>Время работы:</b> {uptime_str}\n"
        f"📦 <b>Размер кеша:</b> {cache_size} стр.\n"
        f"👥 <b>Всего юзеров:</b> {total_users}\n"
        f"🏆 <b>Популярная группа:</b> {most_popular_group} ({group_count} чел.)\n"
        f"📈 <b>Всего запросов:</b> {total_reqs}\n"
        f"👤 <b>Последний активный:</b> {last_use_text}\n"
        f"🔔 <b>Подключено рассылок:</b> {active_schedules}\n"
        f"📨 <b>Рассылок отправлено сегодня:</b> {today_sent}\n"
        f"⏰ <b>Последняя рассылка:</b> {last_sent_text}\n"
    )


def get_users_list_text() -> str:
    if not user_store:
        return "Пользователи ещё не зарегистрированы"
    lines = []
    for uid, info in sorted(user_store.items(), key=lambda x: int(x[0])):
        ts = info.get("last_activity", 0)
        last_seen = f" (последняя активность: {datetime.datetime.fromtimestamp(ts).strftime('%d.%m.%Y %H:%M:%S')})" if ts > 0 else " (активность неизвестна)"
        lines.append(f"{uid} — @{info.get('username', 'без ника')}{last_seen}")
    return "📋 Сохранённые пользователи:\n\n" + "\n".join(lines)


def get_schedule_list_text() -> str:
    lines = []
    for uid_str, info in sorted(user_store.items(), key=lambda x: x[1].get("schedule_time", "99:99")):
        if "schedule_time" in info:
            username = info.get("username", "без ника")
            group = selected_group_per_chat.get(int(uid_str), "не выбрана")
            time_ = info["schedule_time"]
            lines.append(f"• {uid_str} — @{username} → <b>{time_}</b> (группа: {group})")
    if lines:
        return f"🔔 <b>Активные рассылки ({len(lines)}):</b>\n\n" + "\n".join(lines)
    return "📭 Нет подключённых рассылок"
# ----------------- INLINE KEYBOARDS -----------------
def make_inline_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
		InlineKeyboardButton(text="📆 сегодня", callback_data="day_today"),
                InlineKeyboardButton(text="🔁 обновить", callback_data="wk_refresh")
            ],
            [
                InlineKeyboardButton(text="⬅️ предыдущая", callback_data="wk_prev"),
		InlineKeyboardButton(text="📅 эта неделя", callback_data="wk_this"),
                InlineKeyboardButton(text="➡️ следующая", callback_data="wk_next")
            ],
            [
		InlineKeyboardButton(text="📨 рассылка", callback_data="setup_schedule"),
                InlineKeyboardButton(text="⚙️ сменить группу", callback_data="change_group")
            ]
        ]
    )

def build_schedule_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔙 Назад", callback_data="schedule_back"),
                InlineKeyboardButton(text="❌ Отменить рассылку", callback_data="schedule_disable")
            ]
        ]
    )

def build_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")],
            [InlineKeyboardButton(text="🔔 Рассылки", callback_data="admin_schedules")],
            [InlineKeyboardButton(text="📢 Оповещение", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="🛠 Саппорт мод", callback_data="admin_supp")],
            [InlineKeyboardButton(text="🧹 Очистить last sent", callback_data="admin_clear_sent")],
            [InlineKeyboardButton(text="🔍 Debug недели", callback_data="admin_debug_week")],
        ]
    )

def build_courses_kb():
    rows = []

    for c in courses_list():
        rows.append([
            InlineKeyboardButton(text=f"{c} курс", callback_data=f"course_{c}")
        ])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_groups_kb(groups_list):
    rows = []
    row = []

    for g in groups_list:
        row.append(
            InlineKeyboardButton(text=g, callback_data=f"group_{g}")
        )

        if len(row) == 3:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    return InlineKeyboardMarkup(inline_keyboard=rows)

# ----------------- HTML PARSER -------------------------
def parse_schedule_pretty(html: str) -> str:
    if not html or not html.strip():
        return "⚠️ Расписание не загрузилось"

    soup = BeautifulSoup(html, "html.parser")
    result = []
    last_day = None

    for tr in soup.find_all("tr"):
        # === День недели ===
        h3 = tr.find("h3")
        if h3:
            day = " ".join(h3.get_text(" ", strip=True).split())
            if any(word in day.lower() for word in ["предыдущая", "выберите", "курс:"]):
                continue
            if day != last_day:
                if result:                     # отступ перед новым днём
                    result.append("\n")
                result.append(f"📅 <b>{day}</b>\n\n")   # ← вот тут добавили пустую строку
                last_day = day
            continue

        tds = tr.find_all("td", recursive=False)
        if len(tds) < 7 or not tds[0].get_text(strip=True).isdigit():
            continue

        num = tds[0].get_text(strip=True)
        time_str = " ".join(tds[1].stripped_strings).strip()
        way = tds[2].get_text(strip=True).strip()

        # === ЯЧЕЙКА ДИСЦИПЛИНЫ (главная фикса замен) ===
        disc_lines = [line.strip() for line in tds[3].stripped_strings if line.strip()]

        replacement_note = ""
        subject = ""
        teacher = ""
        address = ""
        room = ""

        # Находим "на:" и берём всё после него как реальную пару
        after_na_index = -1
        for i, line in enumerate(disc_lines):
            if "на:" in line.lower():
                after_na_index = i + 1
                replacement_note = " ".join(disc_lines[:i])
                break

        # Если нашли "на:", парсим только часть после него
        lines_to_parse = disc_lines[after_na_index:] if after_na_index != -1 else disc_lines

        for line in lines_to_parse:
            low = line.lower()
            if not subject and not low.startswith("(") and not any(k in low for k in ["шоссе", "кабинет", "ауд"]):
                subject = line
            elif not teacher and ("(" in line or len(line.split()) >= 2):
                teacher = line.strip("() ")
            elif any(k in low for k in ["шоссе", "ул.", "московское"]):
                address = line
            elif "кабинет" in low or "ауд" in low:
                room = line

        # Если после замены "Свободное время" — ставим 💤
        if "свободное время" in " ".join(lines_to_parse).lower():
            subject = "Свободное время"

        # === Ресурсы (все ссылки) ===
        resources = []
        if len(tds) > 5:
            for a in tds[5].find_all("a", href=True):
                url = a["href"].strip()
                txt = a.get_text(strip=True).strip() or "Ссылка"
                resources.append((txt, url))
            if not resources:
                txt = tds[5].get_text(strip=True).strip()
                if txt:
                    resources.append((txt, ""))

        # === Тема и задание ===
        theme = tds[4].get_text(strip=True).strip() if len(tds) > 4 else ""
        task = tds[6].get_text(strip=True).strip() if len(tds) > 6 else ""

        # === Вывод пары ===
        line = f"▫️ <b>{num}</b> | {time_str}"
        if way:
            line += f" <i>({way})</i>"
        line += "\n"

        # === ЗАМЕНА — на новой строке + слово "на" + отступ после ===
        if replacement_note:
            note = replacement_note.strip()
            if not note.lower().endswith("на"):
                note += " на"
            line += f"   <b>⚠️ {note}</b>\n\n"   # ← вот тут добавил пустую строку

        if subject:
            if "свободное" in subject.lower():
                line += f"   💤 {subject}\n"
            else:
                line += f"   📚 {subject}\n"
                if teacher:
                    line += f"   👤 {teacher}\n"
                if address:
                    line += f"   🏢 {address}\n"
                if room:
                    line += f"   🚪 {room}\n"

        if theme:
            line += f"   📝 <b>Тема:</b> {theme}\n"

        for txt, url in resources:
            if url:
                line += f'   🔗 <a href="{url}">{txt}</a>\n'
            else:
                line += f"   🔗 {txt}\n"

        if task:
            line += f"   📌 Задание: {task}\n"

        line += "\n"   # отступ между парами
        result.append(line)

    text = "".join(result).strip()
    return text or "⚠️ Расписание пустое"

# ----------------- SCHEDULE SENDER (РАССЫЛКА) -----------------
def extract_today(schedule_text: str) -> str:
    days = [
        "понедельник", "вторник", "среда", "четверг",
        "пятница", "суббота", "воскресенье"
    ]
    today = days[datetime.datetime.now().weekday()]

    blocks = schedule_text.split("📅")
    for block in blocks:
        if today in block.lower():
            return "📅 " + block.strip()
    return "📅 Сегодня занятий нет (или день не найден в расписании)"

def has_classes_today(week_text: str) -> bool:
    today_text = extract_today(week_text)
    # Отправляем ВСЕГДА, если блок дня есть
    return len(today_text.strip()) > 10 and "не найден" not in today_text.lower()

async def schedule_sender():
    while True:
        try:
            now_dt = datetime.datetime.now()
            now_str = now_dt.strftime("%H:%M")
            today_iso = datetime.date.today().isoformat()

            # === Кто сейчас должен получить рассылку ===
            active_users = [
                uid for uid, info in user_store.items()
                if info.get("schedule_time") == now_str
            ]

            if not active_users:
                await asyncio.sleep(60 - now_dt.second)
                continue

            logger.info(f"🔄 Рассылка: {len(active_users)} активных в {now_str}")

            wk = await get_current_wk()

            # === Группируем по URL (один fetch на группу) ===
            url_to_uids: dict[str, list[str]] = defaultdict(list)
            for uid_str in active_users:
                uid_int = int(uid_str)
                grp = selected_group_per_chat.get(uid_int)
                if not grp or grp not in groups:
                    continue
                obj = groups[grp]["obj"]
                url = f"https://lk.ks.psuti.ru/?mn=2&obj={obj}&wk={wk}"
                url_to_uids[url].append(uid_str)

            sent_count = 0
            sent_this_run = set()  # защита от двойного счёта

            for url, uid_list in url_to_uids.items():
                # Убираем возможные дубли (на всякий случай)
                unique_uids = list(dict.fromkeys(uid_list))  # сохраняет порядок

                async with _shared_session.get(url, timeout=15) as resp:
                    html = await resp.text()

                # Парсим один раз на группу
                week_text = parse_schedule_pretty(html)
                today_text = extract_today(week_text)

                # Пропускаем всю группу, если нет занятий
                lower_text = today_text.lower()
                if "занятий нет" in lower_text or "день не найден" in lower_text or "сегодня занятий нет" in lower_text:
                    logger.info(f"⏭️ Пропуск группы ({len(unique_uids)} чел.) — нет занятий сегодня")
                    continue

                logger.info(f"📨 Отправляем группе ({len(unique_uids)} чел.): {url}")

                for uid_str in unique_uids:
                    uid_int = int(uid_str)
                    info = user_store.get(uid_str)
                    if not info or info.get("last_sent_date") == today_iso:
                        continue

                    if uid_int in sent_this_run:
                        continue  # защита от двойной отправки

                    group_name = selected_group_per_chat.get(uid_int, "не выбрана")
                    msg_text = (
                        f"👤 <b>Ваша группа:</b> {group_name}\n"
                        f"🔔 <b>Расписание на сегодня ({now_str})</b>\n\n"
                        f"{today_text}"
                    )

                    try:
                        sent_msg = await bot.send_message(
                            uid_int, msg_text,
                            parse_mode=ParseMode.HTML,
                            reply_markup=make_inline_kb()
                        )
                        last_msg_per_chat[uid_int] = sent_msg.message_id
                        last_text_per_chat[uid_int] = msg_text

                        sent_count += 1
                        sent_this_run.add(uid_int)
                        info["last_sent_time"] = now_str
                        info["last_sent_date"] = today_iso

                        logger.debug(f"✅ Отправлено {uid_int} (sent_count = {sent_count})")

                    except Exception as e:
                        logger.error(f"❌ Рассылка {uid_int}: {e}")

            # Сохраняем только если что-то отправили
            if sent_count > 0:
                save_users(user_store)
                logger.info(f"📊 Рассылка завершена. Отправлено: {sent_count}")

        except Exception as e:
            logger.error(f"❌ Критическая ошибка в schedule_sender: {type(e).__name__}: {e}", exc_info=True)

        # Ждём ровно до следующей минуты
        await asyncio.sleep(60 - datetime.datetime.now().second)

# ----------------- FETCH (basic) -----------------
async def fetch_page_once(session, url):
    start_time = time.perf_counter() # Засекаем время
    try:
        async with session.get(url, timeout=10) as r:
            status = r.status
            content = await r.text()
            duration = time.perf_counter() - start_time
            
            if status == 200:
                logger.info(f"🌐 [HTTP 200] OK за {duration:.2f}s | URL: {url}")
            else:
                logger.warning(f"⚠️ [HTTP {status}] Нетипичный ответ за {duration:.2f}s | URL: {url}")
                
            return content
    except asyncio.TimeoutError:
        logger.error(f"🕒 Timeout! Сайт не ответил за 10с | URL: {url}")
        return ""
    except Exception as e:
        logger.error(f"❌ Ошибка запроса: {type(e).__name__}: {e} | URL: {url}")
        return ""

# ----------------- CACHE -----------------
_cache: dict[str, tuple[float, str]] = {}
_locks_per_url: "OrderedDict[str, asyncio.Lock]" = OrderedDict()
_fetch_semaphore = asyncio.Semaphore(FETCH_SEMAPHORE_LIMIT)

def _clean_old_cache():
    """Удаляет из кэша записи, которые старше MAX_CACHE_AGE_DAYS."""
    global _cache
    now = time.time()
    max_age_seconds = MAX_CACHE_AGE_DAYS * 24 * 3600

    initial_size = len(_cache)
    _cache = {
        url: data for url, data in _cache.items()
        if (now - data[0]) < max_age_seconds
    }

    if len(_cache) < initial_size:
        logger.info("♻️ Кэш очищен: удалено %d старых записей", initial_size - len(_cache))

def _load_cache_file():
    if not os.path.exists(CACHE_FILE):
        return
    try:
        with open(CACHE_FILE, "rb") as f:
            data = pickle.load(f)
            if isinstance(data, dict):
                _cache.update(data)
                logger.info("✅ Загружен кэш из файла: %d записей", len(data))
    except Exception as e:
        logger.warning("❌ Не удалось загрузить кэш-файл: %s", e)

def _save_cache_file():
    try:
        _clean_old_cache()

        with open(CACHE_FILE + ".tmp", "wb") as f:
            pickle.dump(_cache, f)
        os.replace(CACHE_FILE + ".tmp", CACHE_FILE)
        logger.info("💾 Кэш сохранён в файл (%d записей)", len(_cache))
    except Exception as e:
        logger.warning("❌ Не удалось сохранить кэш-файл: %s", e)

# === ВЫЗОВ ПРИ СТАРТЕ (было обрезано) ===
_load_cache_file()          # ← вот это было потеряно
logger.info("🗄️ Кэш страниц инициализирован (загружено %d записей)", len(_cache))

# ----------------- АВТООПРЕДЕЛЕНИЕ НЕДЕЛИ -----------------
async def get_current_wk() -> int:
    global CURRENT_WK_CACHE
    now = time.time()
    monday_ts = get_current_monday_ts()

    # Если кэш старше начала этой недели — обновляем
    if CURRENT_WK_CACHE["ts"] < monday_ts:
        # Обновляем через любую группу (ИСПП-34)
        url = "https://lk.ks.psuti.ru/?mn=2&obj=218"
        html = await get_cached_page(_shared_session, url)

        if html:
            soup = BeautifulSoup(html, "html.parser")
            # Ищем ссылку "следующая неделя"
            next_link = soup.find("a", string=lambda text: text and "следующая неделя" in text.lower())
            if next_link and "wk=" in next_link.get("href", ""):
                try:
                    next_wk = int(next_link["href"].split("wk=")[-1].split("&")[0])
                    wk = next_wk - 1
                    CURRENT_WK_CACHE = {"wk": wk, "ts": now}
                    logger.info(f"🔄 Определена текущая неделя: {wk} (обновлено с понедельника 00:00)")
                    return wk
                except Exception:
                    pass

        # Fallback — не обновляем ts, чтобы следующая итерация снова попыталась
        logger.warning("Не удалось определить wk, используем старый кэш до успешного обновления")
        return CURRENT_WK_CACHE["wk"]

    # Кэш ещё актуален для этой недели
    return CURRENT_WK_CACHE["wk"]

def _get_lock_for_url(url: str) -> asyncio.Lock:
    lock = _locks_per_url.pop(url, None)
    if lock:
        _locks_per_url[url] = lock
        return lock
    lock = asyncio.Lock()
    _locks_per_url[url] = lock
    if len(_locks_per_url) > LOCKS_CACHE_MAX:
        try:
            _locks_per_url.popitem(last=False)
        except Exception:
            pass
    return lock

async def get_cached_page(session, url):
    now = time.time()
    cached = _cache.get(url)
    if cached:
        ts, val = cached
        if now - ts < CACHE_TTL_SECONDS:
            return val

    lock = _get_lock_for_url(url)

    async with lock:
        cached = _cache.get(url)
        if cached:
            ts, val = cached
            if now - ts < CACHE_TTL_SECONDS:
                return val

        async with _fetch_semaphore:
            delay = 0.5
            for attempt in range(3):
                html = await fetch_page_once(session, url)
                if html:
                    _cache[url] = (time.time(), html)
                    if len(_cache) % 50 == 0:
                        _save_cache_file()
                    return html
                await asyncio.sleep(delay)
                delay *= 2

        _cache[url] = (time.time(), "")
        return ""

# ----------------- SEND MESSAGE / EDIT -----------------
async def send_or_edit_text(text: str, chat_id: int):
    if not text or not text.strip():
        text = "⚠️ Расписание не загрузилось. Нажмите «🔁 обновить»"

    MAX_LEN = 3900

    # Если короткое — одно сообщение
    if len(text) <= MAX_LEN:
        last_id = last_msg_per_chat.get(chat_id)
        last_text = last_text_per_chat.get(chat_id)

        try:
            if last_id and text == last_text:
                return

            if last_id:
                await bot.edit_message_text(
                    text=text,
                    chat_id=chat_id,
                    message_id=last_id,
                    parse_mode=ParseMode.HTML,
                    reply_markup=make_inline_kb(),
                    disable_web_page_preview=True
                )
                last_text_per_chat[chat_id] = text
                return
        except Exception:
            last_msg_per_chat.pop(chat_id, None)
            last_text_per_chat.pop(chat_id, None)

    # === Делим по дням правильно ===
    # Разделяем по "📅 ", но первый блок (с группой) оставляем как есть
    parts = text.split("📅 ")
    header = parts[0].strip()                    # 👤 Ваша группа: ИСПИ-7
    days = ["📅 " + p.strip() for p in parts[1:] if p.strip()]

    messages = []
    current = header

    for day in days:
        if len(current) + len(day) + 2 > MAX_LEN and current != header:
            messages.append(current.strip())
            current = day
        else:
            current += "\n\n" + day

    if current.strip():
        messages.append(current.strip())

    # Отправляем все кроме последнего без кнопок
    for part in messages[:-1]:
        await bot.send_message(
            chat_id=chat_id,
            text=part,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )

    # Последнее сообщение с кнопками
    final_text = messages[-1]
    msg = await bot.send_message(
        chat_id=chat_id,
        text=final_text,
        parse_mode=ParseMode.HTML,
        reply_markup=make_inline_kb(),
        disable_web_page_preview=True
    )

    last_msg_per_chat[chat_id] = msg.message_id
    last_text_per_chat[chat_id] = text

# ----------------- SHOW WEEK -----------------
async def handle_show_week(message, wk, reply_message):
    chat_id = message.chat.id
    html = await get_cached_page(_shared_session, build_url_for_wk(wk, chat_id))

    text = parse_schedule_pretty(html)
    group = selected_group_per_chat.get(chat_id, "не выбрана")
    text = f"👤 <b>Ваша группа:</b> {group}\n\n{text}"

    await send_or_edit_text(text, chat_id)

# ----------------- БОТ КОМАНДЫ -----------------
@dp.message(Command("start"))
async def start(message: Message):
    register_user_from_message(message)
    update_user_activity(message.from_user.id, message.from_user.username)

    chat_id = message.chat.id

    last_msg_per_chat.pop(chat_id, None)
    last_text_per_chat.pop(chat_id, None)

    await bot.send_message(
        chat_id,
        "ТГ БОТ РАСПИСАНИЕ ПГУТИ\n\nhttps://github.com/Sp0nge-bob/TGBOT",
        parse_mode=ParseMode.HTML,
        reply_markup=make_inline_kb()
    )

@dp.message(Command("schedule", "today"))
async def cmd_schedule_today(message: Message):
    chat_id = message.chat.id
    group = selected_group_per_chat.get(chat_id)
    
    if not group:
        await message.answer("⚠️ Сначала выбери группу командой /start")
        return

    wk = await get_current_wk()
    current_wk_per_chat[chat_id] = wk

    url = build_url_for_wk(wk, chat_id)
    html = await get_cached_page(_shared_session, url)
    
    week_text = parse_schedule_pretty(html)
    today_text = extract_today(week_text)

    text = f"👤 <b>Ваша группа:</b> {group}\n\n{today_text}"

    # Отправляем + сохраняем для кнопок «обновить / неделя»
    msg = await message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=make_inline_kb()
    )
    
    last_msg_per_chat[chat_id] = msg.message_id
    last_text_per_chat[chat_id] = text


@dp.message(Command("week"))
async def cmd_week(message: Message):
    chat_id = message.chat.id
    group = selected_group_per_chat.get(chat_id)
    
    if not group:
        await message.answer("⚠️ Сначала выбери группу командой /start")
        return

    wk = await get_current_wk()
    current_wk_per_chat[chat_id] = wk

    url = build_url_for_wk(wk, chat_id)
    html = await get_cached_page(_shared_session, url)
    
    week_text = parse_schedule_pretty(html)

    text = f"👤 <b>Ваша группа:</b> {group}\n\n{week_text}"

    # Отправляем + сохраняем для кнопок
    msg = await message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=make_inline_kb()
    )
    
    last_msg_per_chat[chat_id] = msg.message_id
    last_text_per_chat[chat_id] = text

# ----------------- GROUP MENU -----------------
@dp.callback_query(F.data == "change_group")
async def change_group(cb: CallbackQuery):

    await cb.answer()
    update_user_activity(cb.from_user.id, cb.from_user.username)

    try:
        msg = await bot.send_message(
            cb.message.chat.id,
            "Выберите курс",
            reply_markup=build_courses_kb()
        )
        last_msg_per_chat[cb.message.chat.id] = msg.message_id
        last_text_per_chat[cb.message.chat.id] = "Выберите курс"
    except Exception:
        await send_or_edit_text("Выберите курс", cb.message.chat.id)

@dp.callback_query(F.data.startswith("course_"))
async def select_course(cb: CallbackQuery):

    await cb.answer()
    update_user_activity(cb.from_user.id, cb.from_user.username)

    course = cb.data.split("_")[1]

    selected_course_per_chat[cb.message.chat.id] = course
    save_selections()

    grs = groups_for_course(course)

    try:
        text = f"Курс {course}\n\nВыберите группу"
        msg = await bot.send_message(
            cb.message.chat.id,
            text,
            reply_markup=build_groups_kb(grs)
        )
        last_msg_per_chat[cb.message.chat.id] = msg.message_id
        last_text_per_chat[cb.message.chat.id] = text
    except Exception:
        await send_or_edit_text(f"Курс {course}\n\nВыберите группу", cb.message.chat.id)

@dp.callback_query(F.data.startswith("group_"))
async def select_group(cb: CallbackQuery):

    await cb.answer()
    update_user_activity(cb.from_user.id, cb.from_user.username)

    group = cb.data.split("_", 1)[1]

    selected_group_per_chat[cb.message.chat.id] = group
    save_selections()

    chat_id = cb.message.chat.id

    await send_or_edit_text(
        f"Группа выбрана: {group}\n\n⏳ Загружаю расписание...",
        chat_id
    )

    wk = await get_current_wk()
    current_wk_per_chat[chat_id] = wk

    await handle_show_week(cb.message, wk, cb.message)

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ КОМАНД ====================

async def get_today_schedule_text(chat_id: int) -> str:
    """Возвращает красивое расписание ТОЛЬКО на сегодня"""
    group = selected_group_per_chat.get(chat_id)
    if not group:
        return "⚠️ Сначала выбери группу командой /start"

    url = build_url_for_wk(None, chat_id)                    # текущая неделя
    html = await get_cached_page(_shared_session, url)       # ← ИСПРАВЛЕНО!
    
    if not html:
        return "❌ Не удалось загрузить расписание"

    week_text = parse_schedule_pretty(html)
    today_text = extract_today(week_text)

    return f"👤 <b>Ваша группа:</b> {group}\n\n{today_text}"


async def get_week_schedule_text(chat_id: int) -> str:
    """Возвращает полное расписание на неделю"""
    group = selected_group_per_chat.get(chat_id)
    if not group:
        return "⚠️ Сначала выбери группу командой /start"

    url = build_url_for_wk(None, chat_id)
    html = await get_cached_page(_shared_session, url)       # ← ИСПРАВЛЕНО!
    
    if not html:
        return "❌ Не удалось загрузить расписание"

    week_text = parse_schedule_pretty(html)
    return f"👤 <b>Ваша группа:</b> {group}\n\n{week_text}"

# ----------------- WEEK BUTTONS -----------------
@dp.callback_query(F.data.startswith("wk_"))
async def week_buttons(cb: CallbackQuery):

    await cb.answer()

    # === ОБНОВЛЕНИЕ АКТИВНОСТИ И USERNAME (исправлено) ===
    update_user_activity(cb.from_user.id, cb.from_user.username)

    # === ЛОГИКА КНОПОК === (всё остальное без изменений)
    if cb.data == "wk_this":
        wk = await get_current_wk()

    elif cb.data == "wk_refresh":
        wk = current_wk_per_chat.get(cb.message.chat.id, await get_current_wk())

        last_msg_per_chat.pop(cb.message.chat.id, None)
        last_text_per_chat.pop(cb.message.chat.id, None)

    else:
        # prev / next
        wk = current_wk_per_chat.get(cb.message.chat.id, await get_current_wk())

        if cb.data == "wk_prev":
            wk -= 1
        elif cb.data == "wk_next":
            wk += 1

    # сохраняем выбранную неделю
    current_wk_per_chat[cb.message.chat.id] = wk

    await handle_show_week(cb.message, wk, cb.message)

@dp.callback_query(F.data == "day_today")
async def show_today(cb: CallbackQuery):

    await cb.answer()

    wk = await get_current_wk()
    
    current_wk_per_chat[cb.message.chat.id] = wk

    global _shared_session
    html = await get_cached_page(_shared_session, build_url_for_wk(wk, cb.message.chat.id))

    week_text = parse_schedule_pretty(html)

    today_text = extract_today(week_text)

    group = selected_group_per_chat.get(cb.message.chat.id, "не выбрана")
    today_text = f"👤 <b>Ваша группа:</b> {group}\n\n{today_text}"

    await send_or_edit_text(today_text, cb.message.chat.id)

waiting_for_schedule_time = set()

@dp.callback_query(F.data == "setup_schedule")
async def ask_schedule_time(cb: CallbackQuery):

    await cb.answer()
    chat_id = cb.message.chat.id
    uid = str(cb.from_user.id)
    
    current_time = "не установлена"
    if uid in user_store and "schedule_time" in user_store[uid]:
        current_time = user_store[uid]["schedule_time"]

    waiting_for_schedule_time.add(chat_id)
    
    text = (
        f"⏰ <b>Настройка рассылки</b>\n\n"
        f"Сейчас рассылка: <b>{current_time}</b>\n\n"
        f"Введите время в формате <b>ЧЧ:ММ</b> (например, 08:30).\n"
        f"Бот будет присылать расписание только в учебные дни.\n\n"
        f"Чтобы отключить, напишите: <code>Отменить рассылку</code>\n\n"
        f"ВАЖНО: Бот будет отправлять расписание той группы. Которая была вами выбрана на момент отправки.\n\n"
        f"ВАЖНО: Во избежание спама бот может отправлять рассылку лишь 1 раз в день!\n"
    )
    await cb.message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_schedule_kb()
    )

@dp.callback_query(F.data == "schedule_back")
async def schedule_back(cb: CallbackQuery):

    await cb.answer()
    chat_id = cb.message.chat.id

    waiting_for_schedule_time.discard(chat_id)

    msg = await bot.send_message(
        chat_id,
        "Вы вернулись назад",
        reply_markup=make_inline_kb()
    )

    last_msg_per_chat[chat_id] = msg.message_id
    last_text_per_chat[chat_id] = "Вы вернулись назад"

@dp.callback_query(F.data == "schedule_disable")
async def schedule_disable(cb: CallbackQuery):

    await cb.answer()

    chat_id = cb.message.chat.id
    uid = str(cb.from_user.id)

    if uid in user_store:
        user_store[uid].pop("schedule_time", None)
        save_users(user_store)

    waiting_for_schedule_time.discard(chat_id)

    msg = await bot.send_message(
        chat_id,
        "✅ Рассылка отключена",
        reply_markup=make_inline_kb()
    )

    last_msg_per_chat[chat_id] = msg.message_id
    last_text_per_chat[chat_id] = "✅ Рассылка отключена"

# ----------------- TRIGGER COMMANDS -----------------
@dp.message(Command("stats"))
async def show_stats(message: Message):
    if message.from_user.id != BOT_OWNER_ID:
        return

    # 1. Время работы бота
    uptime_seconds = int(time.time() - START_TIME)
    uptime_str = str(datetime.timedelta(seconds=uptime_seconds))

    # 2. Размер кеша
    cache_size = len(_cache)

    # 3. Всего юзеров
    total_users = len(user_store)

    # 4. Популярная группа
    if selected_group_per_chat:
        most_popular_group, group_count = Counter(selected_group_per_chat.values()).most_common(1)[0]
    else:
        most_popular_group, group_count = "Нет данных", 0

    # 5. Всего запросов
    total_reqs = TOTAL_REQUESTS

    # 6. Последний активный (исключая владельца)
    last_user_name = "Нет данных"
    last_time = 0
    for uid_str, info in user_store.items():
        if int(uid_str) == BOT_OWNER_ID:
            continue
        user_time = info.get("last_activity", 0)
        if user_time > last_time:
            last_time = user_time
            last_user_name = info.get("username", "без_ника")
    last_use_text = f"@{last_user_name} ({datetime.datetime.fromtimestamp(last_time).strftime('%d.%m.%Y %H:%M:%S')})" if last_time > 0 else "Никто еще не пользовался"

    # 7. Активные рассылки
    active_schedules = sum(1 for info in user_store.values() if "schedule_time" in info)

    # === НОВОЕ ===
    today_iso = datetime.date.today().isoformat()
    today_sent = sum(1 for info in user_store.values() if info.get("last_sent_date") == today_iso)

    # Последняя рассылка сегодня
    last_sent_info = None
    for uid_str, info in user_store.items():
        if info.get("last_sent_date") == today_iso and "last_sent_time" in info:
            if last_sent_info is None or info["last_sent_time"] > last_sent_info[1]:
                last_sent_info = (info.get("username", "без ника"), info["last_sent_time"], uid_str)

    if last_sent_info:
        last_sent_text = f"@{last_sent_info[0]} в {last_sent_info[1]}"
    else:
        last_sent_text = "Сегодня ещё не было"

    text = get_stats_text()

    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message(Command("list_users"))
async def list_users(message: Message):
    if message.from_user.id != BOT_OWNER_ID:
        return
    await message.answer(get_users_list_text())

@dp.message(Command("schedule_list"))
async def schedule_list(message: Message):
    if message.from_user.id != BOT_OWNER_ID:
        return

    lines = []
    for uid_str, info in sorted(user_store.items(), key=lambda x: x[1].get("schedule_time", "99:99")):
        if "schedule_time" in info:
            username = info.get("username", "без ника")
            group = selected_group_per_chat.get(int(uid_str), "не выбрана")
            time = info["schedule_time"]
            lines.append(f"• {uid_str} — @{username} → <b>{time}</b> (группа: {group})")

    text = get_schedule_list_text()
    if "<b>" in text:  # активные рассылки
        await message.answer(text, parse_mode=ParseMode.HTML)
    else:
        await message.answer(text)

# словарь: user_id -> target_id (кому пересылать)
active_supp: dict[int, int] = {}

@dp.message(Command("broadcast"))
async def broadcast(message: Message):
    if message.from_user.id != BOT_OWNER_ID:
        return

    if not message.reply_to_message:
        await message.answer("Сделайте reply на сообщение для рассылки.")
        return

    msg = message.reply_to_message

    sent = 0
    failed = 0

    for uid in user_store:
        user_id = int(uid)

        try:
            await bot.copy_message(
                chat_id=user_id,
                from_chat_id=msg.chat.id,
                message_id=msg.message_id
            )

            sent += 1
            await asyncio.sleep(0.05)

        except Exception:
            failed += 1

    await message.answer(
        f"📢 Рассылка завершена\n\n"
        f"Отправлено: {sent}\n"
        f"Ошибок: {failed}"
    )

@dp.message(Command("supp_to"))
async def start_supp(message: Message):
    if message.from_user.id != BOT_OWNER_ID:
        return

    target_id = None

    # если команда ответом на сообщение
    if message.reply_to_message:
        text = message.reply_to_message.text or ""

        match = re.search(r"\((\d+)\)", text)
        if match:
            target_id = int(match.group(1))
        else:
            target_id = message.reply_to_message.from_user.id

    else:
        args = message.text.split(maxsplit=1)

        if len(args) < 2:
            await message.answer("Использование: /supp_to user_id")
            return

        try:
            target_id = int(args[1].strip())
        except ValueError:
            await message.answer("Неверный user_id")
            return

    target_id = str(target_id)

    if target_id not in user_store:
        await message.answer("Такого пользователя нет")
        return

    active_supp[message.from_user.id] = int(target_id)

    await message.answer(f"🎯 Переписка с → {target_id}")

@dp.message(Command("supp_stop"))
async def supp_stop(message: Message):
    if message.from_user.id in active_supp:
        del active_supp[message.from_user.id]
        await message.answer("Переписка остановлена")
    else:
        await message.answer("Переписка не активна")

@dp.message(Command("debug_week"))
async def debug_week(message: Message):
    if message.from_user.id != BOT_OWNER_ID:
        return

    await message.answer("🔍 <b>Отладка автоопределения недели</b>\n", parse_mode=ParseMode.HTML)

    monday_ts = get_current_monday_ts()
    now = time.time()

    current_wk = await get_current_wk()   # ← вызовет реальную функцию

    text = (
        f"📅 Сегодня: <b>{datetime.datetime.now().strftime('%A %d.%m.%Y %H:%M')}</b>\n\n"
        f"Понедельник этой недели (00:00): <b>{datetime.datetime.fromtimestamp(monday_ts).strftime('%d.%m.%Y %H:%M')}</b>\n"
        f"monday_ts = <code>{monday_ts}</code>\n\n"
        f"CURRENT_WK_CACHE:\n"
        f"   wk = <b>{CURRENT_WK_CACHE['wk']}</b>\n"
        f"   ts = <code>{CURRENT_WK_CACHE['ts']}</code> "
        f"({'сегодня' if CURRENT_WK_CACHE['ts'] >= monday_ts else 'СТАРЫЙ — будет обновлён'})\n\n"
        f"✅ Определённая неделя: <b>{current_wk}</b>\n"
        f"(последний парсинг сайта был {'успешным' if CURRENT_WK_CACHE['wk'] > 300 else 'неудачным'})"
    )

    await message.answer(text, parse_mode=ParseMode.HTML)

    # Принудительно обновляем кеш прямо сейчас для теста
    CURRENT_WK_CACHE["ts"] = 0  # сбрасываем ts → следующее нажатие кнопки или рассылка обновит
    await message.answer("🔄 Кеш недели сброшен! Теперь нажми кнопку «🔁 обновить» или дождись рассылки — увидишь новую неделю сразу.")

@dp.message(Command("clear_sent"))
async def clear_sent_dates(message: Message):
    if message.from_user.id != BOT_OWNER_ID:
        await message.answer("⛔ Только владелец может использовать эту команду")
        return

    count = 0
    for uid_str, info in user_store.items():
        if "last_sent_date" in info:
            del info["last_sent_date"]
            count += 1

    save_users(user_store)  # атомарное сохранение как везде в твоём коде

    await message.answer(
        f"✅ <b>last_sent_date очищен у {count} пользователей!</b>\n\n"
        f"Теперь рассылка сработает сегодня (даже если время уже прошло).\n"
        f"Можешь сразу проверить командой /stats",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"🧹 Владелец очистил last_sent_date у {count} пользователей")

@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id != BOT_OWNER_ID:
        return
    text = "Добро пожаловать в админ-панель"
    msg = await message.answer(text, reply_markup=build_admin_kb())
    admin_panel_msg_id[message.from_user.id] = msg.message_id


@dp.callback_query(F.data.startswith("admin_"))
async def admin_panel_callback(cb: CallbackQuery):
    await cb.answer()
    if cb.from_user.id != BOT_OWNER_ID:
        return
    chat_id = cb.message.chat.id
    action = cb.data[6:]
    panel_id = admin_panel_msg_id.get(cb.from_user.id)
    if not panel_id:
        await cb.message.answer("Панель не найдена. Вызови /admin")
        return

    if action == "stats":
        text = get_stats_text()
        await bot.edit_message_text(text, chat_id=chat_id, message_id=panel_id,
                                    parse_mode=ParseMode.HTML, reply_markup=build_admin_kb())

    elif action == "users":
        text = get_users_list_text()
        await bot.edit_message_text(text, chat_id=chat_id, message_id=panel_id,
                                    reply_markup=build_admin_kb())

    elif action == "schedules":
        text = get_schedule_list_text()
        await bot.edit_message_text(text, chat_id=chat_id, message_id=panel_id,
                                    parse_mode=ParseMode.HTML if "<b>" in text else None,
                                    reply_markup=build_admin_kb())

    elif action == "broadcast":
        waiting_for_broadcast.add(chat_id)
        await bot.edit_message_text(
            "Напишите сообщение для массового оповещения, либо напишите отмена.",
            chat_id=chat_id, message_id=panel_id
        )

    elif action == "supp":
        lines = [f"{uid} — @{info.get('username', 'без ника')}" for uid, info in
                 sorted(user_store.items(), key=lambda x: int(x[0]))]
        text = "📋 Список пользователей:\n\n" + "\n".join(lines) + "\n\nУкажите ID для связи (или отмена)"
        waiting_for_supp_id.add(chat_id)
        await bot.edit_message_text(text, chat_id=chat_id, message_id=panel_id)

    elif action == "clear_sent":
        count = 0
        for uid_str, info in user_store.items():
            if "last_sent_date" in info:
                del info["last_sent_date"]
                count += 1
        save_users(user_store)
        text = f"✅ <b>last_sent_date очищен у {count} пользователей!</b>\n\nТеперь рассылка сработает сегодня."
        await bot.edit_message_text(text, chat_id=chat_id, message_id=panel_id,
                                    parse_mode=ParseMode.HTML, reply_markup=build_admin_kb())
        logger.info(f"🧹 Админ очистил last_sent_date у {count} пользователей")

    elif action == "debug_week":
        monday_ts = get_current_monday_ts()
        current_wk = await get_current_wk()
        text = (
            f"🔍 <b>Отладка автоопределения недели</b>\n\n"
            f"📅 Сегодня: <b>{datetime.datetime.now().strftime('%A %d.%m.%Y %H:%M')}</b>\n\n"
            f"Понедельник этой недели (00:00): <b>{datetime.datetime.fromtimestamp(monday_ts).strftime('%d.%m.%Y %H:%M')}</b>\n"
            f"monday_ts = <code>{monday_ts}</code>\n\n"
            f"CURRENT_WK_CACHE:\n   wk = <b>{CURRENT_WK_CACHE['wk']}</b>\n   ts = <code>{CURRENT_WK_CACHE['ts']}</code>\n\n"
            f"✅ Определённая неделя: <b>{current_wk}</b>"
        )
        await bot.edit_message_text(text, chat_id=chat_id, message_id=panel_id,
                                    parse_mode=ParseMode.HTML, reply_markup=build_admin_kb())
        CURRENT_WK_CACHE["ts"] = 0
        logger.info("🔄 Кеш недели сброшен через админ-панель")


@dp.message(lambda m: m.from_user.id == BOT_OWNER_ID and m.chat.id in waiting_for_broadcast)
async def handle_broadcast_input(message: Message):
    chat_id = message.chat.id
    if message.text and message.text.strip().lower() in ["отмена", "отменить"]:
        waiting_for_broadcast.discard(chat_id)
        await bot.edit_message_text("Добро пожаловать в админ-панель", chat_id=chat_id,
                                    message_id=admin_panel_msg_id[chat_id], reply_markup=build_admin_kb())
        await message.answer("Отменено.")
        return

    sent = failed = 0
    for uid in user_store:
        try:
            await bot.copy_message(chat_id=int(uid), from_chat_id=message.chat.id, message_id=message.message_id)
            sent += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    waiting_for_broadcast.discard(chat_id)
    await bot.edit_message_text("Добро пожаловать в админ-панель", chat_id=chat_id,
                                message_id=admin_panel_msg_id[chat_id], reply_markup=build_admin_kb())
    await message.answer(f"📢 Рассылка завершена\nОтправлено: {sent}\nОшибок: {failed}")


@dp.message(lambda m: m.from_user.id == BOT_OWNER_ID and m.chat.id in waiting_for_supp_id)
async def handle_supp_id_input(message: Message):
    chat_id = message.chat.id
    text = message.text.strip().lower()
    if text in ["отмена", "отменить"]:
        waiting_for_supp_id.discard(chat_id)
        await bot.edit_message_text("Добро пожаловать в админ-панель", chat_id=chat_id,
                                    message_id=admin_panel_msg_id[chat_id], reply_markup=build_admin_kb())
        await message.answer("Отменено.")
        return
    try:
        target_id = int(text)
    except:
        await message.answer("Неверный ID, попробуйте снова или отмена.")
        return
    target_str = str(target_id)
    if target_str not in user_store:
        await message.answer("Такого пользователя нет.")
        return
    active_supp[message.from_user.id] = target_id
    waiting_for_supp_id.discard(chat_id)
    await bot.edit_message_text("Добро пожаловать в админ-панель", chat_id=chat_id,
                                message_id=admin_panel_msg_id[chat_id], reply_markup=build_admin_kb())
    await message.answer(f"🎯 Переписка с → {target_id} активирована (/supp_stop для остановки)")

@dp.message(lambda message: message.chat.id in waiting_for_schedule_time)
async def schedule_input(message: Message):
    chat_id = message.chat.id
    uid = str(message.from_user.id)
    text = message.text.strip().lower()

    if text == "отменить рассылку" or text == "отмена":
        if uid in user_store:
            user_store[uid].pop("schedule_time", None)
            save_users(user_store)
        
        waiting_for_schedule_time.discard(chat_id)
        await message.answer("✅ Рассылка отключена.")
        return

    try:
        # Проверка формата
        if ":" not in text:
            raise ValueError
        
        h_str, m_str = text.split(":")
        h, m = int(h_str), int(m_str)

        if not (0 <= h < 24 and 0 <= m < 60):
            raise ValueError

        formatted_time = f"{h:02d}:{m:02d}"

        # Сохранение в базу
        if uid not in user_store:
            user_store[uid] = {}
        
        user_store[uid]["username"] = message.from_user.username or user_store[uid].get("username", "unknown")
        user_store[uid]["schedule_time"] = formatted_time
        user_store[uid]["last_activity"] = time.time()
        
        # ВАЖНО: сохраняем в файл сразу
        save_users(user_store)

        waiting_for_schedule_time.discard(chat_id)
        await message.answer(f"✅ Успешно! Теперь каждый день в <b>{formatted_time}</b> я буду присылать вам расписание (если есть пары).", parse_mode=ParseMode.HTML)
        
    except ValueError:
        await message.answer("⚠️ Неверный формат. Пожалуйста, введите время как <b>08:30</b> или напишите 'Отмена'.", parse_mode=ParseMode.HTML)

# ----------------- UNIVERSAL FORWARD -----------------
@dp.message()
async def forward_messages(message: Message):
    update_user_activity(message.from_user.id, message.from_user.username)
# --- OWNER REPLY MODE ---
    if message.from_user.id == BOT_OWNER_ID and message.reply_to_message:
        text = message.reply_to_message.text or ""

        match = re.search(r"\((\d+)\)", text)
        if match:
            user_id = int(match.group(1))

            try:
                if message.text:
                    await bot.send_message(user_id, message.text)

                elif message.photo:
                    await bot.send_photo(
                        user_id,
                        message.photo[-1].file_id,
                        caption=message.caption
                    )

                elif message.video:
                    await bot.send_video(
                        user_id,
                        message.video.file_id,
                        caption=message.caption
                    )

                elif message.document:
                    await bot.send_document(
                        user_id,
                        message.document.file_id,
                        caption=message.caption
                    )

                elif message.voice:
                    await bot.send_voice(user_id, message.voice.file_id)

                elif message.sticker:
                    await bot.send_sticker(user_id, message.sticker.file_id)

            except Exception as e:
                await message.answer(f"Ошибка отправки: {e}")

            return

    # ----------------- SUPPORT MODE -----------------
    if message.from_user.id in active_supp:
        target_id = active_supp[message.from_user.id]

        try:
            if message.text:
                await bot.send_message(target_id, message.text)
            elif message.photo:
                photo = message.photo[-1].file_id
                await bot.send_photo(target_id, photo=photo, caption=message.caption)
            elif message.video:
                await bot.send_video(target_id, video=message.video.file_id, caption=message.caption)
            elif message.document:
                await bot.send_document(target_id, document=message.document.file_id, caption=message.caption)
            elif message.audio:
                await bot.send_audio(target_id, audio=message.audio.file_id, caption=message.caption)
            elif message.voice:
                await bot.send_voice(target_id, voice=message.voice.file_id, caption=message.caption)
            elif message.animation:
                await bot.send_animation(target_id, animation=message.animation.file_id, caption=message.caption)
            elif message.video_note:
                await bot.send_video_note(
                    target_id,
                    video_note=message.video_note.file_id,
                    duration=message.video_note.duration,
                    length=message.video_note.length
                )
            elif message.sticker:
                await bot.send_sticker(target_id, sticker=message.sticker.file_id)
            else:
                await bot.send_message(target_id, "⚠️ Неподдерживаемый тип сообщения")
        except Exception as e:
            await message.answer(f"⚠️ Ошибка при пересылке: {e}")

        return

    if message.from_user.id == BOT_OWNER_ID:
        return

    chat_id = BOT_OWNER_ID
    user_info = f"От @{message.from_user.username} ({message.from_user.id})"

    try:
        if is_user_spamming(message.from_user.id):
            return

        if message.text:
            await forward_queue.put((
                bot.send_message,
                (chat_id,),
                {"text": "💬 " + user_info + ":\n" + message.text}
            ))

        elif message.photo:
            photo = message.photo[-1].file_id
            caption = "🖼️ " + user_info + ":\n" + (message.caption or "")
            await forward_queue.put((
                bot.send_photo,
                (chat_id,),
                {"photo": photo, "caption": caption}
            ))

        elif message.document:
            doc = message.document.file_id
            caption = "📄 " + user_info + ":\n" + (message.caption or "")
            await forward_queue.put((
                bot.send_document,
                (chat_id,),
                {"document": doc, "caption": caption}
            ))

        elif message.video:
            video = message.video.file_id
            caption = "🎥 " + user_info + ":\n" + (message.caption or "")
            await forward_queue.put((
                bot.send_video,
                (chat_id,),
                {"video": video, "caption": caption}
            ))

        elif message.video_note:
            await forward_queue.put((
                bot.send_message,
                (chat_id,),
                {"text": f"🎬 {user_info}"}
            ))
            await forward_queue.put((
                bot.send_video_note,
                (chat_id,),
                {
                    "video_note": message.video_note.file_id,
                    "duration": message.video_note.duration,
                    "length": message.video_note.length
                }
            ))

        elif message.audio:
            await forward_queue.put((
                bot.send_audio,
                (chat_id,),
                {"audio": message.audio.file_id, "caption": "🎵 " + user_info}
            ))

        elif message.voice:
            await forward_queue.put((
                bot.send_voice,
                (chat_id,),
                {"voice": message.voice.file_id, "caption": "🎙️ " + user_info}
            ))

        elif message.sticker:
            await forward_queue.put((
                bot.send_message,
                (chat_id,),
                {"text": f"⭐ {user_info}: стикер"}
            ))
            await forward_queue.put((
                bot.send_sticker,
                (chat_id,),
                {"sticker": message.sticker.file_id}
            ))

        elif message.animation:
            await forward_queue.put((
                bot.send_animation,
                (chat_id,),
                {"animation": message.animation.file_id, "caption": "🎞️ " + user_info}
            ))

        else:
            await forward_queue.put((
                bot.send_message,
                (chat_id,),
                {"text": f"❓ Неподдерживаемый тип сообщения от {message.from_user.id}"}
            ))

    except Exception as e:
        await forward_queue.put((
            bot.send_message,
            (chat_id,),
            {"text": f"⚠️ Ошибка при пересылке от {message.from_user.id}: {e}"}
       ))

async def forward_worker():
    while True:
        func, args, kwargs = await forward_queue.get()
        try:
            await func(*args, **kwargs)
            await asyncio.sleep(0.05)  # антифлуд
        except Exception as e:
            if "retry after" in str(e).lower():
                await asyncio.sleep(3)
                await forward_queue.put((func, args, kwargs))
            else:
                logger.error(f"Ошибка пересылки: {e}")
        finally:
            forward_queue.task_done()

# ----------------- RUN -----------------
async def main():
    logger.info("🚀 Бот запускается...")
    global _shared_session
    timeout = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=20)
    connector = aiohttp.TCPConnector(
        limit=100,                    # общий лимит соединений
        limit_per_host=10,             # максимум 5 одновременных к lk.ks.psuti.ru
        ttl_dns_cache=300,
        force_close=False,
        enable_cleanup_closed=True    # предотвращает утечки сокетов
    )
    headers = {
        "User-Agent": "KS-Psuti-TGBot/2.0 (Python/aiohttp)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9"
    }
    global _shared_session
    _shared_session = aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=headers
    )
    logger.info("🌐 Глобальная сессия создана (limit_per_host=10 + User-Agent)")

    _load_cache_file()
    logger.info(f"📦 Кеш загружен: {len(_cache)} записей | wk={CURRENT_WK_CACHE['wk']}")

    await set_bot_commands()

    # Запуск фоновых задач
    asyncio.create_task(schedule_sender())
    asyncio.create_task(periodic_save())
    asyncio.create_task(forward_worker())

    try:
        await dp.start_polling(bot)
    finally:
        logger.info("🛑 Завершение работы. Очистка ресурсов...")
        
        # 1. Закрываем сессию
        try:
            if _shared_session:
                await _shared_session.close()
        except Exception as e:
            logger.warning("Ошибка при закрытии сессии: %s", e)

        # 2. Финальное сохранение всех данных
        try:
            save_users(user_store)
            _save_cache_file()
            logger.info("💾 Все данные успешно сохранены на диск!")
        except Exception as e:
            logger.error("Критическая ошибка при финальном сохранении: %s", e)

if __name__ == "__main__":
    asyncio.run(main())
