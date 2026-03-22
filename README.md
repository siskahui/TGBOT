# 📅 ТГ БОТ КС ПГУТИ — Расписание

Телеграм-бот для просмотра расписания КС ПГУТИ.  
Поддерживает группы, рассылки, статистику и админ-функции.

---

## 🚀 Возможности

- 📆 Просмотр расписания (сегодня / неделя)
- 🔔 Автоматические рассылки
- 👥 Поддержка пользователей (support mode)
- 📊 Статистика использования
- 🛠️ Админ-панель
- 🐳 Поддержка Docker

---

## 📄 Документация

- [📜 Changelog](./CHANGELOG.md)

---

## ⚙️ Переменные окружения

Создай `.env` файл на основе `.env.example`:

~~
nano .env
RELEASE_TOKEN=токен бота (через @botfather)
OWNERID=твой Telegram ID (для админ-команд)
~~

---

## 🛠️ Установка

### 🐳 Docker (рекомендуется)

~~
bash
git clone https://github.com/Sp0nge-bob/TGBOT
cd TGBOT

cp .env.example .env
nano .env

docker compose up -d --build
~~

---

### 🐍 Python

~~
bash
git clone https://github.com/Sp0nge-bob/TGBOT
cd TGBOT

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt

cp .env.example .env
nano .env

python tg.py
~~

---

## 📊 Основные команды

### 👤 Пользователь
- `/start` — запуск бота
- выбор группы
- просмотр расписания

### 🛠️ Админ
- `/admin` — админ панель
- `/stats` — статистика
- `/list_users` — список пользователей
- `/schedule_list` — активные рассылки
- `/broadcast` — рассылка
- `/broadtask` — редактирование рассылок

---

## 📦 Стек

- Python
- Docker
- Парсинг HTML

---

## 📌 Планы

- кнопки переключения дней
- улучшение UI
- дальнейшие оптимизации
