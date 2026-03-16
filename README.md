# ТГ РАСПИСАНИЕ


## env.example
RELEASE_TOKEN=Токен созданного тг бота через @botfather  
OWNERID=ID вашего тг аккаунта для доступа к админ командам  


## Обновления

#### BUILD 1.3
Переделал механику автоопределение недели.  
Добавил /debug_week для отладки функции

#### DOCKER UPDATE
Упаковал бота в докер.

#### BUILD 1.4
Запись в users.json каждые 10 минут, вместо после каждого изменения.  
Корректное завершение работы с сохранением файлов.  

### BUILD 1.5 HTML UPDATE
Поменял docker-compose.yml чтобы /list_users выводил корректное время ласт онлайна.  
Фулл переделанный парсер html, теперь выводятся все колонки, постарался сделать всё аккуратно  

#### BUILD 1.5.1 send_or_edit
Переделана функция send_or_edit_text, при превышении лимита тг бот будет делить сообщения на два.  
Причем для большей читабельности текста деление происходит по дням недели


## В планах
Расширить записи в логи для удобства. Мб сделать кнопки переключения дней.



# Установка:
### Докер
```bash
git clone https://github.com/siskahui/TGBOT.git
cd TGBOT
cp .env.example > .env && nano .env
docker compose up -d --build
```

### Python
```bash
git clone https://github.com/siskahui/TGBOT.git
cd TGBOT
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example > .env && nano .env
python tg.py
```
