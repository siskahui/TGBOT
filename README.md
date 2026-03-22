# ТГ БОТ КС ПГУТИ РАСПИСАНИЕ

## 📄 Changelog

- [Changelog](./CHANGELOG.md)

## env.example
RELEASE_TOKEN=Токен созданного тг бота через @botfather  
OWNERID=ID вашего тг аккаунта для доступа к админ командам  

# Установка:
### Докер
```bash
git clone https://github.com/Sp0nge-bob/TGBOT
cd TGBOT
cp .env.example > .env && nano .env
docker compose up -d --build
```

### Python
```bash
git clone https://github.com/Sp0nge-bob/TGBOT
cd TGBOT
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example > .env && nano .env
python tg.py
```
