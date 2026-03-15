import aiohttp
from bs4 import BeautifulSoup
import asyncio
import json
#ПАРСИТ ГРУППЫ И ОСТАВЛЯЕТ ПРОПУСКИ ЧТОБ ЗАПОЛНИТЬ КУРС (ХЗ МБ ПЕРЕДЕЛАЮ)
BASE_URL = "https://lk.ks.psuti.ru/?mn=2"
OUTPUT_FILE = "groups.json"

async def fetch_page(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url, timeout=10) as resp:
        resp.raise_for_status()
        return await resp.text()

async def parse_groups() -> dict[str, dict]:
    async with aiohttp.ClientSession() as session:
        html = await fetch_page(session, BASE_URL)
        soup = BeautifulSoup(html, "html.parser")
        groups = {}

        # ищем все ссылки вида ?mn=2&obj=XXX
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "mn=2" in href and "obj=" in href:
                try:
                    obj = int(href.split("obj=")[1].split("&")[0])
                    name = a.get_text(strip=True)
                    groups[name] = {"obj": obj, "course": ""}  # оставляем поле course пустым
                except Exception:
                    continue
        return groups

async def main():
    groups = await parse_groups()
    print(f"Найдено групп: {len(groups)}")
    for name, data in groups.items():
        print(f"{name} -> {data['obj']}")

    # сохраняем в файл
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(groups, f, ensure_ascii=False, indent=2)
    print(f"Группы сохранены в {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
