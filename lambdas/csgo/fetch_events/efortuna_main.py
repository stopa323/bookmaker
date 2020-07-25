import aiohttp
import asyncio

from bs4 import BeautifulSoup
from typing import List

URL = "https://www.efortuna.pl/zaklady-bukmacherskie/esport-cs-go"


async def fetch_event_url_list() -> List[str]:
    print(f"Fetching CS:GO event list from {URL}")
    async with aiohttp.ClientSession() as session:
        response = await session.request("GET", url=URL)
        page_html = await response.text()

        event_links = extract_event_links(page_html)
        print(f"Found {len(event_links)} CS:GO events")

        return event_links


def extract_event_links(page_html: str) -> List[str]:
    root = BeautifulSoup(page_html, features="html.parser")

    event_tags = root.find_all("a", {"class": "event-name"})
    links = [e_tag.attrs.get("href") for e_tag in event_tags]

    return list(filter(None, links))


def handler(event, ctx):
    loop = asyncio.get_event_loop()
    event_links = loop.run_until_complete(fetch_event_url_list())

    return {"statusCode": 200}
