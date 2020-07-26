import aiohttp
import asyncio
import boto3

from bs4 import BeautifulSoup
from os import environ
from typing import List

URL = "https://www.efortuna.pl/zaklady-bukmacherskie/esport-cs-go"

# Create SQS cliend
sqs = boto3.client('sqs')
SQS_QUEUE_URL = environ["SQS_QUEUE_URL"]


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

    # Send message to SQS queue for each obtained URL
    for link in event_links:
        # TODO: Find out if this may fail somehow
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=link
        )

    return {"statusCode": 200}
