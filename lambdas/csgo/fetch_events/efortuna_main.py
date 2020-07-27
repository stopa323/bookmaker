import aiohttp
import asyncio
import boto3

from bs4 import BeautifulSoup
from os import environ
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


def send_urls_to_sqs(sqs_client, sqs_queue_url, urls):
    try:
        # Send message to SQS queue for each obtained URL
        for _url in urls:
            # TODO: Find out if this may fail somehow
            sqs_client.send_message(
                QueueUrl=sqs_queue_url,
                MessageBody=_url
            )
    except Exception as exc:
        return {"statusCode": 500, "message": exc}


def handler(event, ctx):
    # Create SQS session and check queue URL env. I want to fails asap to not
    # get needlessly billed for empty lambda execution.
    sqs_client = boto3.client('sqs')
    sqs_queue_url = environ.get("SQS_QUEUE_URL")
    if not sqs_queue_url:
        return {"statusCode": 400, "message": "Missing queue URL"}

    loop = asyncio.get_event_loop()
    event_urls = loop.run_until_complete(fetch_event_url_list())

    send_urls_to_sqs(sqs_client, sqs_queue_url, event_urls)

    return {"statusCode": 200}
