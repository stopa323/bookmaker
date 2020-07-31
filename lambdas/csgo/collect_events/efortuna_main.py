import aiohttp
import asyncio

from bs4 import BeautifulSoup
from os import environ


ENVIRONMENT = environ.get("ENVIRONMENT", "dev")
if "prod" == ENVIRONMENT:
    import boto3

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('efortuna-csgo-bets')

URL = "https://www.efortuna.pl/zaklady-bukmacherskie/esport-cs-go"


async def fetch_page() -> str:
    print(f"Fetching CS:GO event list page ({URL})")
    async with aiohttp.ClientSession() as session:
        response = await session.request("GET", url=URL)
        page_html = await response.text()
        return page_html


def parse_events(html: str):
    html = html.replace('<div class="pull-left"', '')
    root = BeautifulSoup(html, features="html.parser")

    # competition_list = [
    #     parse_competition(competition) for competition in root.find_all(
    #         "section", {"class": "competition-box"})]

    events_list = [parse_event(event) for event in root.find_all(
        "a", {"class": "event-name"})]

    timestamps_list = [parse_date(date) for date in root.find_all(
        "td", {"class": "col-date"})]

    results = [{
        "bookmakerName": "efortuna",
        "gameName": "CS:GO",
        "eventURL": event[0],
        "eventID": event[1],
        "eventName": event[2],
        "eventTimestamp": timestamp}
        for event, timestamp in zip(events_list, timestamps_list)]
    return results


def parse_competition(competition):
    try:
        c_data_id = competition.attrs["data-id"]
        c_data_competition = competition.attrs["data-competition"]
        c_data_sport_id = competition.attrs["data-sport-id"]
        c_data_sport = competition.attrs["data-sport"]

        return (c_data_id, c_data_competition, c_data_sport_id, c_data_sport)
    except KeyError as err:
        print(f"Unable to parse competition: {err}. Details: {competition}")
        return None


def parse_event(event):
    try:
        e_href = event.attrs["href"]
        e_data_id = event.attrs["data-id"]
        # Todo: This field has to be normalized
        e_name = event.parent.attrs["data-value"]

        return (e_href, e_data_id, e_name)
    except KeyError as err:
        print(f"Unable to parse event: {err}. Details: {event}")
        return None


def parse_date(date):
    try:
        utc_timestamp = date.attrs["data-value"]
        return utc_timestamp
    except KeyError as err:
        return None


def handler(event, ctx):
    loop = asyncio.get_event_loop()
    html_page = loop.run_until_complete(fetch_page())
    results = parse_events(html_page)

    if "prod" == ENVIRONMENT:
        for r in results:
            table.put_item(Item=r)

    return {"statusCode": 200}
