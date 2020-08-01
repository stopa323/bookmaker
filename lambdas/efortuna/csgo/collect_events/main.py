import aiohttp
import asyncio
import datetime
import re


from boto3.dynamodb.conditions import Attr
from bs4 import BeautifulSoup
from hashlib import sha256
from os import environ
from uuid import uuid4

DYNAMO_TABLE_NAME = environ.get("DYNAMO_TABLE_NAME")

if DYNAMO_TABLE_NAME:
    import boto3

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_TABLE_NAME)

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

    results = [build_match_object(event[2], timestamp, event[0])
               for event, timestamp in zip(events_list, timestamps_list)]
    return results


def build_match_object(name: str, date: str, url: str) -> dict:
    match_obj = {
        "id": str(uuid4()),
        "dataSource": "efortuna",
        "gameName": "CS:GO",
        "eventName": name,
        "eventURL": url,
        "eventTimestamp": date,
    }
    return match_obj


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
        e_name = event.parent.attrs["data-value"].lower()

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


def upsert_db_item(event):
    response = table.scan(
        FilterExpression=Attr("dataSource").eq("game-tournaments") &
                         Attr("eventSHA").eq(event["eventSHA"]))
    if response["Items"]:
        table.delete_item(Key={"id": response["Items"][0]["id"]})

    # Todo: Consider using update
    table.put_item(Item=event)


def handler(event, ctx):
    loop = asyncio.get_event_loop()
    html_page = loop.run_until_complete(fetch_page())
    results = parse_events(html_page)

    if DYNAMO_TABLE_NAME:
        for r in results:
            upsert_db_item(r)

    return {"statusCode": 200}

handler(None,None)