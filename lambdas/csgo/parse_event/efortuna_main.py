import aiohttp
import asyncio
import re

from bs4 import BeautifulSoup
from itertools import chain
from os import environ
from typing import List, Union
from urllib.parse import urljoin

ENVIRONMENT = environ.get("ENVIRONMENT", "dev")

BASE_URL = "https://www.efortuna.pl"

if "prod" == ENVIRONMENT:
    import boto3

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('csgo-bets')


async def get_html(session: aiohttp.ClientSession, url: str) -> str:
    print(f"Fetching CS:GO event page ({url})")
    response = await session.request("GET", url=urljoin(BASE_URL, url))
    return await response.text()


async def fetch_event_pages(urls: List[str]) -> List[str]:
    async with aiohttp.ClientSession() as session:
        get_html_tasks = [get_html(session, _url) for _url in urls]
        html_pages = await asyncio.gather(*get_html_tasks)
        return html_pages


################################################################################
# Pre-processing functions
################################################################################
def replace_characters(value: str) -> str:
    value = re.sub('ó', 'o', value.lower())
    value = re.sub('ł', 'l', value.lower())
    return value


def team_name(value: str) -> str:
    value = replace_characters(value)

    # Filter out undesired characters
    value = re.sub(r'[^A-Z0-9 ]+', '', value.upper().strip())

    # Replace whitespace(s) with underscore character
    value = re.sub(' +', '_', value).strip()

    return value


def get_bet_name(value: str) -> Union[None, str]:
    # Filter out undesired characters
    value = re.sub(r'[^a-z0-9]+', '', value.lower())

    NAME_MAP = {
        "zwycizca1mapy": "1st_map_winner",
        "zwycizca2mapy": "2nd_map_winner",
        "ilomap": "map_count",
        "dokladnywynik": "exact_score",
        "1druzynawygraprzynajmniejjednmap": "1st_team_wins_at_least_once",
        "2druzynawygraprzynajmniejjednmap": "2nd_team_wins_at_least_once",
    }

    try:
        return NAME_MAP[value]
    except KeyError:
        print(f"Unknown bet-type ({value})")
        return


def map_count(value: str):
    value = re.sub(r'[^a-z0-9.]+', '', value.lower())

    if "mniej2.5" == value:
        return "-2.5"
    elif "wicej2.5" == value:
        return "+2.5"


def exact_score(value: str):
    value = re.sub(r'[^0-9:]+', '', value.lower())

    return value


def yes_or_no(value: str) -> bool:
    value = re.sub(r'[^a-z]+', '', value.lower())

    if "tak" == value:
        return True
    elif "nie" == value:
        return False


def get_option_name(bet_name: str, value: str):
    func = {
        "1st_map_winner": team_name,
        "2nd_map_winner": team_name,
        "map_count": map_count,
        "exact_score": exact_score,
        "1st_team_wins_at_least_once": yes_or_no,
        "2nd_team_wins_at_least_once": yes_or_no,
    }[bet_name]
    return func(value)


def option_rate(value: str) -> float:
    # Filter out undesired characters
    value = re.sub(r'[^0-9.,\- ]+', '', value.lower())

    # Replace comma with dot
    value = re.sub(',+', '.', value).strip()

    return float(value)


def build_bet_json(competition_id: str,
                   match_id: str,
                   bet_id: str,
                   bet_type: str,
                   option_value: str,
                   option_rate: float) -> dict:
    bet_json = {
        "competitionID": competition_id,
        "matchID": match_id,
        "betID": bet_id,
        "betType": bet_type,
        "optionValue": option_value,
        "optionRate": str(option_rate),
    }
    return bet_json


async def parse_html(page_html: str) -> List[dict]:
    root = BeautifulSoup(page_html, features="html.parser")
    bets = []

    try:
        section = root.find("section")
        competition_id = section.attrs["data-competition-id"]
        match_id = section.attrs["data-match-id"]

        td = root.find_all("td")

        team1, team2 = td[0].attrs["data-value"].split(" - ")
        team1 = team_name(team1)
        team2 = team_name(team2)

        odd1 = td[1].find("a")
        odd2 = td[2].find("a")

        team1_rate = option_rate(odd1.attrs["data-value"])
        team2_rate = option_rate(odd2.attrs["data-value"])

        bet1_id = odd1.attrs["data-id"]
        bet2_id = odd2.attrs["data-id"]

        bets.append(build_bet_json(competition_id, match_id, bet1_id, "winner",
                                   team1, team1_rate))
        bets.append(build_bet_json(competition_id, match_id, bet2_id, "winner",
                                   team2, team2_rate))

        for market in root.find_all("div", {"class": "market"}):
            bet_name = get_bet_name(market.find("h3").find("a").text)

            if not bet_name:
                print(f"Skipping {bet_name} ({team1} vs {team2})")
                continue

            for option in market.find(
                    "div", {"class": "odds-group"}).find_all("a"):
                opt_name = get_option_name(
                    bet_name, option.find("span", {"class": "odds-name"}).next)
                opt_value = option_rate(
                    option.attrs["data-value"])
                bet_id = option.attrs["data-id"]
                bets.append(build_bet_json(competition_id, match_id, bet_id,
                                           bet_name, opt_name, opt_value))
    except Exception as exc:
        print(exc)
        return bets

    return bets


async def parse_event_pages(html_pages: List[str]):
    parse_html_tasks = [parse_html(html) for html in html_pages]
    bets = await asyncio.gather(*parse_html_tasks)

    # Each task returns list of bets. I have to flatten it.
    bets = list(chain.from_iterable(bets))
    return bets


def extract_urls_from_event(event):
    return [msg["body"] for msg in event["Records"]]


def handler(event, ctx):
    urls = extract_urls_from_event(event)
    loop = asyncio.get_event_loop()
    html_pages = loop.run_until_complete(fetch_event_pages(urls))
    bets = loop.run_until_complete(parse_event_pages(html_pages))

    if "prod" == ENVIRONMENT:
        for b in bets:
            response = table.put_item(Item=b)

    return {"statusCode": 200, "bets": bets}
