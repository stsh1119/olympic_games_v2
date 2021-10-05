from typing import Dict, Iterator, Set
from itertools import tee
import csv


def parse_teams(csv_reader: Iterator) -> Dict[str, str]:
    """Reads and transforms team data from .csv file noc names and teams."""
    teams_data = {}
    for row in csv_reader:
        team = row.get('Team')
        noc_name = row.get('NOC')
        if '-' in team:
            dash_index = team.find('-')
            team = team.replace(team[dash_index:], '')
        teams_data.update({noc_name: team})

    return teams_data


def parse_games(csv_reader: Iterator) -> Dict[str, str]:
    """Parses game-related data from .csv file, returns results only for official games."""
    game_records = {}
    for row in csv_reader:
        year = row.get('Year')
        season = row.get('Season')
        city = row.get('City')
        key = f'{year}_{season}'
        if key == '1906_Summer':
            continue
        if key not in game_records.keys():
            game_records.update({key: city})
        if city not in game_records.get(key):
            game_records.update({key: f'{game_records.get(key)}, {city}'})

    return game_records


def parse_sports(csv_reader: Iterator) -> Set[str]:
    """Parses sports data from .csv file, returns only unique values."""
    unique_sports = set()
    for row in csv_reader:
        sport_name = row.get('Sport')
        unique_sports.add(sport_name)

    return unique_sports


def parse_events(csv_reader: Iterator) -> Set[str]:
    """Parses event data from .csv file, returns only unique values."""
    unique_events = set()
    for row in csv_reader:
        event_name = row.get('Event')
        unique_events.add(event_name)

    return unique_events


def read_data():
    """Reads a .csv file in order for """
    with open('src/athlete_events.csv') as source_file:
        csv_reader = csv.DictReader(source_file)
        teams, games, sports, events, athletes = tee(csv_reader, 5)  # copying the file iterator into separate variables
        parse_teams(teams)
        parse_sports(sports)
        parse_events(events)
        parse_games(games)


if __name__ == '__main__':
    read_data()
