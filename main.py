"""
Reads an athlete_events.csv file, validates the content removing invalid characters
and ingests the data into SQLite3 database for further analysis.
"""

from typing import Dict, Iterator, Set
from itertools import tee
import csv
import re


def parse_athletes(csv_reader: Iterator) -> Dict[str, dict]:
    """Parses athletes data, validates and changes data in case it's corrupted."""
    all_athletes = {}
    for row in csv_reader:
        raw_name = row.get('Name')
        name_no_brackets = re.sub(
            r'\([^()]*\)', '', raw_name.strip()
        )  # Remove part inside brackets
        validated_name = re.sub(
            r' "[^()]*"', '', name_no_brackets
        ).strip()  # Removing double quote marks
        sex = row.get('Sex')

        if sex not in ['M', 'F']:
            sex = None

        if row.get('Year') != 'NA' and row.get('Age') != 'NA':
            year_of_birth = int(row.get('Year')) - int(row.get('Age'))
        else:
            year_of_birth = None

        if row.get('Height') == 'NA' and row.get('Weight') != 'NA':
            parameters = {'weight': row.get('Weight')}
        elif row.get('Height') != 'NA' and row.get('Weight') == 'NA':
            parameters = {'height': row.get('Height')}
        else:  # Both parameters are present
            parameters = {
                'height': row.get('Height'),
                'weight': row.get('Weight'),
            }

        team_id = row.get('ID')

        athlete = {
            'name': validated_name,
            'sex': sex,
            'year_of_birth': year_of_birth,
            'parameters': parameters,
            'team_id': team_id,
        }
        all_athletes.update({validated_name: athlete})

    return all_athletes


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
    """Reads a .csv file in order for other functions to parse & prepare the data."""
    with open('src/athlete_events.csv', encoding='utf-8') as source_file:
        csv_reader = csv.DictReader(source_file)
        teams, games, sports, events, athletes = tee(csv_reader, 5)
        parse_teams(teams)
        parse_sports(sports)
        parse_events(events)
        parse_games(games)
        parse_athletes(athletes)


if __name__ == '__main__':
    read_data()
