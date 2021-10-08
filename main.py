"""
Reads an athlete_events.csv file, validates the content removing invalid characters
and ingests the data into SQLite3 database for further analysis.
"""

from typing import Dict, Iterator, List
from itertools import tee
import csv
import re


# Unofficial game, that should not be added th stats
UNOFFICIAL_GAME = '1906 Summer'

# Constants for mapping records to integer values
MEDALS = {
    'NA': 0,
    'Gold': 1,
    'Silver': 2,
    'Bronze': 3,
}

SEASONS = {'Winter': 1, 'Summer': 0}


def parse_athletes(csv_reader: Iterator, teams_data: dict) -> Dict[str, dict]:
    """Parses athletes data, validates and changes data in case it's corrupted."""
    all_athletes = {}
    for row in csv_reader:
        raw_name = row.get('Name')  # With parentheses, content inside and quote marks
        name_no_brackets = re.sub(r'\([^()]*\)', '', raw_name.strip())
        valid_name = re.sub(r' "[^()]*"', '', name_no_brackets).strip()

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

        athlete_id = row.get('ID')
        athlete_noc = row.get('NOC')
        team_id = teams_data.get(athlete_noc)[1]

        athlete = {
            'name': valid_name,
            'sex': sex,
            'year_of_birth': year_of_birth,
            'parameters': parameters,
            'athlete_id': athlete_id,
            'team_id': team_id,
        }
        all_athletes.update({valid_name: athlete})

    return all_athletes


def parse_teams(csv_reader: Iterator) -> Dict[str, list]:
    """Reads and transforms team data from .csv file noc names and teams."""
    teams_data = {}
    for index, row in enumerate(csv_reader):
        team_name = row.get('Team')
        noc_name = row.get('NOC')  # 3-letter code for a team
        if '-' in team_name:
            dash_index = team_name.find('-')
            team_name = team_name.replace(team_name[dash_index:], '')
        teams_data.update({noc_name: [team_name, index]})

    return teams_data


def parse_games(csv_reader: Iterator) -> Dict[str, list]:
    """Parses game-related data from .csv file, returns results only for official games."""
    game_records = {}
    for index, row in enumerate(csv_reader):
        year = row.get('Year')
        season = row.get('Season')
        season_id = SEASONS.get(season)
        city = row.get('City')
        game = f'{year} {season}'  # used as a unique identifier
        if game == UNOFFICIAL_GAME:
            continue
        if game not in game_records.keys():
            game_records.update({game: [city, season_id, year, index]})
        if city not in game_records.get(game)[0]:
            game_records.update(
                {game: [f'{game_records.get(game)[0]}, {city}', season_id, year, index]}
            )

    return game_records


def parse_sports(csv_reader: Iterator) -> Dict[str, int]:
    """Parses sports data from .csv file, returns only unique values."""
    unique_sports = {}
    for index, row in enumerate(csv_reader):
        sport_name = row.get('Sport')
        unique_sports.update({sport_name: index})

    return unique_sports


def parse_events(csv_reader: Iterator) -> Dict[str, int]:
    """Parses event data from .csv file, returns only unique values."""
    unique_events = {}
    for index, row in enumerate(csv_reader):
        event_name = row.get('Event')
        unique_events.update({event_name: index})

    return unique_events


def parse_results(
    csv_reader: Iterator, games_data: dict, sports_data: dict, events_data: dict
) -> List[tuple]:
    """Parses data related to results, using previously parsed records."""
    all_results = []
    for row in csv_reader:
        athlete_id = row.get('ID')
        sport_name = row.get('Sport')
        game_name = row.get('Games')
        event_name = row.get('Event')

        if game_name not in games_data.keys():
            continue
        game_id = games_data.get(game_name)[-1]
        sport_id = sports_data.get(sport_name)
        event_id = events_data.get(event_name)

        medal = MEDALS.get(row.get('Medal'))

        result = (athlete_id, game_id, sport_id, event_id, medal)
        all_results.append(result)

    return all_results


def read_data():
    """Reads a .csv file in order for other functions to parse & prepare the data."""
    with open('src/athlete_events.csv', encoding='utf-8') as source_file:
        csv_reader = csv.DictReader(source_file)
        teams, games, sports, events, athletes, results = tee(csv_reader, 6)
        parsed_teams = parse_teams(teams)
        parsed_sports = parse_sports(sports)
        parsed_events = parse_events(events)
        parsed_games = parse_games(games)
        parsed_athletes = parse_athletes(athletes, parsed_teams)
        parsed_results = parse_results(
            results, parsed_games, parsed_sports, parsed_events
        )


if __name__ == '__main__':
    read_data()
