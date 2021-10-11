"""
Reads an athlete_events.csv file, validates the content removing invalid characters
and ingests the data into SQLite3 database for further analysis.
"""
from itertools import tee
import csv

import db
import parsers


FILE_PATH = 'src/athlete_events.csv'
DATABASE = 'olympic_history.db'


def load() -> None:
    """Reads a .csv, invokes other functions to parse and load the data into a DB."""
    with open(FILE_PATH, encoding='utf-8') as source_file:
        csv_reader = csv.DictReader(source_file)
        teams, games, sports, events, athletes, results = tee(csv_reader, 6)
        parsed_teams = parsers.parse_teams(teams)
        parsed_sports = parsers.parse_sports(sports)
        parsed_events = parsers.parse_events(events)
        parsed_games = parsers.parse_games(games)
        parsed_athletes = parsers.parse_athletes(athletes, parsed_teams)
        parsed_results = parsers.parse_results(
            results, parsed_games, parsed_sports, parsed_events
        )
        with db.connect(DATABASE) as connection:
            db.ingest_games(parsed_games, connection)
            db.ingest_teams(parsed_teams, connection)
            db.ingest_athletes(parsed_athletes, connection)
            db.ingest_sports(parsed_sports, connection)
            db.ingest_events(parsed_events, connection)
            db.ingest_results(parsed_results, connection)

    print('Data was successfully loaded.')


if __name__ == '__main__':
    load()
