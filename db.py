"""
Module, containing functions for connecting and inserting parsed data into olympic-history.db
"""
from typing import Dict, List
import sqlite3

# Names of tables in the database
TEAMS = 'teams'
GAMES = 'games'
ATHLETES = 'athletes'
SPORTS = 'sports'
EVENTS = 'events'
RESULTS = 'results'


def connect(database: str) -> sqlite3.Connection:
    """Connects to a given SQLIte3 database."""
    return sqlite3.connect(database)


def ingest_teams(teams_data: Dict[str, list], conn: sqlite3.Connection) -> None:
    """Inserts parsed data for teams into a database."""
    for noc_name, values in teams_data.items():
        name, team_id = values  # Unpacking name and id
        conn.execute(f"insert into {TEAMS} values (?, ?, ?)", (team_id, name, noc_name))


def ingest_games(games_data: Dict[str, list], conn: sqlite3.Connection) -> None:
    """Ingests parsed data for games into a database."""
    for game_info in games_data.values():
        city, season, year, game_id = game_info
        conn.execute(
            f"insert into {GAMES} values (?, ?, ?, ?)",
            (game_id, year, season, city),
        )


def ingest_athletes(athletes_data: Dict[str, dict], conn: sqlite3.Connection) -> None:
    """Ingests parsed data for athletes into a database."""
    for athlete in athletes_data.values():
        name, sex, year_of_birth, parameters, athlete_id, team_id = athlete.values()
        conn.execute(
            f"insert into {ATHLETES} values (?, ?, ?, ?, ?, ?)",
            (athlete_id, name, year_of_birth, sex, str(parameters), team_id),
        )


def ingest_sports(sports_data: Dict[str, int], conn: sqlite3.Connection) -> None:
    """Ingests parsed data for sports into a database."""
    for sport, sport_id in sports_data.items():
        conn.execute(f"insert into {SPORTS} values (?, ?)", (sport_id, sport))


def ingest_events(events_data: Dict[str, int], conn: sqlite3.Connection) -> None:
    """Ingests parsed data for events into a database."""
    for event, event_id in events_data.items():
        conn.execute(f"insert into {EVENTS} values (?, ?)", (event_id, event))


def ingest_results(results_data: List[tuple], conn: sqlite3.Connection) -> None:
    """Ingests parsed data for results into a database."""
    for result in results_data:
        conn.execute(
            f"insert into {RESULTS}(athlete_id, game_id, sport_id, event_id, medal) "
            f"values (?, ?, ?, ?, ?)",
            result,
        )
