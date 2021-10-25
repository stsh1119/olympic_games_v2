"""
Module for parsing, validating user's input and then building charts in case given parameters are valid.
"""
import sqlite3
import sys
from typing import List

from exceptions import BadInput

BLOCK = '█'  # Character for building charts
MAX_BAR_LENGTH = 200  # Maximum length of chart in rectangles(blocks)

DATABASE = 'olympic_history.db'
CHART_TYPES = ('medals', 'top-teams')  # All available chart types

# Constants for mapping records to integer values
MEDALS = {
    'na': 0,
    'gold': 1,
    'silver': 2,
    'bronze': 3,
}
SEASONS = {'winter': 1, 'summer': 0}


def _get_all_teams() -> List[str]:
    """Gets NOC names of all teams present in the database.

    Returns:
        A list of team names, for example ['AFG', 'AHO', 'ALB', 'ALG',...].
    """
    with sqlite3.connect(DATABASE) as conn:
        return [team[0] for team in conn.execute("select noc_name from teams").fetchall()]


def _get_game_years() -> List[int]:
    """Gets all years, when olympic games were conducted.

    Returns:
        A list of game years, for example [1992, 2012, 1920, 1900...].
    """
    with sqlite3.connect(DATABASE) as conn:
        return [year[0] for year in conn.execute("select year from games").fetchall()]


def sanitize_input() -> tuple:
    """Parses & validates user input to make sure chart type is correct and needed params are present."""
    user_input = sys.argv[1:]
    if len(user_input) == 0:
        raise BadInput('No arguments given, try giving some, available charts: medals/top-teams')
    chart_type, *params = user_input
    if chart_type == 'medals':
        try:  # Checking whether given mandatory parameters are valid
            season: int = [SEASONS.get(value.lower()) for value in params if value.lower() in SEASONS][0]
            noc_name: str = [value.upper() for value in params if value.upper() in _get_all_teams()][0]
            # Validating optional parameters
            medal = [MEDALS.get(value) for value in params if value in MEDALS]
            if medal:
                return season, noc_name, medal[0]
            return season, noc_name
        except IndexError as index_missing:
            raise BadInput('Both NOC name and season parameters are required, medal is optional.') from index_missing

    elif chart_type == 'top-teams':
        pass
    else:
        raise BadInput('Chart unavailable: try medals/top-teams.')


def get_medals_stats(valid_input: tuple) -> List[tuple]:
    """Given valid parameters, queries for data that will be used to build a chart.

    Returns:
        List of tuples, each containing year, amount of medals for that year, example [(1988, 1), (1992, 5),...]
    """
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        medal = False if len(valid_input) == 2 else True  # checking if medal is specified
        db_results = cursor.execute(
            "select g.year, count(r.medal) as amount_of_medals "
            "from results r, athletes a, teams t, games g "
            "where r.athlete_id = a.id "
            "and t.id = a.team_id "
            "and g.id = r.game_id "
            "and g.season = ? "  # required
            "and t.noc_name = ? "  # required
            f"{'and medal = ? ' if medal else ''}"  # optional
            "group by g.year "
            "order by g.year",
            valid_input,
        ).fetchall()

        return db_results


def build_medals_chart(db_medal_stats: List[tuple]) -> None:
    """Determines a coefficient(k), so that chart is no longer than 200 blocks and
    builds a bar chart for a given data."""
    max_amount_of_medals = max(db_medal_stats, key=lambda x: x[1])[1]
    k = MAX_BAR_LENGTH / max_amount_of_medals

    for year_data in db_medal_stats:
        year, amount_of_medals = year_data[0], int(year_data[1])
        print(year, int(round(amount_of_medals * k)) * BLOCK)


if __name__ == '__main__':
    try:
        parameters = sanitize_input()
        medal_stats = get_medals_stats(parameters)
        build_medals_chart(medal_stats)
    except BadInput as err:
        print(str(err))
