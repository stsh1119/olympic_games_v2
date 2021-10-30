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


def get_medals_stats() -> List[tuple]:
    """Given valid parameters, queries for data that will be used to build a chart.

    Returns:
        List of tuples, each containing year, amount of medals for that year, example [(1988, 1), (1992, 5),...]
    """
    user_query_params = sys.argv[2:]  # Provided by user
    try:  # Checking whether given mandatory parameters are valid
        season: int = [SEASONS.get(value.lower()) for value in user_query_params if value.lower() in SEASONS][0]
        noc_name: str = [value.upper() for value in user_query_params if value.upper() in _get_all_teams()][0]
        # Validating optional parameters
        medal = next((MEDALS.get(value) for value in user_query_params if value in MEDALS), None)
    except IndexError as index_missing:
        raise BadInput('Both NOC name and season parameters are required, medal is optional.') from index_missing

    valid_params = [param for param in (season, noc_name, medal) if param is not None]

    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        query_results = cursor.execute(
            "select g.year, count(r.medal) as amount_of_medals "
            "from results r, athletes a, teams t, games g "
            "where r.athlete_id = a.id "
            "and t.id = a.team_id "
            "and g.id = r.game_id "
            "and g.season = ? "  # required
            "and t.noc_name = ? "  # required
            f"{'and medal = ? ' if medal is not None else ''}"  # checking if medal is specified
            "group by g.year "
            "order by g.year",
            valid_params,
        ).fetchall()

        return query_results


def get_top_teams_stats() -> List[tuple]:
    """Given valid parameters, queries for data that will be used to build top-teams chart.

    Returns:
        List of tuples, each containing team, amount of medals for it, example [('AUS', 77), ('USA, 76),...]
    """
    user_query_params = sys.argv[2:]
    if len(user_query_params) < 1:
        raise BadInput('Missing mandatory parameter: season')
    try:  # Validating mandatory parameter
        season: int = [SEASONS.get(value.lower()) for value in user_query_params if value.lower() in SEASONS][0]
    except IndexError as missing_index:
        raise BadInput('Incorrect mandatory parameter: season') from missing_index
    # Checking for optional parameters
    medal = next((MEDALS.get(value) for value in user_query_params if value in MEDALS), None)
    year = next((int(value) for value in user_query_params if value.isdigit()), None)

    valid_params = [param for param in (season, medal, year) if param is not None]

    with sqlite3.connect(DATABASE) as conn:
        query_results = conn.execute(
            "with amount_per_team as (select t.noc_name, count(medal) medals_amount "
            "from teams t, results r, athletes a, games g "
            "where t.id = a.team_id "
            "and a.id = r.athlete_id "
            "and g.id = r.game_id "
            "and g.season = ? "  # required
            f"{'and r.medal = ? ' if medal is not None else ''}"
            f"{'and g.year = ? ' if year is not None else ''}"
            "group by t.noc_name "
            "order by medals_amount desc) "
            "select * "
            "from amount_per_team "
            "where medals_amount > (select avg(medals_amount) from amount_per_team)",
            valid_params,
        ).fetchall()
        return query_results


def build_chart(query_result: List[tuple]) -> None:
    """Determines a coefficient(k), so that chart is no longer than 200 blocks and
    builds a bar chart for a given data.

    Args:
        query_result: List of tuples returned from the DB after query execution.
    """
    max_numeric_value = max(query_result, key=lambda x: x[1])[1]
    k = MAX_BAR_LENGTH / max_numeric_value

    for row in query_result:
        dimension, metric = row[0], int(row[1])
        print(dimension, int(round(metric * k)) * BLOCK)


if __name__ == '__main__':
    try:
        if len(sys.argv[1:]) == 0:
            raise BadInput('No arguments given, try giving some, available charts: medals/top-teams')
        if sys.argv[1] == 'medals':
            data_from_db = get_medals_stats()
        elif sys.argv[1] == 'top-teams':
            data_from_db = get_top_teams_stats()
        else:
            raise BadInput('Chart unavailable: try medals/top-teams.')
        build_chart(data_from_db)

    except BadInput as err:
        print(str(err))
