import sys
import sqlite3
from typing import List


BLOCK = '█'  # Character for building charts
MAX_BAR_LENGTH = 200  # Maximum length of chart in rectangles(blocks)

DATABASE = 'olympic_history.db'
CHART_TYPES = ('medals', 'top-teams')  # All available chart types

# Constants for mapping records to integer values
MEDALS = {
    'NA': 0,
    'Gold': 1,
    'Silver': 2,
    'Bronze': 3,
}
SEASONS = {'Winter': 1, 'Summer': 0}


def sanitize_input():
    """Parses & validates user input to make sure chart type is correct and needed params are present."""
    user_input = sys.argv[1:]
    chart_type, *params = user_input  # NOC name, season -> required, medal - Optional
    print(chart_type, params)
    if chart_type not in CHART_TYPES:
        raise Exception("Incorrect chart type: try either 'medals' or 'top-teams'.")


def get_medals_stats() -> List[tuple]:
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        db_results = cursor.execute(
            "select g.year, count(r.medal) as amount_of_medals "
            "from results r, athletes a, teams t, games g "
            "where r.athlete_id = a.id "
            "and t.id = a.team_id "
            "and g.id = r.game_id "
            "and g.season = 0 "  # required
            "and t.noc_name = 'USA' "  # required
            # "and medal = 2 "  # optional
            "group by g.year "
            "order by g.year;"
        ).fetchall()
        return db_results


def build_chart(chart_data: List[tuple]) -> None:
    """Determines a coefficient(k), so that chart is no longer than 200 blocks and
    builds a bar chart for a given data."""
    max_amount_of_medals = max(chart_data, key=lambda x: x[1])[1]
    k = MAX_BAR_LENGTH / max_amount_of_medals

    for year_data in chart_data:
        year, amount_of_medals = year_data[0], int(year_data[1])
        print(year, int(round(amount_of_medals * k)) * BLOCK)


if __name__ == '__main__':
    # params = sanitize_input()
    medal_stats = get_medals_stats()
    build_chart(medal_stats)
