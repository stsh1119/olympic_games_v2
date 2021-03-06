# olympic_games_v2
Tech stack:  
[![](https://img.shields.io/badge/Python-14354C?style=for-the-badge&logo=python&logoColor=white)]()
[![](https://img.shields.io/badge/SQLite-07405E?style=for-the-badge&logo=sqlite&logoColor=white)]()

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Purpose of the task

⚠️ The provided code is an improved version of [this one](https://github.com/stsh1119/olympic_games).  
Practice writing your own code, so it's better not to use any external libraries.

## Description

There is a database of athletes took part in Olympic Games. The DB has been provided in CSV format: athlete_events.csv  
We have our own relational database, that we would like to import data into. Our database schema must not be changed.  
We use SQLite on this project.  

## Tasks
### 1. Import

* Import all data from CSV file to our database.
* This should be just py script to be run from the command line.
* This code is going to be run once, so the structure and style is not so important.
* This code shouldn’t be covered by tests.
* You don't need to write a parser for CSV in general! Just parse provided file somehow for
this particular case.
* Don’t use any external libs for parse CSV - write own code.

To populate the database with data from .csv file run the following command:
```shell
python main.py
```

### 2. Bar charts

Create a command line tool to show few diagrams. The interface of the tool is the following:
```shell
python stats.py chart_name other_params
```

##### 2.1 Amount of medals

Show bar chart with amount of medals for the certain team specified by NOC name (case-insensitive), 
season and certain medal name (gold, silver, bronze). NOC name and season are required. 

If medal name is not present, all medals should be counted.
Sort result by year in chronological order.
If there is no medals for this year - show 0 (blank bar), but all years must be present.
Chart name is medals.
**Params**: season `[winter|summer]` NOC medal_name `[gold|silver|bronze]` (in any order).
Command examples:

```shell
python stats.py medals summer ukr
python stats.py medals silver UKR winter
```

##### 2.2 Top teams
Show amount of medals per team for the certain year, season and medal type ordered by
amount. Most awarded teams must be on the top. Season is required.
Chart name is top-teams.
**Params**: season `[winter|summer]` year medal_type `[gold|silver|bronze]` (in any order).
Command examples:
```shell
 python stats.py top-teams summer 2004 silver
```

```shell
python stats.py top-teams winter
```
