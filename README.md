# Web scraping ETL with airflow - cricket league data

An introductory data engineering project to extract load and transform data.

## Aim 
Scrape batting data from all matches of the [2023 Indian Premier League]("https://www.espncricinfo.com/series/indian-premier-league-2023-1345038/match-schedule-fixtures-and-results"), load data into a data repository and perform aggregating queries to summarise batter performance. The extract (scrape), load and transform steps are orchestrated with airflow.


## Architecture
This repo runs airflow locally, with an instance of Postgres DB. 

## Extract
The aim is to go through each of the match results pages listed [here]("https://www.espncricinfo.com/series/indian-premier-league-2023-1345038/match-schedule-fixtures-and-results") and extract the following data for each batter:
- the number of runs scored
- the number of balls faced
- the number of 4's hit 
- the number of 6's hit 
- if
 the batter was out

## Load 
- Load to postgres DB after creating a connection.

## Transform
Aggregating queries transform stored data for summary statistics.
WIP - sql/summarise.sql query will return a table that can be created in db or output to csv.

## How to run
Withe Docker Desktop installed, run:
```bash
docker-compose up -d
```


The webserver initializes and accessed at http://localhost:8095.

## Query DB interactively
You can now trigger the etl_dag, once complete you can query the final table (public.scores) after executing:

```
docker exec -it cricket-data-airflow-postgres-1 psql -U airflow
```

e.g. `select * from public.scores;`
<br>

`select count(*) from public.scores;` This return 1,232 - the total number of scores (rows, or times batted) in the 2023 league.

The DB is backed up to postgres folder.

## Stop airflow
To stop running the airflow:

```bash
docker-compose down -v
```

