import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from io import StringIO


def get_html(url :str) -> str:
    '''
    Retrieves html text from url
    '''
    html = requests.get(url).text
    return html
    
def get_scorecard_links(url :str) -> tuple:
    '''
    Return the league year and list of urls of match results for the year
    '''
    html = BeautifulSoup(get_html(url), "html.parser")
    links = [link.get('href') for link in html.find_all('a')]
    
    # links = ['https://www.espncricinfo.com' + l for l in links if l.startswith('/series/indian-premier-league') and l.endswith('full-scorecard')]
    links = ['https://www.espncricinfo.com' + l for l in links if l.startswith('/series/') and l.endswith('full-scorecard')]
    league_year = url.split('/')[-2].split('-')[-2]
    return (league_year, links)

def get_match_results(html :str) -> list:
    '''
    HTML text input is converted to list of tables,
    each table summarises key game results
    '''
    data = pd.read_html(StringIO(html)) 
    data = [table for table in data if 'BATTING' in table.columns]
    return data

def get_team_order(html :str) -> list:
    '''
    HTML text is parsed returning a list of teams batting in order of play
    '''
    html = BeautifulSoup(html, "html.parser")
    teams = [element.text for element in html.find_all('span') if type(element.get('class'))==list and " ".join(element.get('class'))=="ds-text-title-xs ds-font-bold ds-capitalize"]
    return teams

def tidy_columns(table : pd.DataFrame) -> pd.DataFrame:
    '''
    Label columns and drop redundant colums
    '''
    col_names = ['Batter', 'IsOut', 'Runs', 'Balls', 'M', '4s', '6s', 'SR', 'Extra1','Extra2']
    table.columns = col_names
    table.set_index('Batter', inplace = True)
    return table.drop(columns = ['M','SR', 'Extra1','Extra2'], axis=1)

def tidy_rows(table : pd.DataFrame) -> pd.DataFrame:
    '''
    Drop empty rows, and rows with notes
    '''
    table.dropna(inplace = True)
    table.drop([ind for ind in table.index if (ind.startswith('Did not bat') or ind.startswith('Fall of wickets'))], inplace = True)
    return table

def get_all_match_results(results_links :list) -> None:
    '''
    Given a list of urls with match results,
    loop through urls and extract match results from html table,
    tidy rows and columns, dropping redundant data,
    write each table to csv with match metadata in filename.
    '''
    
    (league_year, links) = results_links
    
    dir = 'data'
    if not os.path.exists(os.path.join(dir, league_year)):
        os.makedirs(os.path.join(dir, league_year))
         
    match_count = 1     
       
    for url in links:
        game_count = 1
        match_title = url.split("/")[-2]
        html = get_html(url)
        team_order = get_team_order(html)
        for table in get_match_results(html):
            
            table = tidy_columns(table)
            table = tidy_rows(table)
            table.to_csv(f"{dir}/{league_year}/{match_count:0=2}_{game_count}_{match_title}_{team_order[game_count-1]}.csv")
            game_count += 1
        match_count += 1
        


def main():

    # 2022 results  
    # url1 = 'https://www.espncricinfo.com/series/indian-premier-league-2022-1298423/match-schedule-fixtures-and-results'

    # 2023
    url1 = 'https://www.espncricinfo.com/series/indian-premier-league-2023-1345038/match-schedule-fixtures-and-results'

    links = get_scorecard_links(url1)
    get_all_match_results(links)
    return None

if __name__ == '__main__':
    main()
