import os
import csv


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except:
        print("Error creating table")
       
def remove_symbol(name :str) -> str:
    '''
    Remove symbol annotations from batter name field '\xa0(c)†', '\xa0(c)', '\xa0†'
    '''
    return name.split('\xa0')[0]

def get_is_out_status(status_note :str) -> bool:
    return False if 'not out' in status_note  else True
        
def load_batter_score(con, dir :str) -> None:
    cur = con.cursor()
    for fname in os.listdir(dir):
        fpath = os.path.join(dir, fname)
        with open(fpath,'r') as file:
            match_metadata = fname.split('_')
            team_name = match_metadata[-1][:-4]
            match_id = int(match_metadata[0])
            game_id = int(match_metadata[1])
            match_title = match_metadata[-2]
            league_year = dir.split('/')[-1]
            dr = csv.DictReader(file)
            
            to_db = [(remove_symbol(i['Batter']), i['IsOut'], get_is_out_status(i['IsOut']), i['Runs'], i['Balls'], i['4s'], i['6s'], team_name, match_id, game_id, match_title, league_year) for i in dr]

            cur.executemany("   INSERT INTO scores (BatterName, IsOutNote, IsOut, RunCount, BallCount, Fours, Sixes, TeamName, MatchId, GameId, MatchTitle, LeagueYear) \
                                VALUES (%s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s, %s);", to_db)
    con.commit()


def load(conn):
    
    if conn is not None:
        with open('./sql/make_scores_table.sql', 'r') as sql_script:
            sql_create_scores_table = sql_script.read()
        create_table(conn, sql_create_scores_table)
        for d in os.listdir('./data'):
            load_batter_score(conn,os.path.join('./data',d))
        conn.close()
    else:
        print("No database connection.")
