#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 02:05:32 2021

@author: jdifrisco
"""

from pucknight import ep_soup
from pucknight import nhl_sel
import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine, MetaData, Table
import psycopg2
from psycopg2 import Error
import os
import time
import datetime

#SQL ALCHEMY Engine
host = '127.0.0.1'
port = '5432'
dbname = 'pucknight'
user = 'postgres'
password = '654yam'
engine = create_engine('postgresql://' + user + ':' + password + '@' + host + ':'+ port + '/' + dbname)

#PSYCOPG2 Connection
connection = psycopg2.connect(user='postgres',
                            password='654yam',
                            host = '127.0.0.1',
                            port = '5432',
                            database = 'pucknight')

connect_creds = psycopg2.connect(user='postgres',
                              password='654yam',
                              host = '127.0.0.1',
                              port = '5432',
                              database = 'pucknight')
    

def df_to_csv(df):
    """
    Creates a CSV out of the df. 
    Used for loading in DB.

    Parameters
    ----------
    df : dataframe
        A dataframe that is fot DB load

    Returns
    -------
    None.

    """
    file = 'file_load_x.csv'
    path = '/users/jdifrisco/desktop/pucknight/py_to_pg/'
    if os.path.exists(path+file):
        os.remove(path+file)
        f_write_contents = open(path+file , 'w')
    else: 
        f_write_contents = open(path+file , 'w')
    df.to_csv(path+file,sep='|', index=False)        


################################################################
def teams_db(df):
    """
    Database load for a single league-season. 
    Reads the df_skaters generated in ep_soup.playersbyseason()
    Creates a CSV output, and performs a trunc-and-load for the team.teams table

    Parameters
    ----------
    df : df
        df_skaters generated using ep_soup.playersbyseason()

    Returns
    -------
    None.

    """
    teams = df[['team','league','season','team_linkid','link_team']].drop_duplicates(subset=['team','season'])
    teams['teamid'] = (teams.team_linkid +'/'+teams.league +'/'+teams.season)
    
    ########  TRUNCATES THE TEAM TABLE FOR TEAMIDs BEING LOADED   
    league_load = list(teams['teamid'].unique())
    check_in = '('
    for p in league_load:
        if "'" in p:
            p = p.replace("'","''")
        check_in += str("'"+p+"'"+",")
    check_in = check_in + ')'
    check_in = check_in[:-2]
    check_in += ')'
    league_trunc_q = '''delete from teams.teams
                    where teamid in 
                    '''+check_in
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    df_to_csv(teams)
    teams_load =  """copy teams.teams
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(league_trunc_q)
        cursor.execute(teams_load)
        connection.commit()
        print("Existing Stats deleted in PostgreSQL ")        
        print("teams loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')
          
            
          
            
def player_db(dfs, dfg):
    """
    Loads the PlayerID table with dfs generated in ep_soup.playersbyseason()
    Checks for duplicates by subtracting ID's from existing db values

    Parameters
    ----------
    dfs : df
        df_skaters from ep_soup.playersbyseason()
    dfg : df
        df_goalies from ep_soup.playersbyseason()

    Returns
    -------
    None.

    """
    pid = pd.DataFrame(columns=['playername','link','playerid'])
    players = [dfs[['playername','link','playerid']],
               dfg[['playername','link','playerid']]]
    pid = pid.append(players)
    pid.drop_duplicates(subset=(['playername','playerid']),keep='last',inplace=True)
    players_list = pid['playerid'].to_list()
    
    player_id_q = ''' select playerid from players.playerid '''
    players_in_db_list = pd.read_sql_query(player_id_q, con=engine)['playerid'].to_list()
    not_in_db = list(set(players_list) - set(players_in_db_list))
    player_load = pid[pid['playerid'].isin(not_in_db)]
    print("Players to Load: "+ str(len(player_load)))
    
    df_to_csv(player_load)
    playerid_q =  """copy players.playerid
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')      
    cursor = connection.cursor()       
    try:        
        cursor.execute(playerid_q)
        connection.commit()
        print("playerid loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')
          
            

def season_stats_s(df):    
    """
    Loads the teams.season_stats_s table using df_skaters from ep_soup.playersbyseason()
    Performs trunc-and-load all items at the teamID level

    Parameters
    ----------
    df : df
        df_skaters from ep_soup.playersbyseason()

    Returns
    -------
    None.

    """
    league_load = list(df['teamid'].unique())
    check_in = '('
    for p in league_load:
        if "'" in p:
            p = p.replace("'","''")
        check_in += str("'"+p+"'"+",")
    check_in = check_in + ')'
    check_in = check_in[:-2]
    check_in += ')'
    league_trunc_q = '''delete from teams.season_stats_s 
                    where teamid in 
                    '''+check_in
    
    df = df[['gp','g','a','tp','ppg','pim','+/-','playerid','teamid']]
    df_to_csv(df)
    season_stats_load =  """copy teams.season_stats_s
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
      
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')      
    cursor = connection.cursor()       
    try:        
        cursor.execute(league_trunc_q)
        print("Existing Stats deleted in PostgreSQL")        
        cursor.execute(season_stats_load)
        connection.commit()
        print("Season Stats successfully loaded in teams.season_stats_s ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')
          
            

def season_stats_g(df):   
    """
    Loads the teams.season_stats_g table using df_goalies from ep_soup.playersbyseason()
    Performs trunc-and-load all items at the teamID level

    Parameters
    ----------
    df : df
        df_goalies from ep_soup.playersbyseason()

    Returns
    -------
    None.

    """    
    league_load = list(df['teamid'].unique())
    check_in = '('
    for p in league_load:
        if "'" in p:
            p = p.replace("'","''")
        check_in += str("'"+p+"'"+",")
    check_in = check_in + ')'
    check_in = check_in[:-2]
    check_in += ')'
    league_trunc_q = '''delete from teams.season_stats_g
                    where teamid in 
                    '''+check_in
    
    df = df[['gp','gaa','sv%','teamid','playerid']]
    df_to_csv(df)
    season_stats_load =  """copy teams.season_stats_g
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')      
    cursor = connection.cursor()       
    try:        
        cursor.execute(league_trunc_q)
        print("Existing Stats deleted in PostgreSQL ")
        cursor.execute(season_stats_load)
        connection.commit()
        print("Season Stats successfully loaded in teams.season_stats_g ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')          
            
            
        
def playerpage_s_db(s_dict):
    """
    Loads career stats into players.career_stats_s using s_dict from ep_soup.playerpage()
    Performs a trunc-and-load at the playerID level
    
    Parameters
    ----------
    s_dict : dictionary of {pid: df_career}
        s_dict from ep_soup.playerpage()

    Returns
    -------
    None.

    """
    for pid, css_df in s_dict.items():
        try:
            css_df.replace('-',0,inplace=True)        
            stat_line_trunc_q = """delete from players.career_stats_s
                            where playerid = ('%s')"""%(pid)
        
            
            css_df = css_df[['gp','g','a','tp','pim','+/-',
                           'post_gp','post_g','post_a','post_tp','post_pim','post_+/-',
                           'teamid','playerid','captain']]
            df_to_csv(css_df)
            career_stats_load =  """copy players.career_stats_s
            FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
              DELIMITER '|' 
              CSV HEADER 
              QUOTE '\"' 
              ESCAPE '''';
              """
            connection = psycopg2.connect(user='postgres',
                                        password='654yam',
                                        host = '127.0.0.1',
                                        port = '5432',
                                        database = 'pucknight')      
            cursor = connection.cursor()       
            try:        
                cursor.execute(stat_line_trunc_q)
                cursor.execute(career_stats_load)        
                connection.commit()                
                print("Existing Stats deleted from players.career_stats_s ")                                
                print("Career-Stats successfully loaded in players.career_stats_s: "+pid)
            
            except(Exception,Error) as error:
                print('Error while connecting to PostgreSQL',error)
                
            finally:
                if (connection):
                    cursor.close()
                    connection.close()
                    print('PostgreSQL connection is closed')
        except(Exception,Error) as error:
            print("PlayerPage_S Load Failed on "+pid, error)            
            
            
            
def playerpage_g_db(g_dict):
    """
    Loads career stats into players.career_stats_g using g_dict from ep_soup.playerpage()
    Performs a trunc-and-load at the playerID level
    
    Parameters
    ----------
    g_dict : dictionary of {pid: df_career}
        g_dict from ep_soup.playerpage()

    Returns
    -------
    None.

    """    
    for pid, gss_df in g_dict.items():
        try:
            gss_df.replace('-',0,inplace=True)        
            stat_line_trunc_q = '''delete from players.career_stats_g
                            where playerid = '''+ "'"+pid+"'"            

            gss_df = gss_df[['gp','gd','gaa','sv%','ga','sv','so','wlt','toi',
                              'post_gp','post_gd','post_gaa','post_sv%',
                              'post_ga','post_sv','post_so','post_wlt','post_toi',
                              'teamid','playerid','captain']]
            df_to_csv(gss_df)
            career_stats_load =  """copy players.career_stats_g
            FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
              DELIMITER '|' 
              CSV HEADER 
              QUOTE '\"' 
              ESCAPE '''';
              """
            connection = psycopg2.connect(user='postgres',
                                        password='654yam',
                                        host = '127.0.0.1',
                                        port = '5432',
                                        database = 'pucknight')      
            cursor = connection.cursor()       
            try:        
                cursor.execute(stat_line_trunc_q)            
                print("Existing Stats deleted in players.career_stats_g ")                
                cursor.execute(career_stats_load)
                connection.commit()
                print("Career-Stats successfully loaded in players.career_stats_g: "+pid)
            
            except(Exception,Error) as error:
                print('Error while connecting to PostgreSQL',error)
                
            finally:
                if (connection):
                    cursor.close()
                    connection.close()
                    print('PostgreSQL connection is closed')
        
        except(Exception,Error) as error:
            print("PlayerPage_G Load Failed on "+pid, error)   
            
            
      
def playerinfo_db(pi_d):
    """
    Loads players.info using pi_d from ep_soup.playerinfo()
    Performs a trunc-and-load at the playerID level
    
    Parameters
    ----------
    pi_d : dictionary of {pid: df_info}
        pi_d from ep_soup.playerinfo()

    Returns
    -------
    None.

    """      
    for pid, info_df in pi_d.items():        
        info_row_delete = '''delete from players.info
                        where playerid ='''+"'"+pid+"'"
        
        df_to_csv(info_df)
        info_load =  """copy players.info
        FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
          DELIMITER '|' 
          CSV HEADER 
          QUOTE '\"' 
          ESCAPE '''';
          """
        connection = psycopg2.connect(user='postgres',
                                    password='654yam',
                                    host = '127.0.0.1',
                                    port = '5432',
                                    database = 'pucknight')      
        cursor = connection.cursor()               
        try:        
            cursor.execute(info_row_delete)
            print("Existing Player Info deleted in PostgreSQL: "+pid)            
            cursor.execute(info_load)
            connection.commit()
            print("Player Info successfully loaded in PostgreSQL: "+pid)
        except(Exception,Error) as error:
            print('Error while connecting to PostgreSQL',error)
            
        finally:
            if (connection):
                cursor.close()
                connection.close()
                print('PostgreSQL connection is closed')              
                                

def league_standings_load(df):
    """
    Loads teams.season_standings using df from ep_soup.leaguestandings()
    Performs a trunc-and-load at the teamid level
    
    Parameters
    ----------
    df : df
        dataframe from ep_soup.leaguestandings()

    Returns
    -------
    None.

    """     
    team_load = list(df['teamid'].unique())
    team_check_in = '('
    for t in team_load:
        if "'" in t:
            t = t.replace("'","''")
        team_check_in += str("'"+t+"'"+",")
    team_check_in = team_check_in + ')'
    team_check_in = team_check_in[:-2]
    team_check_in += ')'
    
    standings_trunc_q = '''delete from teams.season_standings
                    where teamid in 
                    '''+team_check_in
             
    df_to_csv(df)
    season_standings_load =  """copy teams.season_standings
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')      
    cursor = connection.cursor()       
    try:        
        cursor.execute(standings_trunc_q)
        print("Existing Standings deleted in teams.season_standings ")        
        cursor.execute(season_standings_load)
        connection.commit()
        print("Season Standings successfully loaded in teams.season_standings ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')                


def league_awards_load(df):
    league_load = list(df['link'].unique())
    link_check_in = '('
    for t in league_load:
        if "'" in t:
            t = t.replace("'","''")
        link_check_in += str("'"+t+"'"+",")
    link_check_in = link_check_in + ')'
    link_check_in = link_check_in[:-2]
    link_check_in += ')'
    
    award_trunc_q = '''delete from teams.league_awards
                    where link in 
                    '''+link_check_in
             
    df_to_csv(df)
    league_awards_load =  """copy teams.league_awards
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')      
    cursor = connection.cursor()       
    try:        
        cursor.execute(award_trunc_q)
        print("Existing Standings deleted in teams.league_awards ")        
        cursor.execute(league_awards_load)
        connection.commit()
        print("Season Standings successfully loaded in teams.league_awards ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')                
    


def award_winners_load(df):   
    df = df[['Season','playerid','League','award']]    
    df_to_csv(df)
    league_awards_load =  """copy teams.award_winners
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')      
    cursor = connection.cursor()       
    try:        
        cursor.execute(league_awards_load)
        connection.commit()
        print("Award Winners successfully loaded in teams.award_winners ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')          


def league_level_refresh(league, season):
    
    dfs, dfg = ep_soup.playersbyseason(league, season)
    time.sleep(2)
    stand = ep_soup.league_standings(league, season)    
    time.sleep(2)
    player_db(dfs, dfg)    
    time.sleep(2)
    season_stats_s(dfs)
    time.sleep(2)
    season_stats_g(dfg)
    time.sleep(2)
    teams_db(dfs)
    time.sleep(2)
    league_standings_load(stand)        

def league_full_refresh(league, season):

    dfs, dfg = ep_soup.playersbyseason(league, season)
    time.sleep(2)
    stand = ep_soup.league_standings(league, season)    
    time.sleep(2)
    player_db(dfs, dfg)    
    time.sleep(2)
    season_stats_s(dfs)
    time.sleep(2)
    season_stats_g(dfg)
    time.sleep(2)
    teams_db(dfs)
    time.sleep(2)
    league_standings_load(stand)    
    time.sleep(2)
    
    pid = pd.DataFrame(columns=['playername','link','playerid'])
    players = [dfs[['playername','link','playerid']],
                dfg[['playername','link','playerid']]]
    pid = pid.append(players)
    pid.drop_duplicates(subset=(['playername','playerid']),keep='last',inplace=True)
    
    skaters_list = dfs['playerid'].to_list()
    goalies_list = dfg['playerid'].to_list()
    players_list = pid['playerid'].to_list()
    
    ld = {**ep_soup.link_dict(dfs), 
          **ep_soup.link_dict(dfg)}
    
    pp_s, pp_g = ep_soup.playerpage(ld)
    time.sleep(2)
    pi = ep_soup.playerinfo(ld)
    time.sleep(2)    
    playerpage_s_db(pp_s)
    playerpage_g_db(pp_g)
    playerinfo_db(pi)
    

def league_quick_refresh(league, season):

    dfs, dfg = ep_soup.playersbyseason(league, season)
    time.sleep(2)
    stand = ep_soup.league_standings(league, season)
    time.sleep(2)
    
    player_db(dfs, dfg)    
    time.sleep(2)
    season_stats_s(dfs)
    time.sleep(2)
    season_stats_g(dfg)
    time.sleep(2)
    teams_db(dfs)    
    time.sleep(2)
    league_standings_load(stand)    
    
    pid = pd.DataFrame(columns=['playername','link','playerid'])
    players = [dfs[['playername','link','playerid']],
                dfg[['playername','link','playerid']]]
    pid = pid.append(players)
    pid.drop_duplicates(subset=(['playername','playerid']),keep='last',inplace=True)
    
    skaters_list = dfs['playerid'].to_list()
    goalies_list = dfg['playerid'].to_list()
    players_list = pid['playerid'].to_list()
    
    skaters_missing_css = ''' select playerid from players.career_stats_s '''
    players_in_db_list = pd.read_sql_query(skaters_missing_css, con=engine)['playerid'].to_list()
    css_to_add = list(set(skaters_list) - set(players_in_db_list))
    
    goalies_missing_csg = ''' select playerid from players.career_stats_g '''
    players_in_db_list = pd.read_sql_query(goalies_missing_csg, con=engine)['playerid'].to_list()
    csg_to_add = list(set(goalies_list) - set(players_in_db_list))    
    ### end of duplicate-check
    
    ld = {**ep_soup.link_dict(dfs), 
          **ep_soup.link_dict(dfg)}
    supp_dict = {}
    for p in css_to_add:
        supp_dict[p] = ld[p]
    for p in csg_to_add:
        supp_dict[p] = ld[p]        
    
    ## FOR FULL REFRESH: REPLACE SUPP_DICT WITH LD    
    pp_s, pp_g = ep_soup.playerpage(supp_dict)
    time.sleep(2)
    pi = ep_soup.playerinfo(supp_dict)
    time.sleep(2)
    
    playerpage_s_db(pp_s)
    playerpage_g_db(pp_g)
    playerinfo_db(pi)
    
        
######################
### NHL game-stat data

def nhl_db(df, tbl):
    df_to_csv(df)
    connection = psycopg2.connect(user='postgres',
                              password='654yam',
                              host = '127.0.0.1',
                              port = '5432',
                              database = 'pucknight')
    cursor = connection.cursor()  
    
    load_query =  """copy nhl_game_stats.%s
            FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
             DELIMITER '|' 
             CSV HEADER 
             QUOTE '\"' 
             ESCAPE '''';
             """%(tbl)
    try:    
        cursor = connection.cursor()
        cursor.execute(load_query)
        connection.commit()
        print("NHL Games table updated successfully in NHLGameStats ")
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')    
            
            
def contract_load(cash_df):
    
    szn = list(set(cash_df['season']))[0]
    contract_trunc_q = '''delete from players.contracts
                    where season = '%s'
                    '''%(szn)
    #### Trunc for Player-Szn dupes 
        
    df_to_csv(cash_df)
    cash_load =  """copy players.contracts
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')      
    cursor = connection.cursor()       
    try:              
        cursor.execute(contract_trunc_q)
        cursor.execute(cash_load)
        connection.commit()
        print("Contracts successfully trunc-and-loaded in players.contracts ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')                          