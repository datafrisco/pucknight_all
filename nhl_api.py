#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 13:03:05 2022

@author: jdifrisco
"""
import requests
import json
import pandas as pd
from pucknight import pg_db
import sqlalchemy as db
from sqlalchemy import create_engine, MetaData, Table
import psycopg2
from psycopg2 import Error


#SQL ALCHEMY Engine
host = '127.0.0.1'
port = '5432'
dbname = 'pucknight'
user = 'postgres'
password = '654yam'
engine = create_engine('postgresql://' + user + ':' + password + '@' + host + ':'+ port + '/' + dbname)

# #PSYCOPG2 Connection
connection = psycopg2.connect(user='postgres',
                            password='654yam',
                            host = '127.0.0.1',
                            port = '5432',
                            database = 'pucknight')


##################
## API Endpoints #
##################
def api_endpoints():
    api_end = 'https://statsapi.web.nhl.com/api/v1/configurations'
    response = requests.get(api_end, params={'format':'json'})
    page = response.json()

    endpoint_df = pd.DataFrame()
    for i in range(len(page)):
        ep = page[i]['endpoint']
        desc = page[i]['description']
        ep_row = pd.DataFrame([[ep, desc]],
                              columns = ['endpoint','description'])
        endpoint_df = pd.concat([endpoint_df,ep_row])
    
    return(endpoint_df)


def api_endpoint_db(endpoint_df):
    
    del_q = '''delete from nhl_api.api_endpoints'''    
    
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(endpoint_df)
    ep_load =  """copy nhl_api.api_endpoints
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(del_q)
        cursor.execute(ep_load)
        connection.commit()        
        print("API Endpoints loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')
    


##################
## dim Franchise #
##################

def dimFranchise():
    api_end = 'https://statsapi.web.nhl.com/api/v1/franchises'
    response = requests.get(api_end, params={'format':'json'})
    page = response.json()
    
    fran_df = pd.DataFrame()
    
    for i in range(len(page['franchises'])):
        fran = page['franchises'][i]
        
        franchise_id = fran['franchiseId']
        first_season = fran['firstSeasonId']    
        try:
            last_season = fran['lastSeasonId']
        except:
            last_season = 'active'
        mostRecentTeam_id = fran['mostRecentTeamId']
        team_name = fran['teamName']
        location = fran['locationName']
        tm_link = fran['link']
        
        tm_row = pd.DataFrame([[franchise_id, first_season, last_season, mostRecentTeam_id,
                  team_name, location, tm_link]],columns = ['franchise_id','first_season', 'last_season',
                                      'mostRecentTeam_id','team_name','location',
                                      'tm_link'])                          
        fran_df = pd.concat([fran_df,tm_row])
        
    fran_df.reset_index(inplace=True)       
    fran_df.drop(columns='index', inplace=True)
    
    return(fran_df)


def franchise_db(df):    
    
    franchise_del_q = '''delete from nhl_api.dim_franchise'''    
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(df)
    fran_load =  """copy nhl_api.dim_franchise
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(franchise_del_q)
        cursor.execute(fran_load)
        connection.commit()        
        print("Franchises loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')


##################
##### TEAMS #####
##################

def sznTeamsRosters(season):
    api_szn1 = int(season[:4])
    api_szn2 = api_szn1+1
    api_szn = str(api_szn1) + str(api_szn2)
    api_end = 'https://statsapi.web.nhl.com/api/v1/teams?expand=team.roster&season='+api_szn
    response = requests.get(api_end, params={'format':'json'}) 
    page = response.json() #two item dict
    
    rosters = pd.DataFrame()
    tms = page['teams']
    for i in range(len(tms)):
        teamid = tms[i]['id']
        tm_ros = tms[i]['roster']['roster']
        for p in range(len(tm_ros)):
            playerid = tm_ros[p]['person']['id']
            playername = tm_ros[p]['person']['fullName']
            try:
                jersey_num = tm_ros[p]['jerseyNumber']
            except:
                jersey_num = 'na'
            position = tm_ros[p]['position']['code']
        
            roster_row = pd.DataFrame([[season, teamid, playerid, playername, jersey_num ,position]],
                                      columns = ['season','teamid','playerid','playername','jersey_num','position'])
            rosters = pd.concat([rosters,roster_row])
    
    rosters.reset_index(inplace=True)
    rosters.drop(columns=['index'], inplace=True)        
        
    ## dimTeams
    szn_teams = pd.DataFrame()
    tms = page['teams']
    for n in range(len(page['teams'])):
        tm_id = tms[n]['id']
        franchise_id = tms[n]['franchiseId']
        tm_name = tms[n]['teamName']
        try:
            venue_id = tms[n]['venue']['id']
        except:
            venue_id = ''
        try:        
            venue = tms[n]['venue']['name']
        except:
            venue = ''
        try:        
            venue_city = tms[n]['venue']['city']
        except:
            venue_city = 'na'        
        location = tms[n]['locationName']
        division_id = tms[n]['division']['id']
        division = tms[n]['division']['name']
        conference_id = tms[n]['conference']['id']
        conference = tms[n]['conference']['name']
    
        tm_row = pd.DataFrame([[season ,tm_id, franchise_id, tm_name, venue_id, venue, venue_city,
                                location, division_id, division, conference_id, conference]],
                              columns = ['season','tm_id','franchise_id','tm_name', 'venue_id',
                                         'venue','venue_city','location','division_id','division','conference_id','conference'])
        
        szn_teams = pd.concat([tm_row, szn_teams])
    szn_teams.reset_index(inplace=True)
    szn_teams.drop(columns=['index'], inplace=True) 
    
    return(szn_teams, rosters)


def teams_db(tms_df):
    
    szn = tms_df.season.iloc[0]
    team_del_q = '''delete from nhl_api.dim_teams
                    where season = '%s'
                    '''%(szn)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(tms_df)
    team_load =  """copy nhl_api.dim_teams
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(team_del_q)
        cursor.execute(team_load)
        connection.commit()        
        print("Teams loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')    

#####

def rosters_db(ros_df):
    
    szn = ros_df.season.iloc[0]
    ros_del_q = '''delete from nhl_api.rosters
                    where season = '%s'
                    '''%(szn)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(ros_df)
    roster_load =  """copy nhl_api.rosters
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(ros_del_q)
        cursor.execute(roster_load)
        connection.commit()        
        print("Rosters loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')  


####### PLAYER INFO #######
def player_info(pid):
    people_api_end = 'https://statsapi.web.nhl.com/api/v1/people/'+str(pid)
    response = requests.get(people_api_end, params={'format':'json'}) 
    page = response.json() 
        
    player_id = page['people'][0]['id']
    playername = page['people'][0]['fullName']
    first_name = page['people'][0]['firstName']
    last_name = page['people'][0]['lastName']
    if 'primaryNumber' in list(page['people'][0].keys()):
        primary_nbr = page['people'][0]['primaryNumber']    
    else:
        primary_nbr = -1        
    dob = page['people'][0]['birthDate']
    birth_city = page['people'][0]['birthCity']
    if 'birthStateProvince' in list(page['people'][0].keys()):
        birth_state = page['people'][0]['birthStateProvince']
    else:
        birth_state = ''                    
    birth_country = page['people'][0]['birthCountry']
    nation = page['people'][0]['nationality']
    if 'height' in list(page['people'][0].keys()):
        height = page['people'][0]['height']
    else:
        height = ''
    if 'weight' in list(page['people'][0].keys()):
        weight = page['people'][0]['weight']
    else:
        weight = -1
    pos_code = page['people'][0]['primaryPosition']['code']
    position = page['people'][0]['primaryPosition']['name']
    pos_type = page['people'][0]['primaryPosition']['type']
    
    player_row = pd.DataFrame([[player_id, playername, first_name, last_name,
                                 primary_nbr, dob, birth_city, birth_state, birth_country,
                                 nation, height, weight, 
                                 pos_code, position, pos_type]],
                               columns = ['player_id', 'playername', 'first_name', 'last_name',
                                 'primary_nbr', 'dob', 'birth_city', 'birth_state', 'birth_country',
                                 'nation', 'height', 'weight', 
                                 'pos_code', 'position', 'pos_type'])
        
    return(player_row)


def player_info_db(plr_info_df):
    
    plr_list = list(plr_info_df.player_id)
    check_in = "("
    for pid in plr_list:
        pa = str(pid)+','
        check_in = check_in + pa
    check_in = check_in[:-1] + ')'
    
    plr_del_q = """delete from nhl_api.dim_players
    where player_id in """ + check_in

    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(plr_info_df)
    player_load =  """copy nhl_api.dim_players
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(plr_del_q)
        cursor.execute(player_load)
        connection.commit()        
        print("Player Dimensions loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')     



## TEAM SEASON STATS

# def seasonStats(season)

# # season = '2020-21'
# api_szn1 = int(season[:4])
# api_szn2 = api_szn1+1
# api_szn = str(api_szn1) + str(api_szn2)

# api_end_base = 'https://statsapi.web.nhl.com/api/v1/'
# api_cat = 'teams'
# api_mod = '?expand=team.stats&season='

# api_end = api_end_base + api_cat + api_mod + api_szn
    
# response = requests.get(api_end, params={'format':'json'}) 
# page = response.json() #two item dict
# # page.keys()

# tms = len(page['teams'])

# page['teams'][12].keys()

# team_id = page['teams'][12]['id']
# team_name = page['teams'][12]['name']
# team_abbv = page['teams'][12]['abbreviation']

# stat_type = page['teams'][12]['teamStats'][0]['type']['gameType']['description']

# page[api_cat][12]['teamStats'][0].keys()
# stats_dict = page[api_cat][12]['teamStats'][0]['splits']

# for stat in list(stats_dict[0]['stat']):
#     stat = stats_dict[0]['stat'][stat]
    
# gp = stats_dict[0]['stat']['gamesPlayed']
# wins = stats_dict[0]['stat']['wins']
# losses = stats_dict[0]['stat']['losses']
# ot = stats_dict[0]['stat']['ot']

# stats_ranking = page[api_cat][12]['teamStats'][0]['splits'][1]


#### END

##################
### PLAY_TYPES ###
##################

#####
#####

def dimPlayType():

    api_end = 'https://statsapi.web.nhl.com/api/v1/playTypes'
    response = requests.get(api_end, params={'format':'json'}) 
    page = response.json() #two item dict
        
    play_type_df = pd.DataFrame()
        
    for i in range(len(page)):
        play_name = page[i]['name']
        play_id = page[i]['id']    
        playerTypes = page[i]['playerTypes']
        code = page[i]['code']
        seCode = page[i]['secondaryEventCodes']
                          
        play_row = pd.DataFrame([[play_name, play_id, playerTypes,code,seCode]],
                             columns = ['play_name','id','player_type',
                                           'code','seCode'])
        
        play_type_df = pd.concat([play_type_df, play_row])
            
        
        
    play_type_df.reset_index(inplace=True)
    play_type_df.drop(columns=['index'], inplace=True)
        
    return(play_type_df)



##################
  ### GAMES ###
##################

## DOESN'T WORK
def game_ids(season):
    # season is the year of season-start eg. '2021'
    game_id_dict = {}
    
    # PRESEASON GAMES
    max_gms = 200
    game_ind = '0000'
    game_id_list = []
    gt='01'
    
    for gm in range(max_gms):
        game_num = game_ind + str(gm+1)
        game_num = game_num[-4:]
        game_id = season + gt + game_num
        game_api_end = 'https://statsapi.web.nhl.com/api/v1/game/'+game_id+'/boxscore/'
        response = requests.get(game_api_end, params={'format':'json'})  
        page = response.json()
        if 'teams' in page.keys():
            game_id_list.append(game_id)
        else:
            pass
    game_id_dict[season+gt] = game_id_list
    print('Preseason Games found for ' + season + ' through ' + game_id)
       
    #REGULAR SEASON GAMES
    max_gms = 2000
    game_ind = '0000'
    game_id_list = []
    gt='02'
    for gm in range(max_gms):
        game_num = game_ind + str(gm+1)
        game_num = game_num[-4:]
        game_id = season + gt + game_num
        game_api_end = 'https://statsapi.web.nhl.com/api/v1/game/'+game_id+'/boxscore/'
        response = requests.get(game_api_end, params={'format':'json'})  
        page = response.json()
        if 'teams' in page.keys():
            game_id_list.append(game_id)
        else:
            pass
    game_id_dict[season+gt] = game_id_list
    print('Regular Season Games found for ' + season + ' through ' + game_id)
    
    #PLAYOFFS GAMES
    d1 = 0
    d2 = range(6)
    d3 = range(10)
    d4 = range(8)
    gt = '03'
    
    game_id_list = []
    for m in d2:
        for n in d3:
            for o in d4:
                try: 
                    game_id = season + gt + str(d1) + str(m) + str(n) +str(o)
                    game_api_end = 'https://statsapi.web.nhl.com/api/v1/game/'+game_id+'/boxscore/'
                    response = requests.get(game_api_end, params={'format':'json'})  
                    page = response.json()
                    if 'teams' in page.keys():
                        game_id_list.append(game_id)
                    else:
                        pass                        
                except:
                    pass
    game_id_dict[season+gt] = game_id_list
    print('Playoff  Games found for ' + season + ' through ' + game_id)
    

    return(game_id_dict)
    


##################
### FEED/LIVE ####
##################

def gameLiveFeed(game_id): 
    api_end = 'https://statsapi.web.nhl.com/api/v1/game/'+game_id+'/feed/live'
    response = requests.get(api_end, params={'format':'json'}) 
    page = response.json() #two item dict
    
    
    game_date = page['gameData']['datetime']['dateTime'][:10]
    try:
        game_start = page['gameData']['datetime']['dateTime'][11:19]
    except: 
        game_start = '00:00:00-5:00' 
    try: 
        game_end = page['gameData']['datetime']['endDateTime'][11:19]
    except:
        game_end = '00:00:00-5:00'
    game_status = page['gameData']['status']['detailedState']
    game_pk = page['gamePk']
    try:
        game_venue_id = page['gameData']['venue']
    except:
        game_venue_id = ''
    
    away_team = page['gameData']['teams']['away']['id']
    home_team = page['gameData']['teams']['home']['id']
    
    game_info_row = pd.DataFrame([[game_id, game_date,game_start, game_end, game_status,
                                   game_pk, away_team, home_team]],
                                 columns = ['game_id', 'game_date','datetime_start','datetime_end',
                                            'game_status','game_pk','away_team_id','home_team_id'])

    
    ### key: liveData
    officials_df = pd.DataFrame()
    for r in range(len(page['liveData']['boxscore']['officials'])):        
        off_id = page['liveData']['boxscore']['officials'][r]['official']['id']
        off_name = page['liveData']['boxscore']['officials'][r]['official']['fullName']
        off_type = page['liveData']['boxscore']['officials'][r]['officialType']
        officials_row = pd.DataFrame([[game_pk, off_id, off_name, off_type]],
                                    columns=['game_id','official_id','official_name',
                                        'official_type'])
        officials_df = pd.concat([officials_df,officials_row])



    
    ### PLAYS
    plays = page['liveData']['plays']['allPlays']
    event_df = pd.DataFrame(columns = ['game_id','event','event_type', 'event_desc'
                                       ,'event_idx','event_id','period',
                                       'p_time_rem','away_score','home_score',
                                       'x_coord','y_coord','event_team'])
    
    plr_df = pd.DataFrame(columns = ['game_id','event_id','event_idx','playerid'])
    for i in range(len(plays)):
        keys = plays[i].keys()
        play = plays[i]
        result = play['result']           
        about = play['about']
        xy = play['coordinates']
            
        event_type_id = result['eventTypeId']
        event_idx = about['eventIdx']
        event_id = about['eventId']
        try: 
            sec_type = result['secondaryType']
        except:
            sec_type = ''        
        try: 
            event_desc = result['description']
        except:
            event_desc = ''                    
        period = about['period']
        p_time_rem = about['periodTimeRemaining']
        away_score = about['goals']['away']
        home_score = about['goals']['home']
        if len(xy)>0:
            x_coord = xy['x']
            y_coord = xy['y']
        else: 
            x_coord = ''
            y_coord = ''
            
        if 'team' in keys: 
            tm = play['team']    
            event_team = tm['id']
        else: 
            event_team = ''
            
        event_row = pd.DataFrame([[game_id, event_type_id, sec_type, event_desc
                                   , event_idx, event_id, period,p_time_rem,
                     away_score, home_score, x_coord, y_coord, event_team]],
                                 columns = ['game_id','event', 'event_type','event_desc'
                                            , 'event_idx','event_id','period',
                                       'p_time_rem','away_score','home_score',
                                       'x_coord','y_coord','event_team'])
        event_df = pd.concat([event_df,event_row])            
                
            
        if 'players' in keys:
            plr_event_df = pd.DataFrame(columns = ['game_id','event_id','event_idx','playerid','action'])
            plrs = play['players']
            for i in range(len(plrs)):
                p = plrs[i]
                plr = p['player']['id']
                action = p['playerType']
                plr_row = pd.DataFrame([[game_id,event_id, event_idx, plr,action]]
                                       ,columns = ['game_id','event_id','event_idx','playerid','action'])
                plr_event_df = pd.concat([plr_event_df,plr_row])
            plr_df = pd.concat([plr_df,plr_event_df])
    

    return (game_info_row, officials_df, event_df, plr_df)


###

def game_livefeed_db(game_info, officials_df, event_df, plr_df):
    
    #game_info
    game_id = game_info.game_id.iloc[0]
    status = game_info.game_status.iloc[0]

    game_del_q = '''delete from nhl_api.game_info
                    where game_id = %s
                    and game_status = '%s'
                    '''%(game_id, status)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(game_info)
    game_info_load =  """copy nhl_api.game_info
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(game_del_q)
        cursor.execute(game_info_load)
        connection.commit()        
        print("Game Info loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')  

    #official
    game_id = officials_df.game_id.iloc[0]
    game_del_q = '''delete from nhl_api.game_officials
                    where game_id = %s
                    '''%(game_id)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(officials_df)
    officials_load =  """copy nhl_api.game_officials
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(game_del_q)
        cursor.execute(officials_load)
        connection.commit()        
        print("Game Officials loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')  


    
    #events
    game_id = event_df.game_id.iloc[0]
    game_del_q = '''delete from nhl_api.game_events
                    where game_id = %s
                    '''%(game_id)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(event_df)
    event_load =  """copy nhl_api.game_events
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(game_del_q)
        cursor.execute(event_load)
        connection.commit()        
        print("Game Events loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')  
            
    #player_events
    game_id = plr_df.game_id.iloc[0]
    game_del_q = '''delete from nhl_api.player_events
                    where game_id = %s
                    '''%(game_id)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(plr_df)
    plr_load =  """copy nhl_api.player_events
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(game_del_q)
        cursor.execute(plr_load)
        connection.commit()        
        print("Player Events loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')              
    


### Game-Final Livefeed

def gameDecisions(game_id): 
    api_end = 'https://statsapi.web.nhl.com/api/v1/game/'+game_id+'/feed/live'
    response = requests.get(api_end, params={'format':'json'}) 
    page = response.json() #two item dict
        
    game_decisions = pd.DataFrame()            
    winning_g = page['liveData']['decisions']['winner']['id']
    losing_g = page['liveData']['decisions']['loser']['id']
    try:
        first_star = page['liveData']['decisions']['firstStar']['id']
    except:
        first_star = ''
    try:
        second_star = page['liveData']['decisions']['secondStar']['id']
    except:
        second_star = ''
    try:
        third_star = page['liveData']['decisions']['thirdStar']['id']
    except:
        third_star = ''
    
    decision_row = pd.DataFrame([[game_id, winning_g, losing_g
                                  , first_star, second_star, third_star]],
                                columns = ['game_id','winning_g','losing_g',
                                           'first_star','second_star','third_star'])
    game_decisions = pd.concat([game_decisions, decision_row])
    
    return(game_decisions)
    
# ### Game-Final Livefeed DB
def decisions_db(game_decisions):
    
    game_id = game_decisions.game_id.iloc[0]
    game_del_q = '''delete from nhl_api.game_decisions
                    where game_id = %s
                    '''%(game_id)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(game_decisions)
    decisions_load =  """copy nhl_api.game_decisions
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(game_del_q)
        cursor.execute(decisions_load)
        connection.commit()        
        print("Game Decisions loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')  

##################
### BOXSCORE  ####
##################

def gameBoxscore(game_id): 
    boxscore_api_end = 'https://statsapi.web.nhl.com/api/v1/game/'+game_id+'/boxscore/'
    response = requests.get(boxscore_api_end, params={'format':'json'}) 
    page = response.json() #two item dict
    
    tm_away_id = page['teams']['away']['team']['id']
    tm_away_players = list(page['teams']['away']['players'].keys())
    tm_away_goalies = list(page['teams']['away']['goalies'])
    tm_away_skaters = list(page['teams']['away']['skaters'])
    tm_away_scratches = list(page['teams']['away']['scratches'])
    
    tm_away_hc = page['teams']['away']['coaches']
    
    tm_away_skaters = [x for x in tm_away_skaters if x not in tm_away_scratches]
    
    
    tm_home_id = page['teams']['home']['team']['id']
    tm_home_players = list(page['teams']['home']['players'].keys())
    tm_home_goalies = list(page['teams']['home']['goalies'])
    tm_home_skaters = list(page['teams']['home']['skaters'])
    tm_home_scratches = list(page['teams']['home']['scratches'])
    
    tm_home_skaters = [x for x in tm_home_skaters if x not in tm_home_scratches]
    
    
    lineups = pd.DataFrame()
    for ap in tm_away_skaters: 
        plr_row = pd.DataFrame([[game_id,tm_away_id, ap, 'skater']],
                               columns = ['game_id','team_id','player_id','status'])
        lineups = pd.concat([lineups,plr_row])
    for ag in tm_away_goalies: 
        plr_row = pd.DataFrame([[game_id,tm_away_id, ag, 'goalie']],
                               columns = ['game_id','team_id','player_id','status'])
        lineups = pd.concat([lineups,plr_row])    
    for ascr in tm_away_scratches: 
        plr_row = pd.DataFrame([[game_id,tm_away_id, ascr, 'scratch']],
                               columns = ['game_id','team_id','player_id','status'])
        lineups = pd.concat([lineups,plr_row])    
        
    for hp in tm_home_skaters: 
        plr_row = pd.DataFrame([[game_id,tm_home_id, hp, 'skater']],
                               columns = ['game_id','team_id','player_id','status'])
        lineups = pd.concat([lineups,plr_row])
    for hg in tm_home_goalies: 
        plr_row = pd.DataFrame([[game_id,tm_home_id, hg, 'goalie']],
                               columns = ['game_id','team_id','player_id','status'])
        lineups = pd.concat([lineups,plr_row])    
    for hscr in tm_home_scratches: 
        plr_row = pd.DataFrame([[game_id,tm_home_id, hscr, 'scratch']],
                               columns = ['game_id','team_id','player_id','status'])
        lineups = pd.concat([lineups,plr_row])    
        
    
    
    game_plr_info = pd.DataFrame()
    for pinfo in list(page['teams']['away']['players'].keys()):
        player_id = page['teams']['away']['players'][pinfo]['person']['id']
        try:
            jersey_num = page['teams']['away']['players'][pinfo]['jerseyNumber']
        except:
            jersey_num = -1
        if 'captain' in list(page['teams']['away']['players'][pinfo]['person'].keys()):
            captain = page['teams']['away']['players'][pinfo]['person']['captain']
        else:
            captain = ''
        if 'alernateCaptain' in list(page['teams']['away']['players'][pinfo]['person'].keys()):
            alt_captain = page['teams']['away']['players'][pinfo]['person']['alternateCaptain']
        else: 
            alt_captain = ''
        pos_code = page['teams']['away']['players'][pinfo]['position']['code']
        pos = page['teams']['away']['players'][pinfo]['position']['name']
        pos_type = page['teams']['away']['players'][pinfo]['position']['type']
        pos_abbv = page['teams']['away']['players'][pinfo]['position']['abbreviation']
    
        plr_row = pd.DataFrame([[game_id,tm_away_id, player_id, jersey_num, captain, alt_captain,
                                 pos_code, pos, pos_type,pos_abbv]]
            ,columns=['game_id','team_id','player_id','jersey_num','captain','alt_captain',
                                 'pos_code','pos','pos_type','pos_abbv'])    
        game_plr_info = pd.concat([game_plr_info,plr_row])

        
    for pinfo in list(page['teams']['home']['players'].keys()):
        player_id = page['teams']['home']['players'][pinfo]['person']['id']
        try: 
            jersey_num = page['teams']['home']['players'][pinfo]['jerseyNumber']
        except:
            jersey_num = -1
        if 'captain' in list(page['teams']['home']['players'][pinfo]['person'].keys()):
            captain = page['teams']['home']['players'][pinfo]['person']['captain']
        else:
            captain = ''
        if 'alernateCaptain' in list(page['teams']['home']['players'][pinfo]['person'].keys()):
            alt_captain = page['teams']['home']['players'][pinfo]['person']['alternateCaptain']
        else: 
            alt_captain = ''        
        pos_code = page['teams']['home']['players'][pinfo]['position']['code']
        pos = page['teams']['home']['players'][pinfo]['position']['name']
        pos_type = page['teams']['home']['players'][pinfo]['position']['type']
        pos_abbv = page['teams']['home']['players'][pinfo]['position']['abbreviation']
    
        plr_row = pd.DataFrame([[game_id, tm_home_id, player_id, jersey_num, captain, alt_captain,
                                 pos_code, pos, pos_type,pos_abbv]]
            ,columns=['game_id','team_id','player_id','jersey_num','captain','alt_captain',
                                 'pos_code','pos','pos_type','pos_abbv'])    
        game_plr_info = pd.concat([game_plr_info,plr_row])
        
    game_roster_df = lineups.merge(game_plr_info, how='left', on = ['game_id','team_id','player_id'],
                  suffixes=['gp_info','lineup'])        
   
    
    #### Game Stat Lines ####
    skater_lines = pd.DataFrame()
    for pinfo in tm_away_skaters:
        pid = 'ID'+str(pinfo)
        toi = page['teams']['away']['players'][pid]['stats']['skaterStats']['timeOnIce']
        assists = page['teams']['away']['players'][pid]['stats']['skaterStats']['assists']
        goals = page['teams']['away']['players'][pid]['stats']['skaterStats']['goals']
        shots = page['teams']['away']['players'][pid]['stats']['skaterStats']['shots']
        hits = page['teams']['away']['players'][pid]['stats']['skaterStats']['hits']
        ppg = page['teams']['away']['players'][pid]['stats']['skaterStats']['powerPlayGoals']
        ppa = page['teams']['away']['players'][pid]['stats']['skaterStats']['powerPlayAssists']
        pim = page['teams']['away']['players'][pid]['stats']['skaterStats']['penaltyMinutes']
        fo_w = page['teams']['away']['players'][pid]['stats']['skaterStats']['faceOffWins']
        fo = page['teams']['away']['players'][pid]['stats']['skaterStats']['faceoffTaken']
        takeaways = page['teams']['away']['players'][pid]['stats']['skaterStats']['takeaways']
        giveaways = page['teams']['away']['players'][pid]['stats']['skaterStats']['giveaways']
        shg = page['teams']['away']['players'][pid]['stats']['skaterStats']['shortHandedGoals']
        sha = page['teams']['away']['players'][pid]['stats']['skaterStats']['shortHandedAssists']
        blocks = page['teams']['away']['players'][pid]['stats']['skaterStats']['blocked']
        plus_min = page['teams']['away']['players'][pid]['stats']['skaterStats']['plusMinus']
        ev_toi = page['teams']['away']['players'][pid]['stats']['skaterStats']['evenTimeOnIce']
        pp_toi = page['teams']['away']['players'][pid]['stats']['skaterStats']['powerPlayTimeOnIce']
        sh_toi = page['teams']['away']['players'][pid]['stats']['skaterStats']['shortHandedTimeOnIce']
        
        plr_line = pd.DataFrame([[game_id, tm_away_id, pinfo, toi, assists, goals, shots, hits, ppg, ppa, pim,
                     fo_w, fo, takeaways, giveaways, shg, sha, blocks, plus_min,
                     ev_toi, pp_toi, sh_toi]], 
                                columns = ['game_id','team_id', 'pid','toi','assists','goals','shots',
                               'hits','ppg','ppa','pim','fo_w','fo','takeaways',
                               'giveaways','shg','sha','blocks','plus_min',
                               'ev_toi','pp_toi','sh_toi'])
        
        skater_lines = pd.concat([skater_lines, plr_line])
    
    
        
    for pinfo in tm_home_skaters:
        pid = 'ID'+str(pinfo)
        toi = page['teams']['home']['players'][pid]['stats']['skaterStats']['timeOnIce']
        assists = page['teams']['home']['players'][pid]['stats']['skaterStats']['assists']
        goals = page['teams']['home']['players'][pid]['stats']['skaterStats']['goals']
        shots = page['teams']['home']['players'][pid]['stats']['skaterStats']['shots']
        hits = page['teams']['home']['players'][pid]['stats']['skaterStats']['hits']
        ppg = page['teams']['home']['players'][pid]['stats']['skaterStats']['powerPlayGoals']
        ppa = page['teams']['home']['players'][pid]['stats']['skaterStats']['powerPlayAssists']
        pim = page['teams']['home']['players'][pid]['stats']['skaterStats']['penaltyMinutes']
        fo_w = page['teams']['home']['players'][pid]['stats']['skaterStats']['faceOffWins']
        fo = page['teams']['home']['players'][pid]['stats']['skaterStats']['faceoffTaken']
        takeaways = page['teams']['home']['players'][pid]['stats']['skaterStats']['takeaways']
        giveaways = page['teams']['home']['players'][pid]['stats']['skaterStats']['giveaways']
        shg = page['teams']['home']['players'][pid]['stats']['skaterStats']['shortHandedGoals']
        sha = page['teams']['home']['players'][pid]['stats']['skaterStats']['shortHandedAssists']
        blocks = page['teams']['home']['players'][pid]['stats']['skaterStats']['blocked']
        plus_min = page['teams']['home']['players'][pid]['stats']['skaterStats']['plusMinus']
        ev_toi = page['teams']['home']['players'][pid]['stats']['skaterStats']['evenTimeOnIce']
        pp_toi = page['teams']['home']['players'][pid]['stats']['skaterStats']['powerPlayTimeOnIce']
        sh_toi = page['teams']['home']['players'][pid]['stats']['skaterStats']['shortHandedTimeOnIce']
        
        plr_line = pd.DataFrame([[game_id,tm_home_id, pinfo, toi, assists, goals, shots, hits, ppg, ppa, pim,
                     fo_w, fo, takeaways, giveaways, shg, sha, blocks, plus_min,
                     ev_toi, pp_toi, sh_toi]], 
                                columns = ['game_id','team_id', 'pid','toi','assists','goals','shots',
                               'hits','ppg','ppa','pim','fo_w','fo','takeaways',
                               'giveaways','shg','sha','blocks','plus_min',
                               'ev_toi','pp_toi','sh_toi'])
        
        skater_lines = pd.concat([skater_lines, plr_line])
        
    
    
    goalie_lines = pd.DataFrame()
    for pinfo in tm_away_goalies:
        pid = 'ID'+str(pinfo)
        toi = page['teams']['away']['players'][pid]['stats']['goalieStats']['timeOnIce']
        assists = page['teams']['away']['players'][pid]['stats']['goalieStats']['assists']
        goals = page['teams']['away']['players'][pid]['stats']['goalieStats']['goals']
        pim = page['teams']['away']['players'][pid]['stats']['goalieStats']['pim']
        shots = page['teams']['away']['players'][pid]['stats']['goalieStats']['shots']
        saves = page['teams']['away']['players'][pid]['stats']['goalieStats']['saves']
        pp_saves = page['teams']['away']['players'][pid]['stats']['goalieStats']['powerPlaySaves']
        sh_saves = page['teams']['away']['players'][pid]['stats']['goalieStats']['shortHandedSaves']
        ev_saves = page['teams']['away']['players'][pid]['stats']['goalieStats']['evenSaves']
        sh_shots = page['teams']['away']['players'][pid]['stats']['goalieStats']['shortHandedShotsAgainst']
        ev_shots = page['teams']['away']['players'][pid]['stats']['goalieStats']['evenShotsAgainst']
        pp_shots = page['teams']['away']['players'][pid]['stats']['goalieStats']['powerPlayShotsAgainst']
        try:
            decision = page['teams']['away']['players'][pid]['stats']['goalieStats']['decision']            
        except:
            decision = ''            
        try:
            save_pct = page['teams']['away']['players'][pid]['stats']['goalieStats']['savePercentage']
        except:
            save_pct = -1
        try:
            pp_save_pct = page['teams']['away']['players'][pid]['stats']['goalieStats']['powerPlaySavePercentage']
        except: 
            pp_save_pct = -1
        try: 
            ev_save_pct = page['teams']['away']['players'][pid]['stats']['goalieStats']['evenStrengthSavePercentage']
        except:
            ev_save_pct = -1
    
        g_line = pd.DataFrame([[game_id, tm_away_id, pinfo, toi, assists, goals,
                                     pim, shots, saves, pp_saves, sh_saves, ev_saves,
                                     sh_shots, ev_shots, pp_shots, decision, save_pct,
                                     pp_save_pct, ev_save_pct]],
                                   columns = ['game_id', 'tm_away_id', 'pid', 'toi', 'assists', 'goals',
                                     'pim', 'shots', 'saves', 'pp_saves', 'sh_saves', 'ev_saves',
                                     'sh_shots', 'ev_shots', 'pp_shots', 'decision', 'save_pct',
                                     'pp_save_pct', 'ev_save_pct'])
        goalie_lines = pd.concat([goalie_lines, g_line])
    
    
    for pinfo in tm_home_goalies:
        pid = 'ID'+str(pinfo)
        toi = page['teams']['home']['players'][pid]['stats']['goalieStats']['timeOnIce']
        assists = page['teams']['home']['players'][pid]['stats']['goalieStats']['assists']
        goals = page['teams']['home']['players'][pid]['stats']['goalieStats']['goals']
        pim = page['teams']['home']['players'][pid]['stats']['goalieStats']['pim']
        shots = page['teams']['home']['players'][pid]['stats']['goalieStats']['shots']
        saves = page['teams']['home']['players'][pid]['stats']['goalieStats']['saves']
        pp_saves = page['teams']['home']['players'][pid]['stats']['goalieStats']['powerPlaySaves']
        sh_saves = page['teams']['home']['players'][pid]['stats']['goalieStats']['shortHandedSaves']
        ev_saves = page['teams']['home']['players'][pid]['stats']['goalieStats']['evenSaves']
        sh_shots = page['teams']['home']['players'][pid]['stats']['goalieStats']['shortHandedShotsAgainst']
        ev_shots = page['teams']['home']['players'][pid]['stats']['goalieStats']['evenShotsAgainst']
        pp_shots = page['teams']['home']['players'][pid]['stats']['goalieStats']['powerPlayShotsAgainst']
        try:
            decision = page['teams']['home']['players'][pid]['stats']['goalieStats']['decision']            
        except:
            decision = ''            
        try:
            save_pct = page['teams']['home']['players'][pid]['stats']['goalieStats']['savePercentage']
        except:
            save_pct = -1
        try:
            pp_save_pct = page['teams']['home']['players'][pid]['stats']['goalieStats']['powerPlaySavePercentage']
        except: 
            pp_save_pct = -1
        try: 
            ev_save_pct = page['teams']['home']['players'][pid]['stats']['goalieStats']['evenStrengthSavePercentage']
        except:
            ev_save_pct = -1
    
        g_line = pd.DataFrame([[game_id, tm_home_id, pinfo, toi, assists, goals,
                                     pim, shots, saves, pp_saves, sh_saves, ev_saves,
                                     sh_shots, ev_shots, pp_shots, decision, save_pct,
                                     pp_save_pct, ev_save_pct]],
                                   columns = ['game_id', 'tm_away_id', 'pid', 'toi', 'assists', 'goals',
                                     'pim', 'shots', 'saves', 'pp_saves', 'sh_saves', 'ev_saves',
                                     'sh_shots', 'ev_shots', 'pp_shots', 'decision', 'save_pct',
                                     'pp_save_pct', 'ev_save_pct'])
        goalie_lines = pd.concat([goalie_lines, g_line])        
    
    coach_df = pd.DataFrame()
    for l in range(len(page['teams']['away']['coaches'])):
        name = page['teams']['away']['coaches'][l]['person']['fullName']
        coach_type = page['teams']['away']['coaches'][l]['position']['name']
        coach_row = pd.DataFrame([[game_id, tm_away_id, name, coach_type,'away']],
                                 columns=['game_id', 'team','coach_name',
                                          'coach_type', 'home_away'])
        coach_df = pd.concat([coach_row,coach_df])
    for l in range(len(page['teams']['home']['coaches'])):
        name = page['teams']['home']['coaches'][l]['person']['fullName']
        coach_type = page['teams']['home']['coaches'][l]['position']['name']
        coach_row = pd.DataFrame([[game_id, tm_home_id, name, coach_type,'home']],
                                 columns=['game_id', 'team','coach_name',
                                          'coach_type', 'home_away'])
        coach_df = pd.concat([coach_row,coach_df])    


    return (game_roster_df, skater_lines, goalie_lines, coach_df) 


################
################
################

def boxscore_db(game_lineup_df, skater_lines, goalie_lines, coach_df):

    #game_lineup
    game_id = game_lineup_df.game_id.iloc[0]
    game_del_q = '''delete from nhl_api.game_lineup
                    where game_id = %s
                    '''%(game_id)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(game_lineup_df)
    lineup_load =  """copy nhl_api.game_lineup
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(game_del_q)
        cursor.execute(lineup_load)
        connection.commit()        
        print("Game Lineups loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')  

    #skater_lines
    game_id = game_lineup_df.game_id.iloc[0]
    game_del_q = '''delete from nhl_api.skater_lines
                    where game_id = %s
                    '''%(game_id)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(skater_lines)
    skater_load =  """copy nhl_api.skater_lines
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(game_del_q)
        cursor.execute(skater_load)
        connection.commit()        
        print("Skater Lines loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')  

    #goalie_lines
    game_id = game_lineup_df.game_id.iloc[0]
    game_del_q = '''delete from nhl_api.goalie_lines
                    where game_id = %s
                    '''%(game_id)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(goalie_lines)
    goalie_load =  """copy nhl_api.goalie_lines
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(game_del_q)
        cursor.execute(goalie_load)
        connection.commit()        
        print("Goalie Lines loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')  
    
    #coaches
    game_id = game_lineup_df.game_id.iloc[0]
    game_del_q = '''delete from nhl_api.game_coaches
                    where game_id = %s
                    '''%(game_id)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(coach_df)
    coach_load =  """copy nhl_api.game_coaches
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(game_del_q)
        cursor.execute(coach_load)
        connection.commit()        
        print("Coach Info loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')  
    




#####################
#####################
#####################

def game_shifts(game_id):
    
    ### SHIFTS
    shift_api_end = 'https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId='+game_id
    response = requests.get(shift_api_end, params={'format':'json'}) 
    page = response.json() #two item dict
    
    shift_df = pd.DataFrame()
    for i in range(len(page['data'])):
        data_id = page['data'][i]['id']
        detail_code = page['data'][i]['detailCode']
        duration = page['data'][i]['duration']
        end_time = page['data'][i]['endTime']
        event_description = page['data'][i]['eventDescription']
        event_details = page['data'][i]['eventDetails']
        event_num = page['data'][i]['eventNumber']
        game_id = page['data'][i]['gameId']
        period = page['data'][i]['period']
        player_id = page['data'][i]['playerId']
        shift_num = page['data'][i]['shiftNumber']
        start_time = page['data'][i]['startTime']
        team_id = page['data'][i]['teamId']
        type_code = page['data'][i]['typeCode']
        
        shift_row = pd.DataFrame([[data_id, detail_code, game_id, player_id, team_id,
                                   period, shift_num, start_time, end_time, duration,
                                   type_code, event_num, event_details, event_description]],
                                 columns = ['data_id', 'detail_code','game_id','player_id',
                                            'team_id','period','shift_num','start_time',
                                            'end_time','duration','type_code','event_id',
                                            'event_details','event_description'])
    
        shift_df = pd.concat([shift_df, shift_row])


    return(shift_df)

def game_shifts_db(game_shifts):
    
    #game_info
    game_id = game_shifts.game_id.iloc[0]

    game_del_q = '''delete from nhl_api.game_info
                    where game_id = %s
                    '''%(game_id)
    ########
    # LOAD THE CSV
    connection = psycopg2.connect(user='postgres',
                                password='654yam',
                                host = '127.0.0.1',
                                port = '5432',
                                database = 'pucknight')
    cursor = connection.cursor()       
    
    pg_db.df_to_csv(game_shifts)
    game_shifts_load =  """copy nhl_api.game_shifts
    FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
      DELIMITER '|' 
      CSV HEADER 
      QUOTE '\"' 
      ESCAPE '''';
      """
    try:       
        cursor.execute(game_del_q)
        cursor.execute(game_shifts_load)
        connection.commit()        
        print("Game Shifts loaded successfully in PostgreSQL ")
    
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL',error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')  