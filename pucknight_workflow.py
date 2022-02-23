#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 19:02:12 2021

@author: jdifrisco
"""
from pucknight import ep_soup, nhl_sel, pg_db, pn_queries, nhl_api

import time
import datetime
from psycopg2 import Error

import pandas as pd
import numpy as np

# import sqlalchemy as db
# from sqlalchemy import create_engine, MetaData, Table


# DAILY REFRESH
################################################
all_leagues= [
                # 'NHL'
                 'KHL'
                ,'SHL', 'Liiga'
                , 'Champions HL'
                , 'OG'
                , 'DEL',  'HockeyAllsvenskan', 'NL', 'AHL'
                , 'Czech', 'Slovakia' 
                ,  'WHL', 'OHL', 'QMJHL'
                , 'J20 SuperElit', 'U20-Elit'
                , 'NCAA'
                , 'AJHL', 'BCHL','MJAHL','SIJHL','SJHL','CCHL'
                , 'NOJHL', 'OJHL', 'MJAHL','QJHL'
                , 'USHL', 'NAHL'
                , 'NCDC', 'USPHL Premier', 'USPHL Elite'
                , '18U AAA', '16U AAA'
                , 'AYHL 18U', 'AYHL 16U', 'AYHL 15U', 'AYHL 14U'
                , 'NAPHL 18U','NAPHL 16U', 'NAPHL 15U', 'NAPHL 14U'
                , 'AEHL U18', 'AEHL U15'
                , 'CSSHL U16', 'CSSHL U15'
                 ]

for al in all_leagues:     
    pg_db.league_quick_refresh(al,'2021-22')
    time.sleep(5)
    print("refresh complete for " + al)
    
# pg_db.league_level_refresh('WC','2020-21')    
# pg_db.league_quick_refresh('OG', '2021-22')
# pg_db.league_full_refresh('FPHL', '2021-22')



#############################
##### NHL API Refresh #######
#############################

szn = '2021-22'
    
### Roster Refresh
teams, ros = nhl_api.sznTeamsRosters(szn)
nhl_api.rosters_db(ros)

## Get the dimPlayer page for anybody missing
missing_players_q = """select distinct player_id
from nhl_api.game_lineup gl 
join nhl_api.game_ids gid
    on gl.game_id = gid.game_id
where 
gid.season = '%s'
and gid.game_type = 'Regular Season'
and player_id not in(
	select player_id from nhl_api.dim_players	
)"""%(szn)
pids_needed = list(pd.read_sql_query(missing_players_q, con=pg_db.engine).player_id)

for pid in pids_needed:
    try:
        plr_info = nhl_api.player_info(pid)
        nhl_api.player_info_db(plr_info)
    except:
        print('Error in Player Dimension for :' + str(pid))
        continue

### Use Existing Game IDs to gather Game Info        
games_df = pn_queries.api_games(season=szn, game_type='Regular Season')
### Games Today
## Search Games by Date Range
gd = datetime.date(2022, 2, 22)
gms_day = games_df[(games_df.game_date <= gd)
          & (games_df.game_status != 'Final') 
         ]\
    [['game_id','game_date','home_team','away_team','game_status']]

# gms_day
nhl_api.games_refresh(list(gms_day.game_id))
games_df = pn_queries.api_games(season=szn, game_type='Regular Season')


################################################

# Player Stats Updated Through:
pd.read_sql_query('select max(game_date) from nhl_game_stats.skaters_misc', con=pg_db.engine).iloc[0]

################################################
### NHL Game Stats 
sd = '2022-02-16'
ed = '2022-02-21'


nhl_games = nhl_sel.nhl_game_results(sd, ed)
pg_db.nhl_db(nhl_games, 'nhl_games')

nhl_daily_s = nhl_sel.nhl_daily_stats_skaters(sd, ed)
pg_db.nhl_db(nhl_daily_s, 'skaters')

nhl_daily_g = nhl_sel.nhl_daily_stats_goalies(sd, ed)
pg_db.nhl_db(nhl_daily_g, 'goalies')

nhl_daily_fo = nhl_sel.nhl_daily_stats_faceoffs(sd, ed)
pg_db.nhl_db(nhl_daily_fo, 'skaters_faceoffs')

nhl_daily_misc = nhl_sel.nhl_daily_stats_misc(sd, ed)
pg_db.nhl_db(nhl_daily_misc, 'skaters_misc')

nhl_daily_sat = nhl_sel.nhl_daily_stats_sat(sd, ed)
pg_db.nhl_db(nhl_daily_sat, 'skaters_sat')

nhl_daily_shottype = nhl_sel.nhl_daily_stats_shottype(sd, ed)
pg_db.nhl_db(nhl_daily_shottype, 'skaters_shottype')

nhl_daily_pp = nhl_sel.nhl_daily_stats_powerplay(sd, ed)
pg_db.nhl_db(nhl_daily_pp, 'skaters_pp')

nhl_daily_pen = nhl_sel.nhl_daily_stats_penalties(sd, ed)
pg_db.nhl_db(nhl_daily_pen, 'skaters_penalties')

nhl_daily_toi = nhl_sel.nhl_daily_stats_toi(sd, ed)
pg_db.nhl_db(nhl_daily_toi, 'skaters_toi')

nhl_daily_shootout = nhl_sel.nhl_daily_stats_shootout(sd,ed)
pg_db.nhl_db(nhl_daily_shootout, 'skaters_shootout')











######
game_id = 2021020583
######

home = games_df[games_df['game_id']==game_id].home_team.iloc[0]
away = games_df[games_df['game_id']==game_id].away_team.iloc[0]

games_df[games_df['game_id']==game_id]\
    [['game_date','home_team','home_score','away_team','away_score','venue']].iloc[0]

lineups = pn_queries.api_lineups(game_id)
# lineups.columns
lineups[['team_name','jersey_num','playername','pos_abbv']]

sl = pn_queries.api_s_lines(game_id)
# sl.columns
sl[['team_name','playername','jersey_num','pos_abbv','toi','goals','assists']]

gl = pn_queries.api_g_lines(game_id)
# gl.columns
gl[['team_name', 'playername','jersey_num','pos_abbv','toi','shots','saves']]


events = pn_queries.game_events(game_id)
# events.columns

events[['period','action','event_type']]

events.groupby(['event','action']).agg({'team_name':'count'})

goals = events[events['event']=='GOAL']\
    .groupby(['period','per_time_rem','team_name','action'
              ,'playername','home_score','away_score'])\
        .agg({'action':'max','x_coord':'max', 'y_coord':'max'})\
            .sort_values(['period','per_time_rem'],ascending=[True,False])
        

shots = events[(events['event']=='SHOT') |
       (events['event']=='BLOCKED_SHOT') |
       (events['event']=='MISSED_SHOT')]\
    .groupby(['period','per_time_rem','team_name','action'
              ,'playername','home_score','away_score'])\
        .agg({'action':'max', 'x_coord':'max', 'y_coord':'max'})\
            .sort_values(['period','per_time_rem'], ascending=[True,False])
                                

lineups.columns


##############################
##############################
##############################


def str_to_date(date_string):
    try:
        dt = datetime.strptime(date_string,'%b. %d, %Y')
    except(Exception,Error) as error:
        dt = datetime.strptime(date_string,'%b %d, %Y')
    except(Exception,Error) as error:
        dt = date_string

    return(dt)

### CAPFRIENDLY ###
cash = ep_soup.salary('2021', '1900-00', 'inactive')        
    # Get the Date into a datetime format
cash['signing_date'] = cash['signing_date'].apply(str_to_date)
    ##Take out dead guys
cash.reset_index(inplace=True)
dec_ind = cash[cash['age'].str.contains("Deceased")].index
for di in dec_ind:         
    cash['age'].loc[di] = int(cash.loc[di]['age'].\
                                split('Deceased (')[1].\
                                    split(')')[0])
cash.drop(columns='index', inplace=True)
    ## LOAD CLEANED DF
pg_db.contract_load(cash)

### ### ### ### ### ###

# cash.iloc[12]
cash[cash['team']=='CHI'][['playername','team','aav','signing_date','full_link']]

cash.iloc[287]
cash['playername']




################################################
league_dict = {
      'Pro Tier 1': ['NHL','KHL', 'SHL']
    
    , 'Pro Tier 2': ['AHL','DEL','Liiga','Czech', 'NL', 'Champions HL']
                   
    , 'Pro Tier 3': ['Czech','ICEHL','Belarus','EIHL','Ligue Magnus','Slovakia',
                     'Norway','Poland','Metal Ligaen','EBEL', 'Ukraine'
                     'Continental Cup','SL']
    
    , 'Pro Tier 4': ['HockeyAllsvenskan','Mestis', 'ECHL']
    , 'Pro Tier 5': ['HockeyEttan']
    
    , 'Major Junior': ['WHL', 'OHL', 'QMJHL', 'NCAA','J20 Nationell', 'MHL']
    
    , 'Junior A': ['USHL', 'NAHL','NCDC',
                   'AJHL', 'BCHL','MJAHL', 'SIJHL','SJHL',
                   'CCHL', 'NOJHL', 'OJHL', 
                   'MJAHL','QJHL', 
                   'J20 Regionell', 
                   
                 # 'RBC Cup',  'U20-Elit','Czech U20',
                 ]
    , 'Junior Z': ['J20 Div.1', 'J20 Div.2']
    
    , 'International': ['WC','Olympics','OGQ', #'WC D1A', 'WC D1B'
                        'U20 WJC', #'U20 WJC D1A', 'U20 WJC D1B',
                        'U18 WJC', 'Hlinka Gretzky Cup'
                        ]
    
    , 'College Tier 2': ['NCAA III', 'ACHA', 'ACHA II', 'ACHA III','USports']
    , 'USHS': ['USDP', 'USHS-Prep', 'USHS-MN', 'USHS-MI', 'USHS-NY','USHS-MA']
    # , '18U'
    
    }


champ_hl_leagues = {1: 'SHL'
                    2: 'NL'
                    3: 'DEL'
                    4: 'Liiga'
                    5: 'Czech'
                    6: 'ICEHL'
                    7: 'Belarus'
                    8: 'EIHL'
                    9: 'Ligue Magnus'
                    10: 'Slovakia'
                    11: 'Norway'
                    12: 'Poland'
                    13: 'Metal Ligaen'
                    100: 'EBEL'
                    # 101: 
                    }

for l in league_dict['Pro Tier 2']:
    time.sleep(3)
    print(str(l))
    pg_db.league_quick_refresh(l, '2021-22')



    
htv_leagues = {
    'Intl_Pro': ['Czech','Ligue Magnus','SPHL','Latvia','French Cup'],
    'CAN_junior_A': ['AJHL','BCHL','CCHL','MJHL','MJAHL','NOJHL','SJHL','SIJHL'],
    'USA_tier_I': ['USHL'],
    'USA_tier_II': ['NAHL','NCDC','USPHL Elite','USPHL Elite'],
    'CAN_junior_B': ['EOJBHL','GOHL','KIJHL','OJHL','PIJHL','VIJHL'],
    'CAN_U18': ['CSSHL U18','QMAAA','MU18HL'],
    'CAN_U17': ['CSSHL U17'],
    'CAN_U16': ['ALLIANCE U18','ALLIANCE U16','CSSHL U16'],
    'CAN_U15': ['CSSHL U15'],
    'USA_AAA': ['16U AAA','18U AAA','15U AAA','NAPHL 18U','USHS-Prep']
    }

for lev,ll in htv_leagues.items():
    for lg in ll[:5]:
        pg_db.league_quick_refresh(lg, '2021-22')
        print("Leage Refresh Completed for: "+str(lg))















