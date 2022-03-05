#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov  5 09:45:49 2021

@author: jdifrisco
"""
from pucknight import pg_db
from pucknight import ep_soup
from pucknight import nhl_sel
from pucknight import nhl_api

from sqlalchemy import create_engine, MetaData, Table
import psycopg2
from psycopg2 import Error
import os
import time

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


### FILLING IN THE GAPS

## Season list for loops
seasons = [
'2021-22', 
'2020-21', '2019-20', '2018-19', '2017-18', '2016-17',
'2015-16', '2014-15', '2013-14', '2012-13', '2011-12', 
'2010-11', '2009-10', '2008-09', '2007-08', '2006-07', '2005-06',
'2004-05', '2003-04', '2002-03', '2001-02','2000-01', 
'1999-00', '1998-99', '1997-98', 
'1996-97','1995-96', '1994-95','1993-94',
'1992-93', '1991-92', '1990-91', '1989-90', '1988-89',
'1998-99', '1997-98', '1996-97', '1995-96', '1994-95',
'1993-94', '1992-93', '1991-92', '1990-91',
'1989-90', '1988-89', '1987-88', '1986-87', '1985-86',
'1984-85','1983-84', '1982-83', '1981-82', '1980-81'          
]


for szn in seasons:
    pg_db.league_quick_refresh('J20 SuperElit',szn)


league_dict = {
      'Pro Tier 1': ['NHL','KHL', 'SHL']
    
    , 'Pro Tier 2': ['AHL','DEL','Liiga','Czech', 'NL', 'Champions HL']
                   
    , 'Pro Tier 3': ['HockeyAllsvenskan','Mestis', 'ECHL','ICEHL']
    , 'Pro Tier 4': ['HockeyEttan','AlpsHL']
    
    , 'Major Junior': ['WHL', 'OHL', 'QMJHL', 'NCAA','J20 SuperElit', 'MHL']
    
    , 'Junior A': ['USHL', 'NAHL','NCDC',
                   'AJHL', 'BCHL','MJAHL', 'SIJHL','SJHL',
                   'CCHL', 'NOJHL', 'OJHL', 
                   'MJAHL','QJHL',
                   'J20 Elit'
                 # 'RBC Cup',  'U20-Elit','Czech U20',
                 ]
    
    , 'International': ['WC','Olympics','OGQ', #'WC D1A', 'WC D1B'
                        'U20 WJC', #'U20 WJC D1A', 'U20 WJC D1B',
                        'U18 WJC', 'Hlinka Gretzky Cup'
                        ]
    
    , 'College Tier 2': ['NCAA III', 'ACHA', 'ACHA II', 'ACHA III','USports']
    , 'USHS': ['USDP', 'USHS-Prep', 'USHS-MN', 'USHS-MI', 'USHS-NY','USHS-MA']
    # , '18U'
    
    }


pipeline_dict = {
      'USA': ['NHL','AHL','ECHL','SPHL',
              'NCAA', 'USHL', 'NAHL', 'USDP','NCDC'
              'AYHL 13U', 'AYHL 14U', 'AYHL 16U','AtJHL', 'EmJHL', 'EJHL','MetJHL',
              'T1EHL 13U', 'T1EHL 14U', 'T1EHL 16U', 'T1EHL 18U', 
              'USHS-Prep', 'USHS-MN', 'USHS-MI', 'USHS-NY','USHS-MA','USHS-RI','USHS-PA',
              '14U A','16U AAA','18U AAA','15U AAA','14U AAA','USPHL 18U','USPHL 16U'
              'BEAST 18U','BEAST 16U','EHL'
              'NAPHL 18U','HPHL 16U','HPHL 15U','HPHL 14U','HPHL 13U']
      
      'Canada': ['OHL','WHL','QMJHL','M-Cup',
                 'AJHL', 'BCHL','MJAHL', 'SIJHL','SJHL','CCHL', 'NOJHL', 'OJHL','QJHL',
                 'ALLIANCE U16', 'ALLIANCE U18','AMBHL','AMHL','AMMHL','BC U18','BCEHL U18','CJHL',
                 'COJCHL','CSSHL U18','CSSHL U17','CSSHL U15','CSSHL U16','GNML','GOJHL','GTHL U16', 'GTHL U15',
                 'MJHL','MMHL' ,'MPHL','NBPEIMMHL',
                 'OPJHL','PEIBAAA','PIJHL','QMAA','QMAAA', 'QMEAA', 'SMAAAHL', 'VIJHL', 'WOJCHL','KIJHL',
                 'OEHL U18','OEHL U16','HEO U15','ETAHL U15','ETAHL U16','WSI U15'
                 'Challenge Midget AAA CCM','MMHL','Telus Cup','TCQ-W','MU18HL','WAAA U17','WAAA U15'
                 'Big Nickel Bantam', 'U18 A1','Dudley Hewitt Cup'
                 'EOJHL','EOJBHL','GHL','CAHS','Sutherland Cup',]
      
      'Sweden': ['SHL','HockeyAllsvenskan', 'Elitserien',
                 'J20 SuperElit', 'J20 Elit','J20 Div.1', 'J20 Div.2',
                 'J18 Elit','J18 Allsvenskan','J18 Div.1','J18 Div.2']
      
      'Finland': ['Liiga','SM-liiga','Mestis','U20 SM-liiga','U18 I-divisioona','U18 SM-sarja', 'U16 SM']
      
      'Russia': ['KHL', 'MHL', 'VHL']
      
      'Czech Rep.': ['Czech', 'Czech U20', 'Czech U18', 'Czech U20 3','Czech3','Czech U16']
      
      'Slovakia': ['Slovakia','Slovakia U18','Slovakia U20','Slovakia2','Czech U18']
      
      'Switzerland': ['NL','Elite Jr. A','Elite Novizen','NLA',
                      'Swiss Cup Q','Novizen A','SwissDiv1']
      
      'Denmark': ['Denmark','Denmark2','Denmark U20','Denmark U17']
      
      'Germany': ['DEL','DNL','Schüler-BL']
      
      'France': ['Ligue Magnus', ]
      
      'International': [['WC','Olympics','OGQ', 'WCup' #'WC D1A', 'WC D1B'
                        'U20 WJC', #'U20 WJC D1A', 'U20 WJC D1B',
                        'U18 WJC', 'Hlinka Gretzky Cup',
                        'WHC-17', 'WJAC-19',
                        ]
    }
      
      

pg_db.league_quick_refresh('Denmark','2021-22')                        
                        

yrs = ['2020','2019','2018','2017','2016','2015','2014','2013','2012'
       ,'2011', '2010','2009','2008','2007','2006','2005','2004','2003'
       , '2002','2001','2000','1999']
def str_to_date(date_string):
    try:
        dt = datetime.datetime.strptime(date_string,'%b. %d, %Y')
    except(Exception,Error) as error:
        dt = datetime.datetime.strptime(date_string,'%b %d, %Y')
    except(Exception,Error) as error:
        dt = date_string
        
    return(dt)

    
cash = ep_soup.salary('2021', '1900-01')    
    # Get the Date into a datetime format
cash['signing_date'] = cash['signing_date'].apply(str_to_date)
cash.reset_index(inplace=True)
dec_ind = cash[cash['age'].str.contains("Deceased")].index
for di in dec_ind:         
    cash['age'].loc[di] = int(cash.loc[di]['age'].\
                                split('Deceased (')[1].\
                                    split(')')[0])
cash.drop(columns='index', inplace=True)

pg_db.contract_load(cash)        




### CAPFRIENDLY ###
cash = ep_soup.salary('2021', '2021-21')        
    # Get the Date into a datetime format
# cash['signing_date'] = cash['signing_date'].apply(str_to_date)
#     #Take out dead guys
# cash.reset_index(inplace=True)
# dec_ind = cash[cash['age'].str.contains("Deceased")].index
# for di in dec_ind:         
#     cash['age'].loc[di] = int(cash.loc[di]['age'].\
#                                 split('Deceased (')[1].\
#                                     split(')')[0])
# cash.drop(columns='index', inplace=True)
    # LOAD CLEANED DF
# pg_db.contract_load(cash)

### ### ### ### ### ###

# cash.iloc[12]
cash[cash['team']=='CHI'][['playername','team','aav']]

cash['playername']

# cash['age'].loc[1477] = int(cash.loc[1365]['age'].\
#                             split('Deceased (')[1].\
#                                 split(')')[0]
# for di in dec_ind:
#     cash['age'].loc[di] = int(cash.loc[di]['age'].\
#                               apply(lambda x: x.split('Deceased (')[1]).\
#                                   apply(lambda x: x.split(')')[0]))
    
# cash.drop(columns='level_0', inplace=True)

cash.iloc[dec_ind].iloc[0]
cash.iloc[0]




#######################################
#### NHL Game Stats Records Check #####
#######################################
db_check_q = """
with
gms as
(
select game_date, count(*) tms, count(*)*18 exp_skaters
from nhl_game_stats.nhl_games
where game_date > '2022-01-01'
group by game_date

),

s as
(
select game_date, count(*) s_cnt
from nhl_game_stats.skaters
where season = '2021-22'
group by game_date
order by game_date asc
	)
, 
misc as
(
select game_date, count(*) misc_cnt
from nhl_game_stats.skaters_misc
where season = '2021-22'
group by game_date
order by game_date asc
	)
	
, sat as
(
select game_date, count(*) sat_cnt
from nhl_game_stats.skaters_sat
where season = '2021-22'
group by game_date
order by game_date asc
	)
	
, fo as
(
select game_date, count(*) fo_cnt
from nhl_game_stats.skaters_faceoffs
where season = '2021-22'
group by game_date
order by game_date asc
	)
	
, pen as
(
select game_date, count(*) pen_cnt
from nhl_game_stats.skaters_penalties
where season = '2021-22'
group by game_date
order by game_date asc
	)
	
, pp as
(
select game_date, count(*) pp_cnt
from nhl_game_stats.skaters_pp
where season = '2021-22'
group by game_date
order by game_date asc
	)
	
, st as
(
select game_date, count(*) st_cnt
from nhl_game_stats.skaters_shottype
where season = '2021-22'
group by game_date
order by game_date asc
	)	
	
select 
gms.game_date
, gms.tms, gms.exp_skaters
, s_cnt, misc_cnt, sat_cnt, pen_cnt, pp_cnt, st_cnt--, fo_cnt
from gms
left join s
	on gms.game_date = s.game_date
left join misc
	on gms.game_date = misc.game_date
left join sat
	on gms.game_date = sat.game_date
left join pen
	on gms.game_date = pen.game_date
left join pp
	on gms.game_date = pp.game_date
left join st
	on gms.game_date = st.game_date
left join fo
	on gms.game_date = fo.game_date		
    
order by gms.game_date desc
	"""
    
db_check = pd.read_sql(db_check_q, con=pg_db.engine)   
db_check.iloc[:10][['game_date','exp_skaters','s_cnt','misc_cnt','sat_cnt',
                    'pen_cnt','pp_cnt']]

db_check.columns

#######################################
#### NHL PlayerID Crosswalk Update ####
#######################################
"""
nhl_game_stats.playerid_xwalk

-- Auto-insert for players where playername matches (per team, per season)
-- Players need to have a game-played to be entered
    -- eg. Jack Eichel shows as missing, not on name difference, but due to 0 gp
"""

## ADD A SEASON'S PLAYERS
szn = '2021-22'

pid_truncate = """delete from nhl_game_stats.playerid_xwalk
where season = '%s'
"""%(szn)

pid_add_s = """
INSERT INTO nhl_game_stats.playerid_xwalk 
select pid.playerid, pid.playername as ep_playername
, t.team, ta.team as team_abbv
, chel_szn.playername as nhl_playername, chel_szn.team nhl_team
, t.season
from players.playerid pid
join teams.season_stats_s sss
	on sss.playerid = pid.playerid
join teams.teams t
	on sss.teamid = t.teamid
join nhl_game_stats.team_abbvs ta
	on t.team = ta.team_name
	and t.season = ta.season
join 
	( 
		select playername, team, season
		, count(game_date) gp, max(game_date)
			from nhl_game_stats.skaters 
			where season = '%s'
			group by playername, team, season
	) chel_szn
	on pid.playername = chel_szn.playername
	and t.season = chel_szn.season
	and ta.team = chel_szn.team
where t.league = 'NHL' and t.season = '%s'
order by t.team, chel_szn.team
"""%(szn,szn)

## Add the Goalies
pid_add_g = """
INSERT INTO nhl_game_stats.playerid_xwalk 
select pid.playerid, pid.playername as ep_playername
, t.team, ta.team as team_abbv
, chel_szn.playername as nhl_playername, chel_szn.team nhl_team
, t.season
from players.playerid pid
join teams.season_stats_g ssg
	on ssg.playerid = pid.playerid
join teams.teams t
	on ssg.teamid = t.teamid
join nhl_game_stats.team_abbvs ta
	on t.team = ta.team_name
	and t.season = ta.season
join 
	( 
		select playername, team, season
		, count(game_date) gp, max(game_date)
			from nhl_game_stats.goalies 
			where season = '%s'
			group by playername, team, season
	) chel_szn
	on pid.playername = chel_szn.playername
	and t.season = chel_szn.season
	and ta.team = chel_szn.team
where t.league = 'NHL' and t.season = '%s'
order by t.team, chel_szn.team
"""%(szn,szn)


## Trunc and Load all manually adjusted players
nhl_api_playerid_xwalk_supp.xlsx
pid_supp = pd.read_excel('/Users/jdifrisco/Desktop/PuckNight/playerid_xwalk_supp.xlsx')

pid_supp = pid_supp[pid_supp.nhl_playername.notnull()]
insert_rows = pid_supp[pid_supp['season']==szn]



pg_db.df_to_csv(insert_rows)    
            

connection = psycopg2.connect(user='postgres',
                            password='654yam',
                            host = '127.0.0.1',
                            port = '5432',
                            database = 'pucknight')
cursor = connection.cursor()       

load_query =  """copy nhl_game_stats.playerid_xwalk
        FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
         DELIMITER '|' 
         CSV HEADER 
         QUOTE '\"' 
         ESCAPE '''';
         """
try:       
    cursor.execute(pid_truncate)
    cursor.execute(pid_add_s)
    cursor.execute(pid_add_g)
    cursor.execute(load_query)
    connection.commit()
    print("PlayerID XWalk Successfully Reloaded for " + szn)        

except(Exception,Error) as error:
    print('Error while connecting to PostgreSQL',error)
    
finally:
    if (connection):
        cursor.close()
        connection.close()
        print('PostgreSQL connection is closed')      


###########
###########
###########
###########




### Get valid players not included in the Select insert
missing_players = """
select pid.playerid, pid.playername as ep_playername, sss.gp
, t.team, ta.team team_abbv, t.season
, px.nhl_playername, px.nhl_team
from teams.season_stats_s sss
join players.playerid pid
	on sss.playerid = pid.playerid
join teams.teams t
	on sss.teamid = t.teamid
join nhl_game_stats.team_abbvs ta
	on t.team = ta.team_name
	and t.season = ta.season
left join nhl_game_stats.playerid_xwalk px
	on pid.playerid = px.playerid
	and ta.team = px.team_abbv
	and t.season = px.season
where t.league = 'NHL' and sss.gp>0
and t.season = '%s'
order by px.nhl_playername desc, t.team
"""%(szn)


missing_goalies = """
select pid.playerid, pid.playername as ep_playername, ssg.gp
, t.team, ta.team team_abbv, t.season
, px.nhl_playername, px.nhl_team
from teams.season_stats_g ssg
join players.playerid pid
	on ssg.playerid = pid.playerid
join teams.teams t
	on ssg.teamid = t.teamid
join nhl_game_stats.team_abbvs ta
	on t.team = ta.team_name
	and t.season = ta.season
left join nhl_game_stats.playerid_xwalk px
	on pid.playerid = px.playerid
	and ta.team = px.team_abbv
	and t.season = px.season
where t.league = 'NHL' and ssg.gp>0
and t.season = '%s'
order by px.nhl_playername desc, t.team
"""%(szn)


s_to_supp = pd.read_sql_query(missing_players, con=pg_db.engine)
g_to_supp = pd.read_sql_query(missing_goalies, con=pg_db.engine)


s_to_supp[['playerid','ep_playername','nhl_playername', 'team_abbv']].iloc[:20]

g_to_supp[['playerid','ep_playername','nhl_playername', 'team_abbv']].iloc[:20]


"""
In the excel file: '/Users/jdifrisco/Desktop/PuckNight/playerid_xwalk_supp.xlsx'
Manually add the NHL Playername and team to the valid EP PID, Name, Team row
DO NOT COPY-PASTE

s_to_supp with no nhl_playername

Look player up in nhl_game_stats with: 
    select playername, count(game_date)
    from nhl_game_stats.skaters
    where team = 'MTL' and season = '2021-22'
    group by playername
    order by playername

"""

insert_rows.iloc[3].playerid
insert_rows.iloc[3][['playerid', 'ep_playername']]


###########################################
######## DB FILL USING NHL API ############
###########################################


##################
# Endpoint Tests #
##################

endpoints_df = nhl_api.api_endpoints()

i = 8
endpoints_df.description.iloc[i]
url = 'https://statsapi.web.nhl.com'+endpoints_df.endpoint.iloc[i]
response = requests.get(url, params={'format':'json'})
page = response.json()

pid = 8476880
ps_url = 'https://statsapi.web.nhl.com/api/v1/people/'+str(pid)+'/stats?stats='+\
    'gameLog'
response = requests.get(ps_url, params={'format':'json'})
page = response.json()

page['stats'][0].keys()

page['stats'][0]['splits'][10]


##################
##################
##################
##################
##################

endpoints_df = nhl_api.api_endpoints()
nhl_api.api_endpoint_db(endpoints_df)

dimFran = nhl_api.dimFranchise()
nhl_api.franchise_db(dimFran)

dimPlayType = nhl_api.dimPlayType()

### Generate Valid Game IDs ###
db_szn = '2017-18'
gid_dict = nhl_api.game_ids(db_szn[:4])
nhl_api.game_id_db(gid_dict)     
       
# IDs and Info By Season
game_id_info_q = """select 
gid.season, gid.game_type, gi.game_status, count(gi.game_id)
from nhl_api.game_ids gid
left join nhl_api.game_info gi
	on gid.game_id = gi.game_id
group by gid.season, gid.game_type, gi.game_status	
order by season desc, game_type desc
"""
games_db = pd.read_sql_query(game_id_info_q, con=pg_db.engine)

## Historical Scrape - Select a set of IDs with no Info
szn = '2017-18'
gt = 'Regular Season'
games_q = """select *
from nhl_api.game_ids 
where season = '%s' and game_type = '%s'
"""%(szn, gt)
games_l = list(pd.read_sql_query(games_q, con=pg_db.engine).game_id)

### Season Level Teams and Rosters
teams, ros = nhl_api.sznTeamsRosters(szn)
nhl_api.teams_db(teams)
nhl_api.rosters_db(ros)
        
### All Season Game Info
nhl_api.games_refresh(games_l)


missing_players_q = """select distinct player_id
from nhl_api.game_lineup
where player_id not in(
	select player_id from nhl_api.dim_players	
)"""
pids_needed = list(pd.read_sql_query(missing_players_q, con=pg_db.engine).player_id)

len(pids_needed)

for pid in pids_needed:
    try:
        plr_info = nhl_api.player_info(pid)
        nhl_api.player_info_db(plr_info)
    except:
        print('Error in Player Dimension for :' + str(pid))
        continue




    
#### Game by Game Info 
szn = '2021-22'
gt = 'Regular Season'
games_q = """ select ids.*, gi.game_date, gi.game_status
from nhl_api.game_ids ids
left join nhl_api.game_info gi
    on ids.game_id = gi.game_id
where ids.season = '%s' and ids.game_type = '%s'
order by ids.game_id asc
"""%(szn, gt)
game_id_df = pd.read_sql_query(games_q, con=pg_db.engine)  

## LIST OF NULL GAME IDS - No Schedule Info 
null_game_ids = list(game_id_df[game_id_df.game_date.isnull()].game_id)
len(null_game_ids)
### With Game Rescheduling, earlier game_ids can be left behind
### Scrape all Nulls for updated scheduling
szn = '2021-22'
gt = 'Regular Season'
games_q = """ select ids.*, gi.game_date, gi.game_status
from nhl_api.game_ids ids
left join nhl_api.game_info gi
    on ids.game_id = gi.game_id
where ids.season = '%s' and ids.game_type = '%s'
order by ids.game_id asc
"""%(szn, gt)
game_id_df = pd.read_sql_query(games_q, con=pg_db.engine)  

## LIST OF NULL GAME IDS - No Schedule Info 
null_game_ids = list(game_id_df[game_id_df.game_date.isnull()].game_id)
nhl_api.games_refresh(null_game_ids[:])


### NUCLEAR REFRESH
# nhl_api.games_refresh(game_id_df)




##### Check the API request for a game_id
# game_id = null_game_ids[1]
# api_end = 'https://statsapi.web.nhl.com/api/v1/game/'+str(game_id)+'/feed/live'
# response = requests.get(api_end, params={'format':'json'}) 
# feed_page = response.json() #two item dict
# boxscore_api_end = 'https://statsapi.web.nhl.com/api/v1/game/'+str(game_id)+'/boxscore/'
# response = requests.get(boxscore_api_end, params={'format':'json'}) 
# box_page = response.json() #two item dict
# feed_page
# box_page

###########################################
###########################################
###########################################


######## EP TO NHL-API CROSSWALKS ##########

## Name-matching crosswalk
"""
delete from nhl_api.playerid_xwalk

with 
api_roster as
(
select ros.season, ros.team_id, ros.player_id, ros.jersey_num, ros.playername
, dt.team_id, dt.franchise_id
, concat(dt.team_location,' ', dt.team_name) as team_full
from nhl_api.rosters ros
join nhl_api.dim_teams dt
	on ros.season = dt.season
	and ros.team_id = dt.team_id
), 

ep_roster as 
(
select sss.playerid, pid.playername, t.team, t.season, sss.gp
from teams.season_stats_s sss
join teams.teams t
	on sss.teamid = t.teamid
join players.playerid pid
	on sss.playerid = pid.playerid
where t.league = 'NHL'
UNION
select sss.playerid, pid.playername, t.team, t.season, sss.gp
from teams.season_stats_g sss
join teams.teams t
	on sss.teamid = t.teamid
join players.playerid pid
	on sss.playerid = pid.playerid
where t.league = 'NHL'

),

name_join as 
(
select ep.playerid as ep_playerid, ep.playername as ep_playername, ep.team as ep_team--, ep.gp
, api.playername as api_playername, api.player_id
from ep_roster ep
join api_roster api
	on ep.playername = api.playername
	and ep.team = api.team_full
	and ep.season = api.season
order by ep.team, api_playername desc, ep.gp desc
)

-- CREATE TABLE nhl_api.playerid_xwalk AS
insert into nhl_api.playerid_xwalk
select distinct 
ep_playerid, ep_playername,
api_playername, player_id
from name_join
"""


### FIND UNMATCHED PLAYERS AND COPY DATA IN XLSX
"""

select 
sss.playerid, pid.playername, t.team, t.season, sss.gp
, px.nhl_playername, px.nhl_playerid
from teams.season_stats_s sss
join teams.teams t
	on sss.teamid = t.teamid
join players.playerid pid
	on sss.playerid = pid.playerid
left join nhl_api.playerid_xwalk px
	on pid.playerid = px.ep_playerid	
where t.league = 'NHL' and t.season = '2021-22'
UNION
select 
sss.playerid, pid.playername, t.team, t.season, sss.gp
, px.nhl_playername, px.nhl_playerid
from teams.season_stats_g sss
join teams.teams t
	on sss.teamid = t.teamid
join players.playerid pid
	on sss.playerid = pid.playerid
left join nhl_api.playerid_xwalk px
	on pid.playerid = px.ep_playerid	
where t.league = 'NHL' and t.season = '2021-22'

order by nhl_playername desc, team asc, gp desc
"""

### SEARCH FOR PLAYER USING SUBSTRING AND COMPLETE THE XLSX ROW
"""
select dp.playername, dp.player_id,
dl.team_id, gid.season, dt.team_name, count(*) as games
from nhl_api.dim_players dp
join nhl_api.game_lineup dl
	on dp.player_id = dl.player_id
join nhl_api.game_ids gid
	on dl.game_id = gid.game_id
join nhl_api.dim_teams dt
	on dl.team_id = dt.team_id
	and gid.season = dt.season
where dp.playername like 'Isac L%'
group by dp.playername, dp.player_id,
dl.team_id, gid.season, dt.team_name
order by gid.season desc
"""

pid_supp = pd.read_excel('/Users/jdifrisco/Desktop/PuckNight/PN Nice/nhl_api_playerid_xwalk_supp.xlsx')
insert_rows = pid_supp[pid_supp.nhl_playername.notnull()]

check_in = '('
for pid in list(insert_rows.nhl_playerid): 
    check_in += str(pid) + ", "
check_in = check_in[:-3]
check_in += ')'



pid_truncate = """delete from nhl_api.playerid_xwalk
where nhl_playerid in %s
"""%(check_in)

pg_db.df_to_csv(insert_rows)    
connection = psycopg2.connect(user='postgres',
                            password='654yam',
                            host = '127.0.0.1',
                            port = '5432',
                            database = 'pucknight')
cursor = connection.cursor()       

load_query =  """copy nhl_api.playerid_xwalk
        FROM '/users/jdifrisco/desktop/pucknight/py_to_pg/file_load_x.csv' 
         DELIMITER '|' 
         CSV HEADER 
         QUOTE '\"' 
         ESCAPE '''';
         """
try:       
    cursor.execute(pid_truncate)
    cursor.execute(load_query)
    connection.commit()
    print("PlayerID XWalk Successfully Reloaded for " + szn)        

except(Exception,Error) as error:
    print('Error while connecting to PostgreSQL',error)
    
finally:
    if (connection):
        cursor.close()
        connection.close()
        print('PostgreSQL connection is closed')      


###########################################
###########################################
###########################################

for k,v in top50.items():
    print("'"+v+"'"+',')
    
for k,v in top50.items():
    print(v)    

### NHL TOP 50 PLAYERS
top50 = {50: '88391',
49: '186311',
48: '12461',
47: '221667',
46: '201671',
45: '250075', 
44: '245142',
43: '9189',
42: '186310',
41: '32885', 
40: '233030',
39: '11113',
38: '266336',
37: '45596',
36: '142796',
35: '177671',
34: '199655',
33: '9215',
32: '90355',
31: '14371',
30: '9223',
29: '152111',
28: '32806',
27: '75379',
26: '11317',
25: '45261',
24: '91186',
23: '50044',
22: '23856',
21: '223194',
20: '4217',
19: '62204',
18: '4231',
17: '70424',
16: '9079', 
15: '191959',
14: '18590',
13: '12668',
12: '10439',
11: '9326',
10: '199898',
9: '6007',
8: '4230',
7: '77237',
6: '130383',
5: '6146',
4: '24664',
3: '71913',
2: '99204',
1: '183442'}


top50s_df = pd.DataFrame()
for k,v in top50.items():
    pq = """ 
    select * 
    from players.career_stats_s css
    join players.playerid pid
     on css.playerid = pid.playerid
    join teams.teams t
        on css.teamid = t.teamid
    join players.info pi
        on pid.playerid = pi.playerid
    where css.playerid = '%s'
    """ %(v)
    css_s_df = pd.read_sql(pq, con=pg_db.engine)
    css_s_df['NHL_rank']=k
    top50s_df = top50s_df.append(css_s_df)
    
top50g_df = pd.DataFrame()
for k,v in top50.items():
    pq = """ 
    select * 
    from players.career_stats_g csg
    join players.playerid pid
     on csg.playerid = pid.playerid
    join teams.teams t
        on csg.teamid = t.teamid
    join players.info pi
        on pid.playerid = pi.playerid
    where csg.playerid = '%s'
    """ %(v)    
    
    css_g_df = pd.read_sql(pq, con=pg_db.engine)
    css_g_df['NHL_rank']=k
    top50g_df = top50g_df.append(css_g_df)


top50s_df[['playername','playerid','position', 'height','weight','nation',
           'shoots_catches', 'cap_hit','place_of_birth']].drop_duplicates()
top50g_df[['playername','playerid','position', 'height','weight','nation',
           'shoots_catches', 'cap_hit','place_of_birth']].drop_duplicates()

top50s_df[top50s_df['league']=='SM-liiga'].groupby('playername')\
    .agg({'team':'max', 'gp':'sum', 'season':'max'})\
        .sort_values(by='season')
        
top50g_df[top50g_df['league']=='WJC-20'].groupby(['playername','team'])\
    .agg({'gp':'sum', 'ga':'max'})\
        .sort_values(by='gp', ascending=False)

top50_df.groupby('nation')['playername'].nunique()


top50g_df.iloc[1]



sd = '2021-10-01'
ed = '2021-10-20'

nhl_daily_misc = nhl_sel.nhl_daily_stats_misc(sd, ed)
pg_db.nhl_db(nhl_daily_misc, 'skaters_misc')

timeframes = [

]

### ERRORS TO REPULL
#Skaters
['2008-09', '2009-02-16', '2009-03-15', 'regular_season']
['2008-09', '2009-03-16', '2009-04-15', 'regular_season']

#MISC
['2014-15', '2014-09-15', '2014-10-15', 'regular_season']

#PP
['2008-09', '2008-11-16', '2008-12-15', 'regular_season']
['2008-09', '2008-12-16', '2009-01-15', 'regular_season']
['2008-09', '2009-02-16', '2009-03-15', 'regular_season']
['2008-09', '2009-03-16', '2009-04-15', 'regular_season']



for tf in timeframes: 
    try:
        nhl_daily_s = nhl_sel.nhl_daily_stats_skaters(tf[1], tf[2], tf[0], tf[3])
        pg_db.nhl_db(nhl_daily_s, 'skaters')
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL'+str(tf[1]),error)

for tf in timeframes: 
    try:
        nhl_daily_s = nhl_sel.nhl_daily_stats_faceoffs(tf[1], tf[2], tf[0], tf[3])
        pg_db.nhl_db(nhl_daily_s, 'skaters_faceoffs')
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL'+str(tf[1]),error)


for tf in timeframes: 
    try:
        nhl_daily_s = nhl_sel.nhl_daily_stats_misc(tf[1], tf[2], tf[0], tf[3])
        pg_db.nhl_db(nhl_daily_s, 'skaters_misc')
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL'+str(tf[1]),error)

for tf in timeframes: 
    try:
        nhl_daily_s = nhl_sel.nhl_daily_stats_sat(tf[1], tf[2], tf[0], tf[3])
        pg_db.nhl_db(nhl_daily_s, 'skaters_sat')
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL'+str(tf[1]),error)

for tf in timeframes: 
    try:
        nhl_daily_s = nhl_sel.nhl_daily_stats_shottype(tf[1], tf[2], tf[0], tf[3])
        pg_db.nhl_db(nhl_daily_s, 'skaters_shottype')
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL'+str(tf[1]),error)


for tf in timeframes:
    try:
        nhl_daily_g = nhl_sel.nhl_daily_stats_goalies(tf[1], tf[2], tf[0], tf[3])        
        nhl_db(nhl_daily_g, 'goalies')
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL'+str(tf[1]),error)


for tf in timeframes:
    try:
        nhl_daily_pp = nhl_sel.nhl_daily_stats_powerplay(tf[1], tf[2], tf[0], tf[3]) 
        pg_db.nhl_db(nhl_daily_pp, 'skaters_pp')
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL'+str(tf[1]),error)

for tf in timeframes:
    try:
        nhl_daily_pen = nhl_sel.nhl_daily_stats_penalties(tf[1], tf[2], tf[0], tf[3]) 
        pg_db.nhl_db(nhl_daily_pen, 'skaters_penalties')
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL'+str(tf[1]),error)

for tf in timeframes:
    try:       
        nhl_daily_toi = nhl_sel.nhl_daily_stats_toi(tf[1], tf[2], tf[0], tf[3]) 
        pg_db.nhl_db(nhl_daily_toi, 'skaters_toi')
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL'+str(tf[1]),error)

for tf in timeframes:
    try:
        nhl_daily_shootout = nhl_sel.nhl_daily_stats_shootout(tf[1], tf[2], tf[0], tf[3]) 
        pg_db.nhl_db(nhl_daily_shootout, 'skaters_shootout')
    except(Exception,Error) as error:
        print('Error while connecting to PostgreSQL'+str(tf[1]),error)



### TF LISTS

# ['2020-21', '2021-01-15', '2021-02-15', 'regular_season'],
# ['2020-21', '2021-02-16', '2021-03-15', 'regular_season'],
# ['2020-21', '2021-03-16', '2021-04-15', 'regular_season'],
# ['2020-21', '2021-04-16', '2021-05-15', 'regular_season'],
# ['2020-21', '2021-05-16', '2021-06-15', 'regular_season'],
# ['2020-21', '2021-04-01', '2021-08-01', 'playoffs'],
 
# ['2019-20', '2019-09-15', '2019-10-15', 'regular_season'],
# ['2019-20', '2019-10-16', '2019-11-15', 'regular_season'],
# ['2019-20', '2019-11-16', '2019-12-15', 'regular_season'],
# ['2019-20', '2019-12-16', '2020-01-15', 'regular_season'], 
# ['2019-20', '2020-01-16', '2020-02-15', 'regular_season'],
# ['2019-20', '2020-02-16', '2020-03-30', 'regular_season'],
# ['2019-20', '2020-07-01', '2020-10-01', 'playoffs'],

# ['2018-19', '2018-09-15', '2018-10-15', 'regular_season'],
# ['2018-19', '2018-10-16', '2018-11-15', 'regular_season'],
# ['2018-19', '2018-11-16', '2018-12-15', 'regular_season'],
# ['2018-19', '2018-12-16', '2019-01-15', 'regular_season'], 
# ['2018-19', '2019-01-16', '2019-02-15', 'regular_season'],
# ['2018-19', '2019-02-16', '2019-03-15', 'regular_season'],
# ['2018-19', '2019-03-16', '2019-04-15', 'regular_season'],
# ['2018-19', '2019-04-16', '2019-05-15', 'regular_season'], 
# ['2018-19', '2019-04-01', '2019-07-01', 'playoffs'],
 
# ['2017-18', '2017-09-15', '2017-10-15', 'regular_season'],
# ['2017-18', '2017-10-16', '2017-11-15', 'regular_season'],
# ['2017-18', '2017-11-16', '2017-12-15', 'regular_season'],
# ['2017-18', '2017-12-16', '2018-01-15', 'regular_season'], 
# ['2017-18', '2018-01-16', '2018-02-15', 'regular_season'],
# ['2017-18', '2018-02-16', '2018-03-15', 'regular_season'],
# ['2017-18', '2018-03-16', '2018-04-15', 'regular_season'],
# ['2017-18', '2018-04-16', '2018-05-15', 'regular_season'], 
# ['2017-18', '2018-04-01', '2018-07-01', 'playoffs'],

# ['2016-17', '2016-09-15', '2016-10-15', 'regular_season'],
# ['2016-17', '2016-10-16', '2016-11-15', 'regular_season'],
# ['2016-17', '2016-11-16', '2016-12-15', 'regular_season'],
# ['2016-17', '2016-12-16', '2017-01-15', 'regular_season'], 
# ['2016-17', '2017-01-16', '2017-02-15', 'regular_season'],
# ['2016-17', '2017-02-16', '2017-03-15', 'regular_season'],
# ['2016-17', '2017-03-16', '2017-04-15', 'regular_season'],
# ['2016-17', '2017-04-16', '2017-05-15', 'regular_season'], 
# ['2016-17', '2017-04-01', '2017-07-01', 'playoffs'],
 
# ['2015-16', '2015-09-15', '2015-10-15', 'regular_season'],
# ['2015-16', '2015-10-16', '2015-11-15', 'regular_season'],
# ['2015-16', '2015-11-16', '2015-12-15', 'regular_season'],
# ['2015-16', '2015-12-16', '2016-01-15', 'regular_season'], 
# ['2015-16', '2016-01-16', '2016-02-15', 'regular_season'],
# ['2015-16', '2016-02-16', '2016-03-15', 'regular_season'],
# ['2015-16', '2016-03-16', '2016-04-15', 'regular_season'],
# ['2015-16', '2016-04-16', '2016-05-15', 'regular_season'], 
# ['2015-16', '2016-04-01', '2016-07-01', 'playoffs'], 

# ['2014-15', '2014-09-15', '2014-10-15', 'regular_season'],
# ['2014-15', '2014-10-16', '2014-11-15', 'regular_season'],
# ['2014-15', '2014-11-16', '2014-12-15', 'regular_season'],
# ['2014-15', '2014-12-16', '2015-01-15', 'regular_season'], 
# ['2014-15', '2015-01-16', '2015-02-15', 'regular_season'],
# ['2014-15', '2015-02-16', '2015-03-15', 'regular_season'],
# ['2014-15', '2015-03-16', '2015-04-15', 'regular_season'],
# ['2014-15', '2015-04-16', '2015-05-15', 'regular_season'], 
# ['2014-15', '2015-04-01', '2015-07-01', 'playoffs'],  

# ['2013-14', '2013-09-15', '2013-10-15', 'regular_season'],
# ['2013-14', '2013-10-16', '2013-11-15', 'regular_season'],
# ['2013-14', '2013-11-16', '2013-12-15', 'regular_season'],
# ['2013-14', '2013-12-16', '2014-01-15', 'regular_season'], 
# ['2013-14', '2014-01-16', '2014-02-15', 'regular_season'],
# ['2013-14', '2014-02-16', '2014-03-15', 'regular_season'],
# ['2013-14', '2014-03-16', '2014-04-15', 'regular_season'],
# ['2013-14', '2014-04-16', '2014-05-15', 'regular_season'], 
# ['2013-14', '2014-04-01', '2014-07-01', 'playoffs'],  

# ['2012-13', '2013-01-01', '2013-02-15', 'regular_season'],
# ['2012-13', '2013-02-16', '2013-03-15', 'regular_season'],
# ['2012-13', '2013-03-16', '2013-04-15', 'regular_season'],
# ['2012-13', '2013-04-16', '2013-05-15', 'regular_season'], 
# ['2012-13', '2013-04-01', '2013-07-01', 'playoffs'],  

# ['2011-12', '2011-09-15', '2011-10-15', 'regular_season'],
# ['2011-12', '2011-10-16', '2011-11-15', 'regular_season'],
# ['2011-12', '2011-11-16', '2011-12-15', 'regular_season'],
# ['2011-12', '2011-12-16', '2012-01-15', 'regular_season'], 
# ['2011-12', '2012-01-16', '2012-02-15', 'regular_season'],
# ['2011-12', '2012-02-16', '2012-03-15', 'regular_season'],
# ['2011-12', '2012-03-16', '2012-04-15', 'regular_season'],
# ['2011-12', '2012-04-16', '2012-05-15', 'regular_season'], 
# ['2011-12', '2012-04-01', '2012-07-01', 'playoffs'],   

# ['2010-11', '2010-09-15', '2010-10-15', 'regular_season'],
# ['2010-11', '2010-10-16', '2010-11-15', 'regular_season'],
# ['2010-11', '2010-11-16', '2010-12-15', 'regular_season'],
# ['2010-11', '2010-12-16', '2011-01-15', 'regular_season'], 
# ['2010-11', '2011-01-16', '2011-02-15', 'regular_season'],
# ['2010-11', '2011-02-16', '2011-03-15', 'regular_season'],
# ['2010-11', '2011-03-16', '2011-04-15', 'regular_season'],
# ['2010-11', '2011-04-16', '2011-05-15', 'regular_season'], 
# ['2010-11', '2011-04-01', '2011-07-01', 'playoffs'],    

# ['2009-10', '2009-09-15', '2009-10-15', 'regular_season'],
# ['2009-10', '2009-10-16', '2009-11-15', 'regular_season'],
# ['2009-10', '2009-11-16', '2009-12-15', 'regular_season'],
# ['2009-10', '2009-12-16', '2010-01-15', 'regular_season'], 
# ['2009-10', '2010-01-16', '2010-02-15', 'regular_season'],
# ['2009-10', '2010-02-16', '2010-03-15', 'regular_season'],
# ['2009-10', '2010-03-16', '2010-04-15', 'regular_season'],
# ['2009-10', '2010-04-16', '2010-05-15', 'regular_season'], 
# ['2009-10', '2010-04-01', '2010-07-01', 'playoffs'],    
 
# ['2008-09', '2008-09-15', '2008-10-15', 'regular_season'],
# ['2008-09', '2008-10-16', '2008-11-15', 'regular_season'],
# ['2008-09', '2008-11-16', '2008-12-15', 'regular_season'],
# ['2008-09', '2008-12-16', '2009-01-15', 'regular_season'], 
# ['2008-09', '2009-01-16', '2009-02-15', 'regular_season'],
# ['2008-09', '2009-02-16', '2009-03-15', 'regular_season'],
# ['2008-09', '2009-03-16', '2009-04-15', 'regular_season'],
# ['2008-09', '2009-04-16', '2009-05-15', 'regular_season'], 
# ['2008-09', '2009-04-01', '2009-07-01', 'playoffs'],    

# ['2007-08', '2009-09-15', '2009-10-15', 'regular_season'],
# ['2007-08', '2009-10-16', '2009-11-15', 'regular_season'],
# ['2007-08', '2009-11-16', '2009-12-15', 'regular_season'],
# ['2007-08', '2009-12-16', '2010-01-15', 'regular_season'], 
# ['2007-08', '2010-01-16', '2010-02-15', 'regular_season'],
# ['2007-08', '2010-02-16', '2010-03-15', 'regular_season'],
# ['2007-08', '2010-03-16', '2010-04-15', 'regular_season'],
# ['2007-08', '2010-04-16', '2010-05-15', 'regular_season'], 
# ['2007-08', '2010-04-01', '2010-07-01', 'playoffs'],    

# ['2006-07', '2009-09-15', '2009-10-15', 'regular_season'],
# ['2006-07', '2009-10-16', '2009-11-15', 'regular_season'],
# ['2006-07', '2009-11-16', '2009-12-15', 'regular_season'],
# ['2006-07', '2009-12-16', '2010-01-15', 'regular_season'], 
# ['2006-07', '2010-01-16', '2010-02-15', 'regular_season'],
# ['2006-07', '2010-02-16', '2010-03-15', 'regular_season'],
# ['2006-07', '2010-03-16', '2010-04-15', 'regular_season'],
# ['2006-07', '2010-04-16', '2010-05-15', 'regular_season'], 
# ['2006-07', '2010-04-01', '2010-07-01', 'playoffs'],    

# ['2005-06', '2009-09-15', '2009-10-15', 'regular_season'],
# ['2005-06', '2009-10-16', '2009-11-15', 'regular_season'],
# ['2005-06', '2009-11-16', '2009-12-15', 'regular_season'],
# ['2005-06', '2009-12-16', '2010-01-15', 'regular_season'], 
# ['2005-06', '2010-01-16', '2010-02-15', 'regular_season'],
# ['2005-06', '2010-02-16', '2010-03-15', 'regular_season'],
# ['2005-06', '2010-03-16', '2010-04-15', 'regular_season'],
# ['2005-06', '2010-04-16', '2010-05-15', 'regular_season'], 
# ['2005-06', '2010-04-01', '2010-07-01', 'playoffs'],    
   
# ['2003-04', '2003-09-15', '2003-10-15', 'regular_season'],
# ['2003-04', '2003-10-16', '2003-11-15', 'regular_season'],
# ['2003-04', '2003-11-16', '2003-12-15', 'regular_season'],
# ['2003-04', '2003-12-16', '2004-01-15', 'regular_season'], 
# ['2003-04', '2004-01-16', '2004-02-15', 'regular_season'],
# ['2003-04', '2004-02-16', '2004-03-15', 'regular_season'],
# ['2003-04', '2004-03-16', '2004-04-15', 'regular_season'],
# ['2003-04', '2004-04-16', '2004-05-15', 'regular_season'], 
# ['2003-04', '2004-04-01', '2004-07-01', 'playoffs'],   

# ['2002-03', '2002-09-15', '2002-10-15', 'regular_season'],
# ['2002-03', '2002-10-16', '2002-11-15', 'regular_season'],
# ['2002-03', '2002-11-16', '2002-12-15', 'regular_season'],
# ['2002-03', '2002-12-16', '2003-01-15', 'regular_season'], 
# ['2002-03', '2002-01-16', '2003-02-15', 'regular_season'],
# ['2002-03', '2003-02-16', '2003-03-15', 'regular_season'],
# ['2002-03', '2003-03-16', '2003-04-15', 'regular_season'],
# ['2002-03', '2003-04-16', '2003-05-15', 'regular_season'], 
# ['2002-03', '2003-04-01', '2003-07-01', 'playoffs'],   

# ['2001-02', '2001-09-15', '2001-10-15', 'regular_season'],
# ['2001-02', '2001-10-16', '2001-11-15', 'regular_season'],
# ['2001-02', '2001-11-16', '2001-12-15', 'regular_season'],
# ['2001-02', '2001-12-16', '2002-01-15', 'regular_season'], 
# ['2001-02', '2002-01-16', '2002-02-15', 'regular_season'],
# ['2001-02', '2002-02-16', '2002-03-15', 'regular_season'],
# ['2001-02', '2002-03-16', '2002-04-15', 'regular_season'],
# ['2001-02', '2002-04-16', '2002-05-15', 'regular_season'], 
# ['2001-02', '2002-04-01', '2002-07-01', 'playoffs'],   







