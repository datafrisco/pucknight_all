#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 15:20:30 2022

@author: jdifrisco
"""

from hockey_rink import NHLRink
import matplotlib.pyplot as plt
import pandas as pd

from pucknight import pn_queries
from pucknight import pg_db

# fig, axs = plt.subplots(1,3, sharey=True, figsize=(12,6),
#                         gridspec_kw={'width_ratios':[1,1,1]})


games_df = pn_queries.api_games()
gd = datetime.date(2022, 1, 28)
games_df[games_df.game_date >= gd]\
    [['game_id','home_team','home_score','away_team','away_score']]


game_id = 2021020445
######

home = games_df[games_df['game_id']==game_id].home_team.iloc[0]
away = games_df[games_df['game_id']==game_id].away_team.iloc[0]

events = pn_queries.game_events(game_id)
# events_df.groupby('action').agg({'event':'count'})
# events_df.groupby('event').agg({'x_coord':'count'})

home_events = events[events.team_name == home]
away_events = events[events.team_name == away]
        
h_shots = home_events[(home_events.event=='SHOT')\
                          &(home_events.action=='Shooter')]
h_goals = home_events[(home_events.event=='GOAL')\
                          &(home_events.action=='Scorer')] 
    
a_shots = away_events[(away_events.event=='SHOT')\
                          &(away_events.action=='Shooter')]
a_goals = away_events[(away_events.event=='GOAL')\
                          &(away_events.action=='Scorer')] 
    
h_missed_shots = home_events[home_events.event.isin(['BLOCKED SHOT','MISSED SHOT'])\
                          &(home_events.action=='Shooter')]    
a_missed_shots = away_events[away_events.event.isin(['BLOCKED SHOT','MISSED SHOT'])\
                          &(away_events.action=='Shooter')]    

a_shots.iloc[0]
a_goals.groupby('period').agg({'action':'count'})
        
h1_shots = h_shots[h_shots['period']=='1']
h2_shots = h_shots[h_shots['period']=='2']
h3_shots = h_shots[h_shots['period']=='3']
h4_shots = h_shots[h_shots['period']=='4']

a1_shots = a_shots[a_shots['period']=='1']
a2_shots = a_shots[a_shots['period']=='2']
a3_shots = a_shots[a_shots['period']=='3']
a4_shots = a_shots[a_shots['period']=='4']

h1_goals = h_goals[h_goals['period']=='1']
h2_goals = h_goals[h_goals['period']=='2']
h3_goals = h_goals[h_goals['period']=='3']
h4_goals = h_goals[h_goals['period']=='4']

a1_goals = a_goals[a_goals['period']=='1']
a2_goals = a_goals[a_goals['period']=='2']
a3_goals = a_goals[a_goals['period']=='3']
a4_goals = a_goals[a_goals['period']=='4']   

rink = NHLRink()
ax.clear()
# rink.scatter(-89,0, color='g',marker='D')

# ax = rink.draw()
# rink.scatter(a1_goals['x_coord'], a1_goals['y_coord'], marker='x', color='g')
# rink.scatter(a2_goals['x_coord'], a2_goals['y_coord'], marker='x', color='b')
# rink.scatter(a3_goals['x_coord'], a3_goals['y_coord'], marker='x', color='r')
# rink.scatter(a4_goals['x_coord'], a4_goals['y_coord'], marker='x', color='y')

# ax = rink.draw()
# rink.scatter(h1_goals['x_coord'], h1_goals['y_coord'], marker='x', color='g')
# rink.scatter(h2_goals['x_coord'], h2_goals['y_coord'], marker='x', color='b')
# rink.scatter(h3_goals['x_coord'], h3_goals['y_coord'], marker='x', color='r')
# rink.scatter(h4_goals['x_coord'], h4_goals['y_coord'], marker='x', color='y')

# ax = rink.draw()
rink.scatter(a_goals['x_coord'], a_goals['y_coord'], marker='x', color='g')

## GOAL SCATTER
ax = rink.draw()
rink.scatter(h1_goals['x_coord'], h1_goals['y_coord'], marker='D', color='g')
rink.scatter(-1 * h2_goals['x_coord'], -1 * h2_goals['y_coord'], marker='D', color='b')
rink.scatter(h3_goals['x_coord'], h3_goals['y_coord'], marker='D', color='r')
rink.scatter(-1 * h4_goals['x_coord'], -1 * h4_goals['y_coord'], marker='D', color='y')

rink.scatter(a1_goals['x_coord'], a1_goals['y_coord'], marker='x', color='g')
rink.scatter(-1 * a2_goals['x_coord'], -1 * a2_goals['y_coord'], marker='x', color='b')
rink.scatter(a3_goals['x_coord'], a3_goals['y_coord'], marker='x', color='r')
rink.scatter(-1 * a4_goals['x_coord'], -1 * a4_goals['y_coord'], marker='x', color='y')


## SHOT SCATTER
ax = rink.draw()
rink.scatter(a1_shots['x_coord'], a1_shots['y_coord'], marker='P', color='g')
rink.scatter(-1 * a2_shots['x_coord'], -1 * a2_shots['y_coord'], marker='P', color='b')
rink.scatter(a3_shots['x_coord'], a3_shots['y_coord'], marker='P', color='r')
rink.scatter(-1 * a4_shots['x_coord'], -1 * a4_shots['y_coord'], marker='P', color='y')

rink.scatter(h1_shots['x_coord'], h1_shots['y_coord'], marker='x', color='g')
rink.scatter(-1 * h2_shots['x_coord'], -1 * h2_shots['y_coord'], marker='x', color='b')
rink.scatter(h3_shots['x_coord'], h3_shots['y_coord'], marker='x', color='r')
rink.scatter(-1 * h4_shots['x_coord'], -1 * h4_shots['y_coord'], marker='x', color='y')



home_events.columns
events.groupby(['team_name','action']).agg({'event':'count'}).\
    sort_values(['team_name','event'], ascending=False)
    
    





p_ev_q = """
select 
gid.game_id, gid.season, gid.game_type 
, gi.game_date
, gl.pos_code, gl.jersey_num--, gl.captain, gl.alt_captain
, ge.period, ge.per_time_rem
, plr.playername
, ge.event, plr_ev.action
, plr_t.team_name team

, opp_pe.action opp_action
, opp_plr.playername opp_playername
, opp_t.team_name opp_team
, opp_gl.pos_code opp_pos
--, opp_gl.jersey_num opp_num, opp_gl.captain opp_cpt, opp_gl.alt_captain opp_alt_c
, ge.x_coord, ge.y_coord
, case 
	when gl.team_id = gi.home_team_id then 'home'
	when gl.team_id = gi.away_team_id then 'away'
	else 'na'
	end as plr_home_away
, gi.away_team_id
, gi.home_team_id
from 
( select *
	from nhl_api.player_events pe
	--where player_id = 8474141
    where game_id = %s
 ) as plr_ev
left join nhl_api.player_events opp_pe
	on plr_ev.game_id = opp_pe.game_id
	and plr_ev.event_idx = opp_pe.event_idx
	and plr_ev.action <> opp_pe.action
join nhl_api.game_events ge
	on plr_ev.game_id = ge.game_id
	and plr_ev.event_idx = ge.event_idx
join nhl_api.game_lineup gl
	on plr_ev.game_id = gl.game_id
	and plr_ev.player_id = gl.player_id
left join nhl_api.game_lineup opp_gl
	on opp_pe.game_id = opp_gl.game_id
	and opp_pe.player_id = opp_gl.player_id	
join nhl_api.game_ids gid
	on plr_ev.game_id = gid.game_id
join nhl_api.game_info gi	
	on gid.game_id = gi.game_id
join nhl_api.dim_players plr
	on plr_ev.player_id = plr.player_id
left join nhl_api.dim_players opp_plr
	on opp_pe.player_id = opp_plr.player_id
join nhl_api.dim_teams plr_t
	on gl.team_id = plr_t.team_id
	and gid.season = plr_t.season
left join nhl_api.dim_teams opp_t
	on opp_gl.team_id = opp_t.team_id
	and gid.season = opp_t.season
order by gi.game_date asc, ge.period asc, ge.per_time_rem desc
"""%(game_id)

plr_events_df = pd.read_sql_query(p_ev_q, con=pg_db.engine)

plr_events_df.columns



### SINGLE GAME EVENT PREP
games_df[games_df['game_id']==game_id].iloc[12]

events_df.groupby('action').agg({'event':'count'})
events_df.groupby('event').agg({'x_coord':'count'})


home_team = games_df[games_df['game_id']==game_id]['home_team']
away_team = games_df[games_df['game_id']==game_id]['away_team']

### HOME TEAM ALWAYS DEFENDS THE RIGHT SIDE IN 1ST & 3RD ???
home_events = events_df[events_df.team_name == home_team]
away_events = events_df[events_df.team_name == away_team]

# home_events.iloc[12]

h_shots = home_events[home_events.event.isin(['BLOCKED SHOT','SHOT'])\
                          &(home_events.action=='Shooter')]    
a_shots = away_events[away_events.event.isin(['BLOCKED SHOT','SHOT'])\
                          &(away_events.action=='Shooter')]    
    
h_missed_shots = home_events[(home_events.event=='MISSED_SHOT')\
                          &(home_events.action=='Shooter')]
h_goals = home_events[(home_events.event=='GOAL')\
                          &(home_events.action=='Scorer')] 
a_goals = away_events[(away_events.event=='GOAL')\
                          &(away_events.action=='Scorer')] 

# a_goals.columns

        
h1_shots = h_shots[h_shots['period']=='1']
h2_shots = h_shots[h_shots['period']=='2']
h3_shots = h_shots[h_shots['period']=='3']
h4_shots = h_shots[h_shots['period']=='4']

a1_shots = a_shots[a_shots['period']=='1']
a2_shots = a_shots[a_shots['period']=='2']
a3_shots = a_shots[a_shots['period']=='3']
a4_shots = a_shots[a_shots['period']=='4']

h1_goals = h_goals[h_goals['period']=='1']
h2_goals = h_goals[h_goals['period']=='2']
h3_goals = h_goals[h_goals['period']=='3']
h4_goals = h_goals[h_goals['period']=='4']

a1_goals = a_goals[a_goals['period']=='1']
a2_goals = a_goals[a_goals['period']=='2']
a3_goals = a_goals[a_goals['period']=='3']
a4_goals = a_goals[a_goals['period']=='4']

h_goals[['period','playername','action','x_coord', 'y_coord']]
a_goals[['period','playername','per_time_rem','action','x_coord', 'y_coord']]

rink = NHLRink()
# rink.draw()
ax.clear()
# rink.scatter(-89,0, color='g',marker='D')
ax = rink.draw()
rink.scatter(a1_shots['x_coord'], a1_shots['y_coord'], color='g')
rink.scatter(-1*a2_shots['x_coord'], -1*a2_shots['y_coord'], color='b')
rink.scatter(a3_shots['x_coord'], a3_shots['y_coord'], color='r')

rink.scatter(a1_goals['x_coord'], a1_goals['y_coord'], marker='x', color='g')
rink.scatter(a2_goals['x_coord'], a2_goals['y_coord'], marker='x', color='b')
rink.scatter(a3_goals['x_coord'], a3_goals['y_coord'], marker='x', color='r')



plr_events_df.groupby(['team','action']).agg({'x_coord':'count'})

plr_events_df[plr_events_df['action']=='Hitter']\
    [['playername','period','event','team','opp_playername','opp_action']]

### Player-specific events
plr_events_df.iloc[38]

plr_events_df.groupby(['event', 'action']).agg({'opp_action':'count'})

plr_goals = plr_events_df[(plr_events_df['action']=='Scorer') &\
                          (plr_events_df['opp_action']=='Goalie')]
plr_goals[['season','game_date','period','opp_playername','opp_team']]


p1_home_g = plr_goals[(plr_goals['period']=='1') \
                      & (plr_goals['plr_home_away']=='home')] 
p2_home_g = plr_goals[(plr_goals['period']=='2') \
                      & (plr_goals['plr_home_away']=='home')] 
p3_home_g = plr_goals[(plr_goals['period']=='3') \
                      & (plr_goals['plr_home_away']=='home')] 
p4_home_g = plr_goals[(plr_goals['period']=='4') \
                      & (plr_goals['plr_home_away']=='home')] 

    
p1_away_g = plr_goals[(plr_goals['period']=='1') \
                      & (plr_goals['plr_home_away']=='away')] 
p2_away_g = plr_goals[(plr_goals['period']=='2') \
                      & (plr_goals['plr_home_away']=='away')] 
p3_away_g = plr_goals[(plr_goals['period']=='3') \
                      & (plr_goals['plr_home_away']=='away')] 
p4_away_g = plr_goals[(plr_goals['period']=='4') \
                      & (plr_goals['plr_home_away']=='away')] 

    
ax = rink.draw()

# rink.scatter(p1_home_g['x_coord'], p1_home_g['y_coord'], color='g')
# rink.scatter(p2_home_g['x_coord'], p2_home_g['y_coord'], color='r')
# rink.scatter(p3_home_g['x_coord'], p3_home_g['y_coord'], color='b')
# rink.scatter(-1*p4_home_g['x_coord'], -1*p4_home_g['y_coord'], color='y')
    
# rink.scatter(p1_away_g['x_coord'], p1_away_g['y_coord'], color='b')    
rink.scatter(p2_away_g['x_coord'], p2_away_g['y_coord'], color='b')    
# rink.scatter(p3_away_g['x_coord'], p3_away_g['y_coord'], color='b')    
# rink.scatter(p4_away_g['x_coord'], p4_away_g['y_coord'], color='y')    


p2_away_g[['season','game_date','period','opp_team', 'x_coord', 'y_coord']]




## Who is Terry Ryan? 

tr = pn_queries.player_info('73557')
tr.iloc[0]

tr_car = pn_queries.player_career('73557')

tr_car.columns
tr_car[['gp','g_gaa','team','league', 'season','postseason']]

tr_awards = pn_queries.awards('73557')


