#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  7 21:19:26 2022

@author: jdifrisco
"""

from pucknight import nhl_api, pn_queries
from hockey_rink import NHLRink
import matplotlib.pyplot as plt
import pandas as pd

league = 'NHL'
season = '2021-22'
stats_date = '2021-10-01'
gd1 = datetime.date(2022, 2, 21)
gd2 = datetime.date(2022, 2, 22)
gd_adj = datetime.date(2022, 2, 21)

stand = pn_queries.league_standings(league,season)
stats = pn_queries.league_scoring(league, season)
goalies = pn_queries.league_goalies(league, season)

fantasy_s = pn_queries.skater_score_by_date(stats_date)
fantasy_g = pn_queries.goalie_score_by_date(stats_date)

fs_totals = fantasy_s.groupby(['ep_playerid','playername'])\
    .agg({'nhl_playerid':'max','skater_score':['mean','sum'],'game_date':'count'}).reset_index()
fs_totals.iloc[12]    
fs_totals.columns = ['ep_playerid','playername','nhl_playerid','ss_mean','ss_tot','gp']
fg_totals = fantasy_g.groupby(['ep_playerid','playername'])\
    .agg({'nhl_playerid':'max','goalie_score':['mean','sum'],'game_date':'count'}).reset_index()
fg_totals.columns = ['ep_playerid','playername','nhl_playerid','gs_mean','gs_tot','gp']

games_df = pn_queries.api_games(season=szn, game_type='Regular Season')

# games_df.columns
### Games Today
## Search Games by Date Range
gms_day = games_df[(games_df.game_date >= gd1) 
          & (games_df.game_date <= gd2)
         ]\
    [['game_id','game_date','home_team','away_team','game_status']]

# ## Refresh all Today's Games
# nhl_api.games_refresh(list(gms_day.game_id))
# games_df = pn_queries.api_games(season=szn, game_type='Regular Season')

# ### IDs of Games to Get ###
# games_ip = games_df[(games_df.game_status != 'Final') 
#                           & (games_df.game_status != 'Scheduled') 
#                           ]\
#     [['game_id','game_date','home_team','away_team','game_status']]
# games_ip

# nhl_api.games_refresh(list(games_ip.game_id))


# games_df[(games_df['home_team']=='Senators')&(games_df['away_team']=='Devils')]\
#     [['game_id','game_date','away_team','game_status']]

gms_d = {}

for gid in list(gms_dday.game_id):
        
    game_id = gid

game_id = 2021020648    

games_df[games_df['game_id']==game_id]\
    [['game_date','home_team','home_score','away_team','away_score','venue','period','game_status']].iloc[0]
home = games_df[games_df['game_id']==game_id].home_team.iloc[0]
away = games_df[games_df['game_id']==game_id].away_team.iloc[0]
lineups = pn_queries.api_lineups(game_id)
fs_lineup = lineups[lineups['pos_abbv']!='G'].merge(fs_totals, how='left', left_on='player_id', right_on='nhl_playerid',
              suffixes=['','_fs'])\
    [['game_id','team_name','jersey_num','playername','pos_abbv','status','player_id','ep_playerid','ss_mean','ss_tot']]
fg_lineup = lineups[lineups['pos_abbv']=='G'].merge(fg_totals, how='left', left_on='player_id', right_on='nhl_playerid',
              suffixes=['','_fs'])\
    [['game_id','team_name','jersey_num','playername','pos_abbv','status','player_id','ep_playerid','gs_mean','gs_tot']]
sl = pn_queries.api_s_lines(game_id)
gl = pn_queries.api_g_lines(game_id)

              
ep_pids=list(lineups[(lineups['status']!='scratch') & \
             (lineups['pos_abbv']!= 'G')].merge(fs_totals[['nhl_playerid','ep_playerid']],
                                            how='left',left_on='player_id',
                                            right_on='nhl_playerid').ep_playerid)
ep_pids_g=list(lineups[(lineups['status']!='scratch') & \
             (lineups['pos_abbv'] == 'G')].merge(fg_totals[['nhl_playerid','ep_playerid']],
                                            how='left',left_on='player_id',
                                            right_on='nhl_playerid').ep_playerid)

plr_info_df = pd.DataFrame()
for p in ep_pids:
    plr_row = pn_queries.player_info(p)
    plr_info_df = pd.concat([plr_info_df,plr_row])
for p in ep_pids_g:
    plr_row = pn_queries.player_info(p)
    plr_info_df = pd.concat([plr_info_df,plr_row])    
fs_lineup_info=fs_lineup.merge(plr_info_df, left_on='ep_playerid'
                               , right_on='playerid',suffixes=['','_info']).drop(columns=['playername_info','playerid'])
fg_lineup_info=fg_lineup.merge(plr_info_df, left_on='ep_playerid'
                               , right_on='playerid',suffixes=['','_info']).drop(columns=['playername_info','playerid'])


plr_car_df = pd.DataFrame()
for p in ep_pids:
    plr_row = pn_queries.player_career(p)
    plr_car_df = pd.concat([plr_car_df,plr_row])
for p in ep_pids_g:
    plr_row = pn_queries.player_career(p)
    plr_car_df = pd.concat([plr_car_df,plr_row])
fs_lineup_car = fs_lineup.merge(plr_car_df, left_on='ep_playerid', right_on='playerid'
                                ,suffixes=['','_car']).drop(columns=['playername_car','playerid'])
fg_lineup_car = fg_lineup.merge(plr_car_df, left_on='ep_playerid', right_on='playerid'
                                ,suffixes=['','_car']).drop(columns=['playername_car','playerid'])

plr_awards = pd.DataFrame()
for p in ep_pids:
    plr_row = pn_queries.awards(p)
    plr_awards = pd.concat([plr_awards,plr_row])
for p in ep_pids_g:
    plr_row = pn_queries.awards(p)
    plr_awards = pd.concat([plr_awards,plr_row])
fs_lineup_awards = fs_lineup.merge(plr_awards, left_on='ep_playerid', right_on='playerid'
                                   , suffixes=['','_aw']).drop(columns=['playerid'])
fg_lineup_awards = fg_lineup.merge(plr_awards, left_on='ep_playerid', right_on='playerid'
                                   , suffixes=['','_aw']).drop(columns=['playerid'])
        
pl_info = fs_lineup_info.append(fg_lineup_info)
pl_car = fs_lineup_car.append(fg_lineup_car)
pl_awards = fs_lineup_awards.append(fg_lineup_awards)


events = pn_queries.game_events(game_id)
# event_types = list(set(events.event))



lineups = pn_queries.api_lineups(game_id)
fs_lineup = lineups[lineups['pos_abbv']!='G'].merge(fs_totals, how='left', left_on='player_id', right_on='nhl_playerid',
              suffixes=['','_fs'])\
    [['game_id','team_name','jersey_num','playername','pos_abbv','status','player_id','ep_playerid','ss_mean','ss_tot']]
fg_lineup = lineups[lineups['pos_abbv']=='G'].merge(fg_totals, how='left', left_on='player_id', right_on='nhl_playerid',
              suffixes=['','_fs'])\
    [['game_id','team_name','jersey_num','playername','pos_abbv','status','player_id','ep_playerid','gs_mean','gs_tot']]
sl = pn_queries.api_s_lines(game_id)
sl['points'] = sl['goals'] + sl['assists']
gl = pn_queries.api_g_lines(game_id)            
events = pn_queries.game_events(game_id)

fo_w = events[(events['event']=='FACEOFF') & (events['action']=='Winner')]\
    .drop(columns=['action','event_type'])
fo_l = events[(events['event']=='FACEOFF') & (events['action']=='Loser')]\
    .drop(columns=['action','event_type'])

fo_events = fo_w.merge(fo_l[['team_name','event_id','event_idx','playername'
                            ,'player_id','pos_code']]
                       , on=['event_id','event_idx']
                       ,suffixes=['_win','_loss'])

## Give n Take
giventake = events[(events['event']=='GIVEAWAY') | (events['event']=='TAKEAWAY')]


## Penalties
pen_tk = events[(events['event']=='PENALTY') & (events['action']=='PenaltyOn')]
pen_dr= events[(events['event']=='PENALTY') & (events['action']=='DrewBy')]

pen_events = pen_tk.merge(pen_dr[['team_name','event_id','event_idx','playername'
                            ,'player_id','action','pos_code']] 
                          , on=['event_id','event_idx']
                          , suffixes=['_pen','_drew'])
## SHOTS ##

    # SOG
shots = events[(events['event']=='SHOT') & (events['action']=='Shooter')]
saves = events[(events['event']=='SHOT') & (events['action']=='Goalie')]

shots= shots.merge(saves, 
                   on=['period','per_time_rem','x_coord','y_coord','home_score','away_score', 'event'],
                   suffixes=['_shot','_goalie'])

    # Blocked Shots
    
    
b_shots = events[(events['event']=='BLOCKED_SHOT') & (events['action']=='Shooter')]
b_blocks = events[(events['event']=='BLOCKED_SHOT') & (events['action']=='Blocker')]

b_shots = b_shots.merge(b_blocks,
                   on=['period','per_time_rem','x_coord','y_coord','home_score','away_score', 'event'],
                   suffixes=['_shot','_block'])

    ## Missed Shots
m_shots = events[(events['event']=='MISSED_SHOT') & (events['action']=='Shooter')]
ms_goalie = events[(events['event']=='MISSED_SHOT') & (events['action']=='Unknown')]

m_shots= m_shots.merge(ms_goalie, 
                   on=['period','per_time_rem','x_coord','y_coord','home_score','away_score', 'event'],
                   suffixes=['_shot','_goalie'])


## Goals
score = events[(events['event']=='GOAL') & (events['action']=='Scorer')]
ass = events[(events['event']=='GOAL') & (events['action']=='Assist')]
goal = events[(events['event']=='GOAL') & (events['action']=='Goalie')]

goals = score.merge(goal,
                    on=['period','per_time_rem','x_coord','y_coord','home_score','away_score', 'event'],
                    suffixes=['_scorer','_goal'])

assists = goals.merge(ass, 
                      on=['period','per_time_rem','x_coord','y_coord','home_score','away_score', 'event'],
                      suffixes = ['_goal','_assist'])

## Hits
hitter = events[(events['event']=='HIT') & (events['action']=='Hitter')]
hittee = events[(events['event']=='HIT') & (events['action']=='Hittee')]

hits = hitter.merge(hittee,
                    on=['period','per_time_rem','x_coord','y_coord','home_score','away_score', 'event'],
                    suffixes=['_hit','_hittee'])

##################
##################
##################

event_types
events.iloc[12]
sl.iloc[12]
gl.iloc[0]
fo_events.iloc[12]
giventake.iloc[12]
pen_events.iloc[2]
shots.iloc[12]
b_shots.iloc[12]
m_shots.iloc[2]
goals.iloc[2]
assists.iloc[2]
hits.iloc[12]

#############
#############

shots.    


#############
#############

h_shots = shots[shots['team_name_shot']==home]
a_shots = shots[shots['team_name_shot']==away]

goals[['period','team_name_scorer']]
h_goals = goals[goals['team_name_scorer']==home]
a_goals = goals[goals['team_name_scorer']==away]

rink = NHLRink()
rink.scatter(h_goals['x_coord'], h_goals['y_coord'], marker='x', color='b')
rink.scatter(a_goals['x_coord'], a_goals['y_coord'], marker='x', color='g')
ax = rink.draw()

### Finding the Home-Away Pattern..
# Home shoots -  1: Left, 2: Right, 3:Left
# Away shoots -  1: Right, 2: Left, 3: Right







### GAME DATA PREP ###
######################
stand[['div_pos','team','wins','losses','goal_diff','division']]
stats[['team','playername','gp','g','a','nation','age_curr','playerid']]\
    .iloc[:12]

fs_lineup[['team_name','playername','pos_abbv','ss_mean','ss_tot']]\
    .sort_values('ss_tot', ascending=False)
fg_lineup[['team_name','playername','pos_abbv','gs_mean','gs_tot']]\
    .sort_values('gs_tot', ascending=False)
# sl.columns
sl[['team_name','playername','jersey_num','pos_abbv','toi','goals','assists']]
# gl.columns
gl[['team_name', 'playername','jersey_num','pos_abbv','toi','shots','saves']]


## LINEUP PLAYERS BY NATION
pl_info.groupby('nation').agg({'ss_mean':'sum','ss_tot':'sum'
                                       ,'team_name':'count','team_name':'nunique'
                                       ,'player_id':'count'})\
    .sort_values('ss_mean', ascending=False)

## LINEUP PLAYERS BY DRAFT YEAR
pl_info.groupby('draft_year').agg({'ss_mean':'sum','ss_tot':'sum'
                                       ,'team_name':'count','team_name':'nunique'
                                       ,'player_id':'count'})\
    .sort_values('ss_mean', ascending=False)

## LINEUP PLAYERS BY DRAFT ROUND
pl_info.groupby('draft_round').agg({'ss_mean':'sum','ss_tot':'sum'
                                       ,'team_name':'count','team_name':'nunique'
                                       ,'player_id':'count'})\
    .sort_values('ss_mean', ascending=False)

## LINEUP PLAYERS BY BIRTH PLACE
pl_info.groupby('place_of_birth').agg({'ss_mean':'sum','ss_tot':'sum'
                                       ,'team_name':'count','team_name':'nunique'
                                       ,'player_id':'count'})\
    .sort_values('player_id', ascending=False).iloc[:10]


pl_car[pl_car!='NHL'].groupby('league').agg({'ss_mean':'sum','ss_tot':'sum'
                                       ,'team_name':'count','team_name':'nunique'
                                       ,'player_id':'count','player_id':'nunique'})\
    .sort_values(['player_id','ss_mean'], ascending=[False,False]).iloc[:20]
    
pl_car[pl_car['league']!='NHL'].groupby('team').agg({'ss_mean':'sum','ss_tot':'sum'
                                       ,'team_name':'count','team_name':'nunique'
                                       ,'player_id':'count','player_id':'nunique'})\
    .sort_values(['player_id','ss_mean'], ascending=[False,False]).iloc[:12]    

## Alumni Fantasy Faceoff
t1 = 'WHL All-Stars'
t2 = 'OHL All-Stars'
pl_car[(pl_car['team']==t1) | (pl_car['team']==t2)]\
    .groupby(['team','team_name','playername']).agg({'pos_abbv':'max','ss_mean':'mean'})

pl_car[pl_car['league']=='M-Cup']\
    .groupby(['team','team_name','playername','season','postseason']).agg({'pos_abbv':'max','ss_mean':'mean'})

pl_car[(pl_car['league']=='WJC-20') & (pl_car['postseason']=='Champion')]\
    .groupby(['team','team_name','playername','season','postseason']).agg({'pos_abbv':'max','ss_mean':'mean'})

## Teammates
pl_car[pl_car['league']!='NHL'].groupby(['teamid'])\
    .agg({'player_id':'count', 'ss_mean':'mean' })\
        .sort_values('player_id',ascending=False).iloc[:20]


pl_car[pl_car['league']!='NHL'].groupby(['league','season','team','postseason'])\
    .agg({'player_id':'count', 'ss_mean':'mean' })\
        .sort_values('player_id',ascending=False).iloc[:20]


# events.columns
# events[['period','action','event_type']]
# events.groupby(['event','action']).agg({'team_name':'count'})

