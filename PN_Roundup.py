#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  8 13:37:16 2021

@author: jdifrisco
"""

from pucknight import pn_queries, ep_soup, pg_db

league_dict = {
      'Pro Tier 1': ['NHL','KHL', 'SHL']
    , 'Pro Tier 2': ['AHL','DEL','Liiga','Czech', 'NL', 'Champions HL'] #'AHL',]               
    , 'Pro Tier 3': ['HockeyAllsvenskan','Mestis', 'ECHL', 'VHL']
    # , 'Pro Tier 4': ['HockeyEttan']
    , 'Major Junior': ['WHL', 'OHL', 'QMJHL'
                       # , 'NCAA',
                       # 'J20 Nationell', 'J20 SuperElit', 'MHL'
                       ]
    , 'Junior A': ['USHL', 'NAHL',
                   'AJHL', 'BCHL','MJAHL', 'SIJHL','SJHL',
                   'CCHL', 'NOJHL', 'OJHL', 'MJAHL','QJHL',
                   'J18 Elit'               
                 # 'RBC Cup',  'U20-Elit','Czech U20'
                 ]
    , 'Junior B': ['NCDC', 'J18 Elit', 'J18 Allsvenskan',  ]
    , 'International': ['WC','Olympics','OGQ', #'WC D1A', 'WC D1B'
                        'U20 WJC', #'U20 WJC D1A', 'U20 WJC D1B',
                        'U18 WJC', 'Hlinka Gretzky Cup', 'WHC-17']
    
    , 'College Tier 2': ['NCAA III', 'ACHA', 'ACHA II', 'ACHA III','USports']
    , 'USHS': ['USDP', 'USHS-Prep', 'USHS-MN', 'USHS-MI', 'USHS-NY','USHS-MA']
    # , '18U'
    
    }




league = 'NCAA'
season = '2021-22'

stand = pn_queries.league_standings(league,season)
stats = pn_queries.league_scoring(league, season)

stand.columns
stats.columns

stand[['div_pos','team','wins','losses','goal_diff','division']]

stats[['team','playername','gp','g','a','nation','age_curr','playerid']]\
    .iloc[12]

home = stand.iloc[43].team
away = stand.iloc[8].team

stand[(stand['team']==home) | (stand['team']==away)]\
    [['team','gp','points','ppg','wins','goals','goals_against','division']].T

matchup_plrs = stats[(stats['team']==home) | (stats['team']==away)]
# matchup_plrs.iloc[12]

plr_car_df = pd.DataFrame()
for p in list(matchup_plrs.playerid):
    plr_row = pn_queries.player_career(p)
    plr_car_df = pd.concat([plr_car_df,plr_row])
plr_car_df = plr_car_df.merge(stats[['playerid','team','gp']], on='playerid',
                              suffixes=['','_cur'])
plr_car_df.iloc[12]

plr_awards = pd.DataFrame()
for p in list(matchup_plrs.playerid):
    plr_row = pn_queries.award_winners(p)
    plr_awards = pd.concat([plr_awards,plr_row])
plr_awards = plr_car_df.merge(stats[['playerid','team']], on='playerid',
                              suffixes=['','_cur'])

# plr_car_df.iloc[12]
#######
#######

plr_info_df.columns

plr_info_df.merge(stats, on='playerid')

## Players Drafted
plr_info_df[plr_info_df.overall_pick.notnull()].merge(stats, on='playerid',
                                                      suffixes=['','_stats'])\
    .groupby(['team','playername']).agg({'draft_year':'max','overall_pick':'max',
                                       'draft_team':'max'})

## Players Committed to NCAA in next year
plr_car_df[(plr_car_df['season']=='2022-23') & (plr_car_df['league']=='NCAA')]\
    .merge(stats, on='playerid', suffixes=['','_stats'])\
        .groupby(['team','season','team_stats','playername'])\
            .agg({'place_of_birth':'max','gp_stats':'max','tp':'max'})

## Player Count by Nation            
plr_car_df[plr_car_df!=league].groupby('nation').agg({'playerid':'count',
                                                      'playerid':'nunique'})\
    .sort_values('playerid',ascending=False).iloc[:15]

## Player Count by Place of Birth                                                      
plr_car_df[plr_car_df!=league].groupby('place_of_birth').agg({'playerid':'count',
                                                              'playerid':'nunique'})\
    .sort_values('playerid',ascending=False).iloc[:15]  
    
## Player Count by Leagues
plr_car_df[plr_car_df!=league].groupby('league').agg({'playerid':'count',
                                                     'playerid':'nunique',
                                                     'szn_age':'mean'})\
    .sort_values('playerid',ascending=False).iloc[:15]

## Player Count by Leagues
plr_car_df[plr_car_df!=league].groupby('teamid').agg({'playerid':'count',
                                                     'playerid':'nunique',
                                                     'postseason':'max',
                                                     'szn_age':'mean'})\
    .sort_values('playerid',ascending=False).iloc[:15]
                                                      
                                                   


plr_car_df.iloc[12]


                                                      