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




league = 'OG'
season = '2021-22'

stand = pn_queries.league_standings(league,season)
stats = pn_queries.league_scoring(league, season)
goalies = pn_queries.league_goalies(league, season)

# stand.columns
# stats.columns

stand[['div_pos','team','wins','losses','goal_diff','division']]

stats[['team','playername','gp','g','a','nation','age_curr','playerid']]\
    .iloc[:12]

###############
###############
# mxpts = range(0, max(stand.points))
# st = pd.DataFrame()
# st['points'] = mxpts
# divs = list(set(stand.division))
# st.sort_values('points', ascending=False, inplace=True)
# for d in divs:
#     st[d] = ''
# for d in divs:
#     div_teams = stand[stand['division']==d][['points','team','division']]
#     for pts in list(div_teams.points):
#         for row in div_teams.itertuples():
#             cv = ''
#             if row[1] == pts:
#                 cv += row[2]+' & '
#                 st.loc[pts, row[3]] = cv
#                 st.loc[pts, row[3]] = st.loc[pts, row[3]][:-3]


## To analyze matchup, select two teams
home = stand.iloc[0].team
away = stand.iloc[2].team
matchup_plrs = stats[(stats['team']==home) | (stats['team']==away)]
matchup_gs = goalies[(goalies['team']==home) | (goalies['team']==away)]


stand[(stand['team']==home) | (stand['team']==away)]\
    [['team','gp','points','ppg','wins','goals','goals_against','division','div_pos','teamid']].T



matchup_plrs.iloc[12]
matchup_plrs.sort_values('tp', ascending=False)\
    [['team','playername','gp','tp','g','age_curr','overall_pick']]


matchup_gs.iloc[1]
matchup_gs.sort_values('gp', ascending=False)\
    [['team','playername','gp','gaa','sv_prc','age_curr','overall_pick']]

## To analyze whole league
# matchup_plrs = stats

### BUILD DFs
plr_info_df = pd.DataFrame()
for p in list(matchup_plrs.playerid):
    plr_row = pn_queries.player_info(p)
    plr_info_df = pd.concat([plr_info_df,plr_row])
plr_info_df = plr_info_df.merge(stats[['playerid','team','gp']], on='playerid',
                              suffixes=['','_cur'])

plr_car_df = pd.DataFrame()
for p in list(matchup_plrs.playerid):
    plr_row = pn_queries.player_career(p)
    plr_car_df = pd.concat([plr_car_df,plr_row])
plr_car_df = plr_car_df.merge(stats[['playerid','team','gp']], on='playerid',
                              suffixes=['','_cur'])

plr_awards = pd.DataFrame()
for p in list(matchup_plrs.playerid):
    plr_row = pn_queries.award_winners(p)
    plr_awards = pd.concat([plr_awards,plr_row])
plr_awards = plr_car_df.merge(stats[['playerid','team']], on='playerid',
                              suffixes=['','_cur'])


#######
#######

#Roster Buiild : Selects

# TP = {}
# TP['central'] = ['632451','554472','482273','290056','687298',
#                  '559293','466653','524885','294577','496170',
#                  '494430','293183','587558','526656','290189',
#                  '410901','620353','497186','474298','369924'
#                  ]
# TP['east'] = []
# TP['midwest'] = []
# TP['selects'] = []
# TP['south'] = []


# stand[stand['division']=='East'].team
# tm = stand[stand['division']=='East'].iloc[0].team

# stats[stats['team']==tm][['playerid','playername']]\
#     .sort_values('playername')
# goalies[goalies['team']==tm][['playerid','playername']]\
#     .sort_values('playername')

#######
#######

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
plr_info_df.groupby('nation').agg({'playerid':'count',
                                                      'playerid':'nunique'})\
    .sort_values('playerid',ascending=False).iloc[:15]

## Player Count by Place of Birth                                                      
plr_info_df[plr_info_df!=league].groupby('place_of_birth').agg({'playerid':'count',
                                                              'playerid':'nunique'})\
    .sort_values('playerid',ascending=False).iloc[:15]  
    
## Player Count by Leagues
plr_car_df[plr_car_df!=league].groupby(['league','season']).agg({'playerid':'count',
                                                     'playerid':'nunique',
                                                     'team_cur':'count',
                                                     'team_cur':'nunique',
                                                     'szn_age':'mean'})\
    .sort_values('playerid',ascending=False).iloc[:15]

## Player Count by Teams
plr_car_df[plr_car_df!=league].groupby('teamid').agg({'playerid':'count',
                                                     'playerid':'nunique',
                                                     'postseason':'max',
                                                     'team_cur':'count',
                                                     'team_cur':'nunique',                                                     
                                                     # 'szn_age':'mean'
                                                     })\
    .sort_values('playerid',ascending=False).iloc[:15]
                                                      
                                                   
## Team Explore
cl_stmt = """ where ct.league = 'USHS-MN' and ct.season = '2019-20'
and vt.season = '2021-22' and vt.league = 'USHL'
and vt.team in ('Chicago Steel','Tri-City Storm')
"""
cl_tm = pn_queries.control_lookback(cl_stmt)

# cl_tm.columns

cl_tm[['playername','team','postseason','v_team']]\
    .sort_values('playername').iloc[:50]


pid = '574744'
plr_car_df[plr_car_df['playerid']==pid]\
    [['teamid','season','team','gp','postseason','szn_age','team_cur']]
plr_info_df[plr_info_df['playerid']==pid].T
    


##########
##########

league = 'NHL'
season = '2021-22'

stand = pn_queries.league_standings(league,season)
stats = pn_queries.league_scoring(league, season)
goalies = pn_queries.league_goalies(league, season)

# stats.columns
stand[['div_pos','team','wins','losses','goal_diff','division']]
stats[['team','playername','gp','g','a','nation','age_curr','playerid']]\
    .iloc[:12]

stand.groupby(['division','div_pos','team']).agg({'points':'max','wins':'max',
                                                  'losses':'max','goals':'max',
                                                  'goals_against':'max'})
    
stand.pivot(index='div_pos',columns='division', values='team'])


## To analyze matchup, select two teams
home = stand.iloc[3].team
away = stand.iloc[6].team
matchup_plrs = stats[(stats['team']==home) | (stats['team']==away)]
matchup_gs = goalies[(goalies['team']==home) | (goalies['team']==away)]

stand[(stand['team']==home) | (stand['team']==away)]\
    [['team','gp','points','ppg','wins','goals','goals_against','division','div_pos','teamid']].T

# matchup_plrs.iloc[12]
matchup_plrs.sort_values('tp', ascending=False)\
    [['team','playername','gp','tp','g','age_curr','overall_pick']].iloc[:12]


# matchup_gs.iloc[1]
matchup_gs.sort_values('gp', ascending=False)\
    [['team','playername','gp','gaa','sv_prc','age_curr','overall_pick']].iloc[:6]

## To analyze whole league
# matchup_plrs = stats

### BUILD DFs
plr_info_df = pd.DataFrame()
for p in list(matchup_plrs.playerid):
    plr_row = pn_queries.player_info(p)
    plr_info_df = pd.concat([plr_info_df,plr_row])
plr_info_df = plr_info_df.merge(stats[['playerid','team','gp']], on='playerid',
                              suffixes=['','_cur'])

plr_car_df = pd.DataFrame()
for p in list(matchup_plrs.playerid):
    plr_row = pn_queries.player_career(p)
    plr_car_df = pd.concat([plr_car_df,plr_row])
plr_car_df = plr_car_df.merge(stats[['playerid','team','gp']], on='playerid',
                              suffixes=['','_cur'])

plr_awards = pd.DataFrame()
for p in list(matchup_plrs.playerid):
    plr_row = pn_queries.award_winners(p)
    plr_awards = pd.concat([plr_awards,plr_row])
plr_awards = plr_car_df.merge(stats[['playerid','team']], on='playerid',
                              suffixes=['','_cur'])













                                                      