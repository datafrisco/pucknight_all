#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 19 20:46:26 2021

@author: jdifrisco
"""

from pucknight import ep_soup
from pucknight import nhl_sel
from pucknight import pg_db
from pucknight import pn_queries
import time

import pandas as pd
import numpy as np

import sqlalchemy as db
from sqlalchemy import create_engine, MetaData, Table

# import matplotlib.pyplot as plt
# import seaborn as sns

league_dict = {
      'Pro Tier 1': ['NHL','KHL', 'SHL']
    
    , 'Pro Tier 2': ['AHL','DEL','Liiga','Czech', 'NL', 'Champions HL'] #'AHL',]
                   
    , 'Pro Tier 3': ['HockeyAllsvenskan','Mestis', 'ECHL']
    , 'Pro Tier 4': ['HockeyEttan']
    
    , 'Major Junior': ['WHL', 'OHL', 'QMJHL', 'NCAA','J20 Nationell', 'MHL']
    
    , 'Junior A': ['USHL', 'NAHL','NCDC',
                   'AJHL', 'BCHL','MJAHL', 'SIJHL','SJHL',
                   'CCHL', 'NOJHL', 'OJHL', 
                   'MJAHL','QJHL'
                   
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

### Content Explore

league = 'NHL'
season = '2021-22'
file_date = '2021-10-27'
#########
stand = pn_queries.league_standings(league, season)
stats = pn_queries.league_scoring(league, season)
stand[['div_pos','team','division','gp','points','ppg','goal_diff']]
stats[['team', 'playername','age_curr','gp','g','tp']][:20]#'ppg','position','nation','place_of_birth','shoots_catches', 'playerid']][0:20]




#### Pucknight Recap
#### NHL ONLY
date = '2021-10-26'
results = pn_queries.game_results_by_date('2021-22', date)
fantasy_s = pn_queries.skater_score_by_date('2021-22',date)
fantasy_g = pn_queries.goalie_score_by_date('2021-22',date)

# fantasy_s[:40][['playername', 'team', 'skater_score', 'playerid']]
# fantasy_g[['playername','team','goalie_score','shots_against','saves', 'goals_against']]

### GAMEDAY MATCHUP STUFF

#### Pucknight Preview
t1 = 'Toronto Maple Leafs'
t2 = 'Chicago Blackhawks'
t1_abbv = 'TOR'
t2_abbv = 'CHI'
matchup = t1_abbv+'-'+t2_abbv


t1_stand = stand[stand['team'] == t1].iloc[0]\
    [['team','division', 'div_pos','gp', 'points', 'wins', 'losses','otw', 'otl', 'goals', 'goals_against', 'goal_diff']]
t2_stand = stand[stand['team'] == t2].iloc[0]\
    [['team','division', 'div_pos','gp', 'points', 'wins', 'losses','otw', 'otl', 'goals', 'goals_against', 'goal_diff']]
r1 = pn_queries.roster(t1, league, season)
r1['pucknight_team'] = t1
r2 = pn_queries.roster(t2, league, season)
r2['pucknight_team'] = t2
lineup = r1.append(r2).reset_index()


### Head to Head - NHL Only

h2h_totals_q = """select season, team, opponent, game_type
, count(game_date) as games, sum(win) as wins
, sum(goals_for) goals, sum(goals_against) goals_against
from nhl_game_stats.nhl_games
where team = '%s'
and opponent = '%s'
group by season, team, opponent, game_type
order by season desc, game_type asc
"""%(t2,t1_abbv)
h2h_szn_summ = pd.read_sql_query(h2h_totals_q, con=pg_db.engine)


h2h_games_q = """select team, opponent, game_type
, game_date, home_away
, season, game_type, win, goals_for, goals_against
from nhl_game_stats.nhl_games
where team = '%s'
and opponent = '%s'
order by game_date desc
"""%(t2,t1_abbv)
h2h_games = pd.read_sql_query(h2h_games_q, con=pg_db.engine)


plyr_games = pd.DataFrame()
for plr in r1['playername'].to_list():     
    h2h_player_stats_q = """select playername, team, opponent, season, game_type
    , count(game_date) gp, sum(goals) g, sum(assists) a, sum(points) tp, sum(plus_minus) plus_minus
    , sum(pim) pim, sum(gwg) gwg, sum(otg) otg, sum(shots) shots, avg(toi/60) avg_toi
    from nhl_game_stats.skaters
    where 
    opponent = '%s'
    and playername = '%s'
    group by playername, team, opponent,season, game_type
    order by season desc, game_type asc
    """%(t2_abbv,plr)
    plyr_v_opp = pd.read_sql_query(h2h_player_stats_q, con=pg_db.engine)    
    plyr_games = plyr_games.append(plyr_v_opp)        
    
for plr in r2['playername'].to_list():     
    h2h_player_stats_q = """select playername, team, opponent, season, game_type
    , count(game_date) gp, sum(goals) g, sum(assists) a, sum(points) tp, sum(plus_minus) plus_minus
    , sum(pim) pim, sum(gwg) gwg, sum(otg) otg, sum(shots) shots, avg(toi/60) avg_toi
    from nhl_game_stats.skaters
    where 
    opponent = '%s'
    and playername = '%s'
    group by playername, team, opponent,season, game_type
    order by season desc, game_type asc
    """%(t1_abbv,plr)
    plyr_v_opp = pd.read_sql_query(h2h_player_stats_q, con=pg_db.engine)    
    plyr_games = plyr_games.append(plyr_v_opp)

plyr_games.columns
plyr_games[['playername', 'season', 'team','opponent','game_type', 'gp','tp','gwg']].iloc[100:150]

# NHL STATS END

# r2.columns
r1[['playername', 'gp', 'tp', 'age','playerid']]
r2[['playername', 'gp', 'tp', 'age','playerid']]


lineup[lineup['playerid']=='9326'].iloc[0]
plyr = pn_queries.player_career('9326')
# plyr.columns
plyr[['gp','tp','season','league', 'postseason']]

lineup_careers = pd.DataFrame()
for pid in lineup['playerid'].to_list():
    pc = pn_queries.player_career(pid)
    lineup_careers = lineup_careers.append(pc)    

## DO THE CONTROL-LOOKBACK THING
cte_query = """ with lineups as
(
select t.team as pucknight_team, t.league as pucknight_league, pid.playername, 
pi.height, pi.weight, pi.position, pi.nation, pi.dob, pi.cap_hit, pid.playerid, 
pi.place_of_birth, pi.nhl_rights, pi.drafted,
sss.gp, sss.g g_gaa, sss.tp tp_svprc
from teams.season_stats_s sss
join teams.teams t
	on sss.teamid = t.teamid
join players.playerid pid
	on sss.playerid = pid.playerid	
join players.info pi
	on pid.playerid = pi.playerid
where 
t.team in ('%s', '%s')
and t.league in('%s')
and t.season in ('%s')
UNION
select t.team as pucknight_team, t.league as pucknight_leauge, pid.playername, 
pi.height, pi.weight, pi.position, pi.nation, pi.dob, pi.cap_hit, pid.playerid, 
pi.place_of_birth, pi.nhl_rights, pi.drafted,
ssg.gp gp, ssg.gaa g_ga, ssg.sv_prc tp_svprc
from teams.season_stats_g ssg
join teams.teams t
	on ssg.teamid = t.teamid
join players.playerid pid
	on ssg.playerid = pid.playerid	
join players.info pi
	on pid.playerid = pi.playerid
where 
t.team in ('%s', '%s')
and t.league in('%s')
and t.season in ('%s')
),

careers as
(
select lineups.pucknight_team, lineups.pucknight_league, lineups.playername, 
lineups.position, lineups.nation, lineups.dob, lineups.cap_hit,
lineups.playerid, lineups.place_of_birth, lineups.nhl_rights, lineups.drafted,
lineups.gp pn_gp, lineups.g_gaa pn_tp, lineups.tp_svprc pn_tp_svprc,
	css.gp gp_skater, css.g, css.tp, css.pim, css.captain captain_s,
	csg.gp gp_goalie, csg.gaa, csg.sv_pct, csg.captain captain_g,
	t.team, t.season, t.league, stand.postseason
from lineups
left join players.career_stats_s css
	on lineups.playerid = css.playerid
left join players.career_stats_g csg
	on lineups.playerid = csg.playerid	
join teams.teams t
	on css.teamid = t.teamid
join teams.season_standings stand
	on t.teamid = stand.teamid
order by cap_hit, playername
),

 
awards as
(
select pucknight_team, pucknight_league, playername, award, aw.season, aw.league, careers.playerid
from careers
join teams.award_winners aw
	on careers.playerid = aw.playerid
    and careers.season = aw.season
)
"""%(t1, t2, league, season, t1, t2, league, season)


################################################

#NHL FILE
with pd.ExcelWriter('/users/jdifrisco/desktop/PuckNight/Matchups'+league+file_date+'.xlsx', engine='xlsxwriter') as writer:
    stand.to_excel(writer,sheet_name='Standings')
    stats.iloc[0:100].to_excel(writer,sheet_name='Top Scoring')
    gms.to_excel(writer,sheet_name='Games Last Night')
    fantasy_s.to_excel(writer,sheet_name='Fantasy Skater Scores')
    fantasy_g.to_excel(writer,sheet_name='Fantasy Goalies Scores')
    h2h_szn_summ.to_excel(writer,sheet_name='H2H Summary')
    h2h_games.to_excel(writer,sheet_name='H2H By Season')
    plyr_games.to_excel(writer, sheet_name='Players v Opp')


nation_cnt = """ select 
nation, count(*) player_cnt, count(distinct pucknight_team) pn_teams, sum(gp) gp
from lineups
group by nation
order by gp desc """

nation_cnt_q = cte_query + nation_cnt
nation_cnt = pd.read_sql_query(nation_cnt_q, con=pg_db.engine)

lineup.columns
lineup[lineup['nation']=='Czech Rep.'][['playerid','playername','gp','g','a','tp'
                                    , 'pucknight_team','ppg','pim', 'drafted'#,'drafted'
                                    ,'place_of_birth', 'age']]



################################################

lineups_sel = """ select * from lineups
ORDER BY tp_svprc desc
 """
lineups_q = cte_query + lineups_sel
lineup_stats = pd.read_sql_query(lineups_q, con=pg_db.engine)

lineup_stats.columns
lineup_stats[['pucknight_team','playername','tp_svprc','playerid']].iloc[-20:]
lineup_stats[['pucknight_team','playername','nation','gp', 'tp_svprc']].iloc[0:20]

################################################

award_cnt_select = """ select league, award, count(distinct playername) winners
from awards
group by league, award
order by winners desc"""
awards_q = cte_query + award_cnt_select
aw_cnt = pd.read_sql_query(awards_q, con=pg_db.engine)

aw_cnt[['award','winners']][0:40]

################################################

awards_select = """ select * 
from careers
join teams.award_winners aw
	on careers.playerid = aw.playerid
	and aw.season = careers.season
    and aw.league = careers.league
where aw.award is not null"""
aw_q = cte_query + awards_select
award_winners = pd.read_sql_query(aw_q, con=pg_db.engine)

award_winners[award_winners['award']=='OHL Champion']\
    [['playername', 'pucknight_team', 'team','season']]


################################################

league_alum_sel = """select league, count(distinct playername) alum_cnt 
from careers
group by league 
order by alum_cnt desc"""

league_alum_q = cte_query + league_alum_sel
league_alum = pd.read_sql_query(league_alum_q, con=pg_db.engine)	

league_alum[1:20]

################################################

l2 = 'OHL'
team_alum_sel = """ select 
league, team, count(distinct playername) alum_cnt, sum(gp_goalie) gp_g, sum(gp_skater) gp_s
from careers
where league in ('%s')
group by league, team
order by alum_cnt desc""" %(l2)

team_alum_q = cte_query + team_alum_sel
league_team_alum_cnt = pd.read_sql_query(team_alum_q, con=pg_db.engine)

################################################	
 
player_league_level_lines = """select
pucknight_team, playername, nation, place_of_birth, dob, position, playerid,
league, team, count(season) total_seasons, min(season) first_season, max(season) last_season, sum(gp_skater) gp_s,
sum(g) goals, sum(tp) tp, 
sum(gp_goalie) as gp_g, min(gaa) gaa, max(sv_pct) svp
from careers
where league in ('%s')
group by pucknight_team, playername, nation, place_of_birth, dob, position, playerid,
league, team
order by league --pucknight_team, playername, team""" %(l2)

ll_lines_q = cte_query + player_league_level_lines
ll_summed = pd.read_sql_query(ll_lines_q, con=pg_db.engine)

ll_summed.iloc[0]
ll_summed.columns
ll_summed[ll_summed['team']=='Soo Greyhounds'][['playername','nation','pucknight_team','total_seasons','last_season','gp_s','gp_g']]

################################################

#Players on the same Team    
lineup_careers.groupby('teamid').agg({'playername':'count', 'postseason':'max'})\
    .sort_values('playername', ascending = False)[2:50]   
    
wo = """where ct.teamid = '1618/WJC-20/2007-08'
--and vt.team in ('Vancouver Canucks', 'Chicago Blackhawks')
and vt.season='2021-22' """
lookback = pn_queries.control_lookback(wo)
# lookback.columns
lookback[['playername','tp','v_league','v_team','playerid']]\
    .sort_values(['v_league','v_team'])

################################################


#### REDUNDANT AFTER THE PG QUERIES ABOVE
####

lineup.columns
#['shoots_catches', 'place_of_birth', 'drafted', 'height', 'weight']
lineup.groupby('nation').agg({'playername':'count'})
lineup[lineup['nation']=='Russia'][['playerid'
                                    , 'age','place_of_birth','team','tp']]

lineup.sort_values('tp', ascending=False)[['gp'
                                           , 'tp', 'playerid', 'team'
                                           ,'playername','age']]
lineup_careers.columns
lineup_careers.groupby('league').agg({'playername':'count'})\
    .sort_values('playername', ascending = False)[1:20]

#Players on the same Team    
lineup_careers.groupby('teamid').agg({'playername':'count', 'team':'max'})\
    .sort_values('playername', ascending = False)[2:40]    

# Players from a previous team
lineup_careers[lineup_careers['teamid']=='17576/WCup/2016-17']\
    [['playerid','tp', 'playername','season', 'postseason']]##'szn_age'

# Players from a previous league
lineup_careers[lineup_careers['league']=='WJC-20']\
    [['playername','playerid','tp',##'szn_age','postseason'
      'team','season']]\
        .sort_values('season', ascending = 'False')
# Player-team count for
lineup_careers[lineup_careers['league']=='WJC-20'].groupby('teamid').agg({'playername':'count'})\
    .sort_values('playername', ascending = False)[:20]\
        .merge(lineup_careers[['teamid','team','playerid', 'gp']], on='teamid')

# Player Career
lineup[lineup['playerid']=='45419'].iloc[0]
lineup_careers[lineup_careers['playerid']=='273678']\
    [['gp','tp','league', 'team','postseason']]
    
    
pn_queries.league_standings('OHL', '2013-14')[['team','postseason']]
# r3 = pn_queries.roster('Erie Otters', 'OHL', '2013-14')
# r3[['playername', 'gp', 'tp', 'age','playerid']]

-- -- ALL PLAYERS WHO HAVE WON A SPECIFIED AWARD
-- select distinct playername, pucknight_team, pucknight_league,  
-- awards.season, awards.award, awards.league, awards.playerid,
-- t.team, t.season
-- from awards 
-- join players.career_stats_s css
-- 	on awards.playerid = css.playerid
-- join players.career_stats_s csg
-- 	on awards.playerid = csg.playerid
-- join teams.teams t
-- 	on t.teamid = css.teamid	
-- 	and awards.season = t.season	
-- 	and awards.league = t.league
-- -- where awards.league = 'USHL'
-- where awards.playerid = '604'
-- order by t.season asc

-- -- ALUMNI COUNT PER PN TEAM
-- select pucknight_team, league, count(distinct playername) alum_cnt 
-- from careers
-- where league in ( 'WHL')--, 'OHL', 'QMJHL')
-- group by league, pucknight_team
-- order by pucknight_team desc

-- -- LIST OF PN_TEAMS WITH ALUM-COUNT BY LEAGUE
-- select distinct(c.pucknight_team), count( distinct cp.playername) cnt
-- from careers c
-- left join careers cp
-- 	on c.playerid = cp.playerid
-- 	and cp.league = 'WHL'
-- group by c.pucknight_team
-- ORDER BY cnt desc

-- -- ALUMNI COUNT FOR A SPECIFID LEAGUE/TOURNAMENT, by Current Team and League Team
-- select league, team, count(distinct playername) alum_cnt 
-- from careers
-- where league in ('WHL')--, 'OHL', 'QMJHL', 'NCAA')
-- group by league, team
-- order by league, alum_cnt desc

-- select pucknight_team, playername, pn_gp, pn_tp,
-- team, league, season, gp, tp, postseason,
-- nation, place_of_birth, dob, cap_hit
-- from careers
-- where league ='WJC-20'--in ('QMJHL' , 'OHL', 'WHL', 'NCAA', 'USHL')
-- order by season

-- -- Season-League Alum Count
-- select pucknight_team, playername, gp, team, league, season, postseason
-- nation, place_of_birth, dob, cap_hit
-- from careers
-- where league = 'J18 Elit' 
-- order by season 




##################
js_q = """
with pgs as
(
select games.season, games.game_type, games.team, games.game_date, games.opponent, games.home_away 
, games.win, games.reg_win, games.reg_ot_win, games.so_win
, games.loss, games.otl, games.tie
, case when g1.toi = '60:00:00' or g1.toi = '60:00:00' then '60:00:00' 
	when reg_win = 1 then '60:00:00'
	when so_win = 1 then '65:00:00'
	when tie = '1' then '65:00:00'  
	else
		(g1.toi+g2.toi
		+ coalesce(g1_sub.toi,make_interval(0))
		+ coalesce(g2_sub.toi, make_interval(0)))/2	
	end as game_time
, games.shots team_shots, games.shots_against team_shots_against
, games.goals_for team_goals_for, games.goals_against team_goals_against
, g1.playername g1_goalie, g1.toi g1_toi, g1.saves g1_saves, g1.goals_against g1_ga
, g1_sub.playername g1_sub_goalie, g1_sub.toi g1_sub_toi, g1_sub.saves g1_sub_saves, g1_sub.goals_against g1_sub_ga
, g2.playername g2_goalie, g2.toi g2_toi, g2.saves g2_saves, g2.goals_against g2_ga
, g2_sub.playername g2_sub_goalie, g2_sub.toi g2_sub_toi, g2_sub.saves g2_sub_saves, g2_sub.goals_against g2_sub_ga

from nhl_game_stats.nhl_games games
join nhl_game_stats.goalies g1
	on games.game_date = g1.game_date
	and games.opponent = g1.opponent
	and g1.starts=1
left join nhl_game_stats.goalies g1_sub
	on games.game_date = g1_sub.game_date
	and games.opponent = g1_sub.opponent
	and g1_sub.starts=0	
join nhl_game_stats.goalies g2
	on games.game_date = g2.game_date
	and games.opponent = g2.team
	and g2.starts = 1
left join nhl_game_stats.goalies g2_sub
	on games.game_date = g2_sub.game_date
	and games.opponent = g2_sub.team
	and g2_sub.starts = 0	
where games.win = 1 or games.tie = '1'
-- and games.game_type = 'playoffs' 
order by season, game_date
) , 

gm_times as
(
select 
case when game_time > '60:00:00' then 'ot'
	else 'regulation'
	end 
	as ot_game
, *  
from pgs
)

select game_type, ot_game, season, team, game_date, opponent, game_time/60 as game_time,
team_shots, team_shots_against, team_goals_for, team_goals_against,
g1_goalie, g2_goalie, g1_sub_goalie, g2_sub_goalie
from gm_times
 """

dtg = pd.read_sql(js_q, con=pg_db.engine)
# dtg.iloc[12]

dtg['gt_str'] = dtg['game_time'].apply(lambda x: str(x).split('days ')[1])
dtg['hr'] = dtg['gt_str'].apply(lambda x: float(x.split(':')[0]))
dtg['mi'] = dtg['gt_str'].apply(lambda x: float(x.split(':')[1]))
dtg['s'] = dtg['gt_str'].apply(lambda x: float(x.split(':')[2]))
dtg['gt_mins'] = dtg['hr']*60+dtg['mi']+dtg['s']/60

### DF of the games, game-time, goalies query
dtg.iloc[100]



# Count and Percentage of games that go to Overtime
ot_gm_cnt = dtg.groupby(['season', 'game_type', 'ot_game', ])\
    .agg({'ot_game':'count'})
ot_gm_pct = ot_gm_cnt.groupby(level=[0,1])\
    .apply(lambda x: round(x / float(x.sum()),2))
ot_gm_pct.columns = ['ot_game_pct']  
ot_gm_pct = ot_gm_pct[ot_gm_pct.index\
                      .get_level_values('ot_game').isin(['ot'])]
               
### Viz of Only OT Games
ot_gm_pct.reset_index()[['season','game_type','ot_game_pct']]\
    .plot(kind='bar', x='season',y='ot_game_pct', stacked=True
          ,rot=45, fontsize=5, ylim=(0,.40))   
#Doesn't differentiate Reg and Playoff games

#Playoff Games Only
ot_gm_pct[ot_gm_pct.index.get_level_values('game_type').isin(['playoffs'])]\
    .plot(kind='bar',rot=45, fontsize=5, ylim=(0,.40))    
ot_gm_pct.groupby('game_type').mean()    

###
dtg.iloc[12121]

dtg[dtg['game_type']=='playoffs']\
    [dtg['ot_game']=='ot']\
        .groupby('season').agg({'gt_mins':'mean'}).plot(kind='bar')


dtg[dtg['game_type']=='playoffs'][dtg['ot_game']=='ot']\
    .pivot(index='season', columns='season', values='gt_mins')
            
            
dtg[dtg['game_type']=='playoffs'][dtg['ot_game']=='ot']\
    .groupby('season').agg({'gt_mins':['mean', 'min','max','var','std']})\
        .plot()


#############    
mu = dtg[dtg['game_type']=='playoffs'][dtg['ot_game']=='ot']['gt_mins'].mean()
sigma = dtg[dtg['game_type']=='playoffs'][dtg['ot_game']=='ot']['gt_mins'].std()
# s = np.random.normal(mu, sigma, 1000)
s = np.array(dtg[dtg['game_type']=='playoffs'][dtg['ot_game']=='ot'][dtg['season']=='2017-18'][['gt_mins']])
count, bins, ignored = plt.hist(s, 30, density=True)
plt.plot(bins, 1/(sigma * np.sqrt(2 * np.pi)) *
               np.exp( - (bins - mu)**2 / (2 * sigma**2) ),
         linewidth=2, color='r')
dtg[dtg['game_type']=='playoffs'][dtg['ot_game']=='ot']['gt_mins']\
    .hist(bins=[60,65, 70, 75, 80,85, 90, 95, 100,120, 140, 160])



########################
reg_ot = regs['gt_mins']
ploff_ot = ploffs['gt_mins']


regs.groupby('season')[['gt_mins']].mean().plot(kind='bar', rot=45, 
                                                  fontsize=12, ylim=(60,65))

ploffs.groupby('season')[['gt_mins']].mean().plot(kind='bar', rot=45, 
                                                  fontsize=12, ylim=(60,85))






######
#Control Team to Current Team
"""
SELECT c_sss.gp, c_sss.g, c_sss.a, c_sss.tp, ct.team, ct.league, c_ss.postseason
, pid.playername, pi.dob, pi.drafted
, vt.season, vt.team, vt.league, v_sss.gp, v_sss.g, v_sss.tp, v_ss.postseason, pid.playerid
from teams.season_stats_s c_sss --season stats for the control team
join teams.teams ct --team info for the control team
	on c_sss.teamid = ct.teamid
left join teams.season_stats_s v_sss --stats for the player's variable season
	on c_sss.playerid = v_sss.playerid
left join teams.teams vt -- team info for the players variable season
	on v_sss.teamid = vt.teamid
join teams.season_standings c_ss -- team standings for the control team
	on c_sss.teamid = c_ss.teamid
join teams.season_standings v_ss -- team standings for the player's variable team
	on vt.teamid = v_ss.teamid
join players.playerid pid
	on c_sss.playerid = pid.playerid
join players.info pi
	on pid.playerid = pi.playerid
where 
	ct.league = 'NCAA' 
-- 	and ct.team in ('Fargo Force', 'USNTDP Juniors')
	and ct.season = '2021-22' --in ('2020-21', '2022-23', '2023-24')
	and vt.league='USHL'
	and vt.season in ('2020-21')	
-- 	and s_ss.postseason = 'Champion'
-- ORDER BY c_sss.tp desc
ORDER BY v_sss.tp desc
, ct.team, pid.playername, vt.season desc

"""





##########
t1
t2
league
season

"""
-- SEE ALL PLAYERS IN THE LINEUP TABLE
select * from lineups
where playerid in ('7084', '262144','6017','34212', '24398',
						'604','20065','33189','23185','15753', '6403', '33350')

-- -- COUNT OF LINEUP PLAYERS BY NATION
-- select nation, count(*) player_cnt, sum(gp) gp
-- from lineups
-- group by nation
-- order by gp desc

-- -- CAREER STATS FROM A LIST OF SPECIFIC PLAYERS
-- SELECT * from careers
-- where playerid = '604'
-- -- in ('7084', '262144','6017','34212', '24398',
-- -- 						'604','20065','33189','23185','15753', '6403', '33350')
-- order by pucknight_team, playername, season				 

-- -- SEE ALL AWARDS GIVEN TO PLAYERS IN LINEUP
-- select league, award, count(distinct playername) weiners
-- from awards
-- group by league, award
-- order by weiners desc


-- -- ALL PLAYERS WHO HAVE WON A SPECIFIED AWARD
-- select distinct playername, pucknight_team, pucknight_league,  
-- awards.season, awards.award, awards.league, awards.playerid,
-- t.team, t.season
-- from awards 
-- join players.career_stats_s css
-- 	on awards.playerid = css.playerid
-- join players.career_stats_s csg
-- 	on awards.playerid = csg.playerid
-- join teams.teams t
-- 	on t.teamid = css.teamid	
-- 	and awards.season = t.season	
-- 	and awards.league = t.league
-- -- where awards.league = 'USHL'
-- where awards.playerid = '604'
-- order by t.season asc


-- -- ALUMNI COUNT BY LEAGUE
-- select league, count(distinct playername) alum_cnt 
-- from careers
-- where playerid in ('7084', '262144','6017','34212', '24398',
-- 						'604','20065','33189','23185','15753', '6403', '33350')
-- group by league 
-- order by alum_cnt desc


-- -- ALUMNI COUNT BY PREVIOUS TEAM
-- select league, team, count(distinct playername) alum_cnt , sum(gp_goalie) gp_g, sum(gp_skater) gp_s
-- from careers
-- where league in ('NCAA', 'WHL', 'OHL', 'QMJHL')
-- group by league, team
-- order by alum_cnt desc

-- -- ALUMNI COUNT PER PN TEAM
-- select pucknight_team, league, count(distinct playername) alum_cnt 
-- from careers
-- where league in ( 'WHL')--, 'OHL', 'QMJHL')
-- group by league, pucknight_team
-- order by pucknight_team desc

-- -- LIST OF PN_TEAMS WITH ALUM-COUNT BY LEAGUE
-- select distinct(c.pucknight_team), count( distinct cp.playername) cnt
-- from careers c
-- left join careers cp
-- 	on c.playerid = cp.playerid
-- 	and cp.league = 'WHL'
-- group by c.pucknight_team
-- ORDER BY cnt desc


-- -- CAREER STATS FROM A LIST OF LEAGUES
-- SELECT * from careers
-- where league in ('M-Cup')--, 'OHL', 'QMJHL', 'NCAA', 'M-Cup')
-- order by season


-- -- PLAYER SUMMED LINES FOR THE SPECIFIED LEAGUE
-- select pucknight_team, playername, nation, place_of_birth, dob, position, playerid,
-- league, team, count(season) total_seasons, min(season) first_season, max(season) last_season, sum(gp_skater) gp_s,
-- sum(g) goals, sum(tp) tp, 
-- sum(gp_goalie) as gp_g, min(gaa) gaa, max(sv_pct) svp
-- from careers
-- -- -- where league in ('WHL')---, 'OHL', 'QMJHL', 'NCAA')
-- where playerid = '604'
-- group by pucknight_team, playername, nation, place_of_birth, dob, position, playerid,
-- league, team
-- order by league --pucknight_team, playername, team


-- -- ALUMNI COUNT FOR A SPECIFID LEAGUE/TOURNAMENT, by Current Team and League Team
-- select league, team, count(distinct playername) alum_cnt 
-- from careers
-- where league in ('WHL')--, 'OHL', 'QMJHL', 'NCAA')
-- group by league, team
-- order by league, alum_cnt desc

-- select pucknight_team, playername, pn_gp, pn_tp,
-- team, league, season, gp, tp, postseason,
-- nation, place_of_birth, dob, cap_hit
-- from careers
-- where league ='WJC-20'--in ('QMJHL' , 'OHL', 'WHL', 'NCAA', 'USHL')
-- order by season

-- -- Season-League Alum Count
-- select pucknight_team, playername, gp, team, league, season, postseason
-- nation, place_of_birth, dob, cap_hit
-- from careers
-- where league = 'J18 Elit' 
-- order by season 

""" %(t1, t2, league, season, t1, t2, league, season)
