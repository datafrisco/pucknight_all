#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  5 09:41:03 2021

@author: jdifrisco
"""
# from pucknight import ep_soup
# from pucknight import nhl_sel
from pucknight import pg_db
# import time

import pandas as pd
# import sqlalchemy as db
# from sqlalchemy import create_engine, MetaData, Table
# import numpy as np



def player_info(pid):
    info_q = """
    select pid.playername, pi.height, pi.weight, pi.shoots_catches
        , pi.position
        , vd.draft_year, vd.draft_round, vd.overall_pick, vd.team as draft_team
        , pi.nation, pi.dob, pi.place_of_birth, pi.youth_team, pi.playerid
    from players.info pi
    join players.playerid pid
    	on pi.playerid = pid.playerid
    left join players."v_NHL_draft" vd
        on pi.playerid = vd.playerid
    where pi.playerid = '%s'
        """%(pid)
        
    pi_df = pd.read_sql_query(info_q, con=pg_db.engine)    
    return(pi_df)

def player_career(pid):
    career_q = """
    select css.gp, css.g g_gaa, css.tp tp_svpct
    , css.post_gp, css.post_g post_g_gaa, css.post_tp post_tp_svpct
    , css.captain
	, t.team, t.league, t.season
	, st.postseason         
    , case when (pi.dob is null or t.season is null) then null else
    round(cast(((DATE_PART('year', concat(left(t.season, 4),'-12-15')::date) - DATE_PART('year', pi.dob::date))*12
    	+ (DATE_PART('month', concat(left(t.season, 4),'-12-15')::date) - DATE_PART('month', pi.dob::date)))/12 as numeric)
    		, 2) 
    		end as szn_age		
    , pid.playername, pi.nation, pi.place_of_birth, pi.dob, pi.position, pi.height, pi.weight
    , vd.draft_year, vd.draft_round, vd.overall_pick, vd.team as draft_team    
    , pid.playerid, t.org_id, css.teamid
    from players.career_stats_s css
    join players.playerid pid
    	on css.playerid = pid.playerid
    join players.info pi
    	on css.playerid = pi.playerid 
    left join teams.teams t
    	on css.teamid = t.teamid
    left join teams.season_standings st
        on t.teamid = st.teamid
    left join teams.award_winners aw
        on css.playerid = aw.playerid
        and t.season = aw.season
        and t.league = aw.league
    left join players."v_NHL_draft" vd
        on pi.playerid = vd.playerid
    where pid.playerid = '%s'
    
    UNION
    
    select csg.gp, csg.gaa g_gaa, csg.sv_pct tp_svpct
    , csg.post_gp, csg.post_gaa post_g_gaa, csg.post_sv_pct post_tp_svpct
    , csg.captain
	, t.team, t.league, t.season
	, st.postseason
    , case when (pi.dob is null or t.season is null) then null else
    round(cast(((DATE_PART('year', concat(left(t.season, 4),'-12-15')::date) - DATE_PART('year', pi.dob::date))*12
    	+ (DATE_PART('month', concat(left(t.season, 4),'-12-15')::date) - DATE_PART('month', pi.dob::date)))/12 as numeric)
    		, 2) 
    		end as szn_age	
	, pid.playername, pi.nation, pi.place_of_birth, pi.dob, pi.position, pi.height, pi.weight 
    , vd.draft_year, vd.draft_round, vd.overall_pick, vd.team as draft_team
    , pid.playerid, t.org_id, csg.teamid 
    from players.career_stats_g csg
    join players.playerid pid
    	on csg.playerid = pid.playerid
    join players.info pi
    	on csg.playerid = pi.playerid 
    left join teams.teams t
    	on csg.teamid = t.teamid
    left join teams.season_standings st
        on t.teamid = st.teamid
    left join teams.award_winners aw
        on csg.playerid = aw.playerid
        and t.season = aw.season
        and t.league = aw.league
    left join players."v_NHL_draft" vd
        on pi.playerid = vd.playerid        
    where pid.playerid = '%s'
	
	order by season asc

    
    """ %(pid, pid)
    
    pc_df = pd.read_sql_query(career_q, con=pg_db.engine)

    return pc_df


def player_summed_career(pid):

    sq_q = """
    select  pid.playername, t.league, pid.playerid
    , count(distinct t.season) as seasons
    , max(t.team) team, count( distinct t.team) teams, min(t.season) minsea, max(t.season) maxsea
    , age( cast(concat(left(min(t.season), 4),'-12-15') as date), max(pi.dob)) as minsea_age
    , age( cast(concat(left(max(t.season), 4),'-12-15') as date), max(pi.dob)) as maxsea_age
    , sum(sss.gp) gp, 
    sum(sss.g) g_gaa, sum(sss.a) a, sum(sss.tp) tp_svpct, sum(sss.pim) pim
    from teams.season_stats_s sss
    join teams.teams t
    	on sss.teamid = t.teamid
    left join teams.season_standings ss
    	on t.teamid = ss.teamid
    join players.playerid pid
    	on sss.playerid = pid.playerid
    join players.info pi
    	on pid.playerid = pi.playerid
    where sss.playerid = '%s'
    group by pid.playername, t.league, pid.playerid
    
    UNION
    
    select  pid.playername, t.league, pid.playerid
    , count(distinct t.season) as seasons
    , max(t.team) team, count( distinct t.team) teams, min(t.season) minsea, max(t.season) maxsea
    , age( cast(concat(left(min(t.season), 4),'-12-15') as date), max(pi.dob)) as minsea_age
    , age( cast(concat(left(max(t.season), 4),'-12-15') as date), max(pi.dob)) as maxsea_age
    , sum(ssg.gp) gp, 
    sum(ssg.gaa) g_gaa, 0 a, sum(ssg.sv_prc) tp_svpct, 0 pim
    from teams.season_stats_g ssg
    join teams.teams t
    	on ssg.teamid = t.teamid
    left join teams.season_standings ss
    	on t.teamid = ss.teamid
    join players.playerid pid
    	on ssg.playerid = pid.playerid
    join players.info pi
    	on pid.playerid = pi.playerid
    where ssg.playerid = '%s'
    group by pid.playername, t.league, pid.playerid
    
    order by minsea asc
    
    """ %(pid, pid)

    psc = pd.read_sql_query(sq_q, con=pg_db.engine)
    
    return psc



def league_standings(lg, szn='2021-22'):
    sq = """
    select ss.div_pos, ss.gp, ss.points, ss.ppg, t.team, ss.wins, ss.otw, ss.losses, ss.otl
    , ss.goals, ss.goals_against, ss.goal_diff
    , ss.division, ss.postseason, ss.league
    from teams.season_standings ss
    join teams.teams t
    	on ss.teamid = t.teamid
    where t.league = '%s'
        and t.season = '%s'
    order by ss.points desc, div_pos asc"""%(lg, szn)
    standings_df = pd.read_sql_query(sq, con=pg_db.engine)
    
    return standings_df


def league_scoring(lg, szn):

    league_scoring_q = """ select 
        t.team, pi.position
        , pid.playername
        , sss.gp, sss.tp
        , sss.g, sss.a, sss.pim
        , sss.plus_minus, sss.ppg 
        , pi.nation, pi.place_of_birth
        , round(cast(EXTRACT(year from age(cast(concat(left(t.season, 4),'-12-15') as date), pi.dob)) +  
		  EXTRACT(month from age(cast(concat(left(t.season, 4),'-12-15') as date), pi.dob))/12 as numeric),1) age_curr
        , pi.dob, pi.shoots_catches, pi.height, pi.weight, pi.contract, pid.playerid
        , vd.draft_year, vd.draft_round, vd.overall_pick, vd.team as draft_team        
        , ss.goals team_goals, ss.goals_against team_ga, ss.goal_diff team_goal_diff, ss.division, ss.div_pos        
        from teams.season_stats_s sss
        join teams.teams t
        	on sss.teamid = t.teamid
        LEFT join teams.season_standings ss
        	on sss.teamid = ss.teamid
        join players.playerid pid
        	on sss.playerid = pid.playerid
        join players.info pi
        	on pid.playerid = pi.playerid
        left join players."v_NHL_draft" vd
            on pi.playerid = vd.playerid             
        where t.league = '%s' and t.season = '%s'
        order by sss.tp desc  """ %(lg, szn)
    
    ls_df = pd.read_sql_query(league_scoring_q, con = pg_db.engine)
        
    return ls_df


def league_goalies(lg, szn):

    g_stats_q = """ select 
        t.team, pi.position
        , pid.playername
        , ssg.gp, ssg.gaa, ssg.sv_prc 
        , pi.nation, pi.place_of_birth
        , round(cast(EXTRACT(year from age(cast(concat(left(t.season, 4),'-10-15') as date), pi.dob)) +  
		  EXTRACT(month from age(cast(concat(left(t.season, 4),'-10-15') as date), pi.dob))/12 as numeric),1) age_curr
        , pi.dob, pi.shoots_catches, pi.height, pi.weight, pi.contract, pid.playerid
        , vd.draft_year, vd.draft_round, vd.overall_pick, vd.team as draft_team                
        , ss.goals team_goals, ss.goals_against team_ga, ss.goal_diff team_goal_diff, ss.division, ss.div_pos
        
        from teams.season_stats_g ssg
        join teams.teams t
        	on ssg.teamid = t.teamid
        LEFT join teams.season_standings ss
        	on ssg.teamid = ss.teamid
        join players.playerid pid
        	on ssg.playerid = pid.playerid
        join players.info pi
        	on pid.playerid = pi.playerid
        left join players."v_NHL_draft" vd
            on pi.playerid = vd.playerid                         
        where t.league = '%s' and t.season = '%s'
        order by ssg.sv_prc""" %(lg, szn)
    
    gs_df = pd.read_sql_query(g_stats_q, con = pg_db.engine)
        
    return gs_df
    
    
def roster(tm, lg, szn):
    roster_q = """
    select sss.gp, sss.g g_gaa, sss.tp tp_svpct, ss.postseason
    , t.team, t.league, t.season
    , pid.playername, pi.position, pi.nation, pi.shoots_catches 
    , round(cast(date_part('year',current_date) - date_part('year',pi.dob) as numeric)
    + cast(date_part('day',current_date) - date_part('day',pi.dob) as numeric)/365 ,2)
    as age
    , pi.place_of_birth, pi.height, pi.weight, pi.playerid
    , vd.draft_year, vd.draft_round, vd.overall_pick, vd.team as draft_team                
    from teams.season_stats_s sss
    join teams.teams t
    	on sss.teamid = t.teamid
    left join teams.season_standings ss
    	on t.teamid = ss.teamid
    left join players.playerid pid
    	on sss.playerid = pid.playerid
    left join players.info pi
    	on pid.playerid = pi.playerid
    left join players."v_NHL_draft" vd
        on pi.playerid = vd.playerid        
    where t.team = '%s'
    	and t.league = '%s'
    	and t.season = '%s'
    
    UNION
    
    select ssg.gp, ssg.gaa g_gaa, ssg.sv_prc tp_svpct, ss.postseason
    , t.team, t.league, t.season
    , pid.playername, pi.position, pi.nation, pi.shoots_catches 
    , round(cast(date_part('year',current_date) - date_part('year',pi.dob) as numeric)
    + cast(date_part('day',current_date) - date_part('day',pi.dob) as numeric)/365 ,2)
    as age
    , pi.place_of_birth, pi.height, pi.weight, pi.playerid
    , vd.draft_year, vd.draft_round, vd.overall_pick, vd.team as draft_team                
    from teams.season_stats_g ssg
    join teams.teams t
    	on ssg.teamid = t.teamid
    left join teams.season_standings ss
    	on t.teamid = ss.teamid
    left join players.playerid pid
    	on ssg.playerid = pid.playerid
    left join players.info pi
    	on pid.playerid = pi.playerid
    left join players."v_NHL_draft" vd
        on pi.playerid = vd.playerid        
    where t.team = '%s'
    	and t.league = '%s'
    	and t.season = '%s'
    """%(tm, lg, szn, tm, lg, szn)
    
    r_df = pd.read_sql_query(roster_q, con=pg_db.engine)
    
    return r_df
        

def awards(pid):
    all_awards = pd.DataFrame()
    awards_q = """ select * 
    from teams.award_winners aw    
    where playerid = '%s'    
    """%(pid)
    p_aw = pd.read_sql_query(awards_q, con=pg_db.engine)
    all_awards = pd.concat([all_awards, p_aw])
        
    return(all_awards)

def award_winners(aw):
    all_awards = pd.DataFrame()
    awards_q = """ select pid.playername, aw.* 
    from teams.award_winners aw
    join players.playerid pid
        on aw.playerid = pid.playerid    
    where award = '%s'
    """%(aw)
    p_aw = pd.read_sql_query(awards_q, con=pg_db.engine)
    all_awards = pd.concat([all_awards, p_aw])
        
    return(all_awards)

    
    
def control_lookback(where_order):
 
    cl_q = """
    SELECT c_sss.gp, c_sss.g g_gaa, c_sss.tp tp_svpct
    , ct.team, ct.league, c_ss.postseason, c_ss.ppg 
    , age(pi.dob) as curr_age
    , pid.playername  
    , pi.position, pi.drafted, pi.nation
    , aw.award
    , age( cast(concat(left(vt.season, 4),'-12-15') as date), pi.dob) as v_age
    , vt.season v_season, vt.team v_team, vt.league v_league
    , v_sss.gp v_gp, v_sss.g v_g_gaa, v_sss.tp v_tp_svpct
    , v_ss.postseason v_postseason, pid.playerid
    from teams.season_stats_s c_sss --season stats for the control team
    join teams.teams ct --team info for the control team
    	on c_sss.teamid = ct.teamid
    left join teams.season_stats_s v_sss --stats for the player's variable season
    	on c_sss.playerid = v_sss.playerid
    left join teams.teams vt -- team info for the players variable season
    	on v_sss.teamid = vt.teamid
    left join teams.season_standings c_ss -- team standings for the control team
    	on c_sss.teamid = c_ss.teamid
    left join teams.season_standings v_ss -- team standings for the player's variable team
    	on vt.teamid = v_ss.teamid
    join players.playerid pid
    	on c_sss.playerid = pid.playerid
    join players.info pi
    	on pid.playerid = pi.playerid
    left join teams.award_winners aw
    	on pid.playerid = aw.playerid
    	and aw.season = vt.season
    	and aw.league = vt.league        
    %s

    UNION
    
    SELECT c_ssg.gp, c_ssg.gaa g_gaa, c_ssg.sv_prc tp_svpct
    , ct.team, ct.league, c_ss.postseason, c_ss.ppg 
    , age(pi.dob) as curr_age
    , pid.playername  
    , pi.position, pi.drafted, pi.nation
    , aw.award
    , age( cast(concat(left(vt.season, 4),'-12-15') as date), pi.dob) as v_age
    , vt.season v_season, vt.team v_team, vt.league v_league
    , v_ssg.gp v_gp, v_ssg.gaa v_g_gaa, v_ssg.sv_prc v_tp_svpct
    , v_ss.postseason v_postseason, pid.playerid
    from teams.season_stats_g c_ssg --season stats for the control team
    join teams.teams ct --team info for the control team
    	on c_ssg.teamid = ct.teamid
    left join teams.season_stats_g v_ssg --stats for the player's variable season
    	on c_ssg.playerid = v_ssg.playerid
    left join teams.teams vt -- team info for the players variable season
    	on v_ssg.teamid = vt.teamid
    left join teams.season_standings c_ss -- team standings for the control team
    	on c_ssg.teamid = c_ss.teamid
    left join teams.season_standings v_ss -- team standings for the player's variable team
    	on vt.teamid = v_ss.teamid
    join players.playerid pid
    	on c_ssg.playerid = pid.playerid
    join players.info pi
    	on pid.playerid = pi.playerid
    left join teams.award_winners aw
    	on pid.playerid = aw.playerid
    	and aw.season = vt.season
    	and aw.league = vt.league 
    %s
    """%(where_order, where_order)         
        
    lookback_df = pd.read_sql_query(cl_q, con=pg_db.engine)    
    
    return lookback_df


def game_results_by_date(season, game_date):
    
    game_results = """ select 
    team, game_date, reg_win, so_win, opponent, goals_for, goals_against
    , shots, shots_against
    , pp_prc, pk_prc, faceoff_win_prc
    from nhl_game_stats.nhl_games 
    where 
    game_date >= '%s'
    and win=1
    """%(game_date)
    gms = pd.read_sql_query(game_results, con=pg_db.engine)
    
    return(gms)


def skater_score_by_date(game_date):    
    
    skater_score = """select 
    pid_xwalk.playerid, s.playername, s.game_date, s.team, s.opponent,
    s.pos as position, s.season, s.game_type,
	
max (
		(cast(extract(hour from toi.ev_toi) + extract(minute from toi.ev_toi/60) as numeric) )*0.1 + 
		(cast(extract(hour from toi.sh_toi) + extract(minute from toi.sh_toi/60) as numeric) )*0.2 + 
		(cast(extract(hour from toi.pp_toi) + extract(minute from toi.pp_toi/60)  as numeric) )*0.075 + 
		(pp.pp_toi_pct*100)*0.0001 + 
		(cast(extract(hour from toi.ot_toi) + extract(minute from toi.ot_toi/60) as numeric) )*0.2 + 
		(cast(extract(hour from s.toi) + extract(minute from s.toi/60) as numeric) )*0 + 
		(toi.shifts)*0 + 
		(s.shots)*3 + 
		(misc.shots_missed_post)*2 + 
		(misc.shots_missed_crossbar)*2 + 
		COALESCE((st.s_slap), 0)*1 + 
		COALESCE((shootout.shootout_attempts), 0)*1 + 
		(misc.shots_missed_wide)*1 + 
		(misc.shots_missed_over)*1 + 
		COALESCE((st.s_def), 0)*0 + 
		COALESCE((st.s_wrist), 0)*0 + 
		COALESCE((st.s_snap), 0)*0 + 
		COALESCE((st.s_back), 0)*0 + 
		COALESCE((st.s_tip), 0)*0 + 
		COALESCE((st.s_wrap), 0)*0 + 
		(pp.pp_shots)*0.5 + 
		(misc.shots_missed)*0 + 
		(sat.sat_relative)*0.1 + 
		(sat.sat_net)*0.3 + 
		(sat.usat_relative)*0.1 + 
		(sat.sat_ahead)*0 + 
		(sat.usat_ahead)*0 + 
		(sat.sat_for)*0 + 
		(sat.usat_for)*0 + 
		(sat.sat_against)*0 + 
		(sat.usat_against)*0 + 
		(sat.sat_behind)*0 + 
		(sat.usat_behind)*0 + 
		(sat.sat_close)*0 + 
		(sat.usat_close)*0 + 
		COALESCE((pp.pp_isat_for), 0)*0 + 
		(sat.usat_net)*0.5 + 
		(pen.penalties_drawn)*2 + 
		(pen.pim)*0 + 
		(pen.penalties_taken)*0 + 
		(pen.major_penalties)*0 + 
		(pen.minor_penalties)*-2 + 
		(pen.misconduct)*-5 + 
		(pen.game_misconduct)*-10 + 
		(misc.hits)*1 + 
		(s.plus_minus)*2 + 
		(misc.blocked_shots)*2 + 
		(misc.takeaways)*2 + 
		(misc.giveaways)*-2 + 
		(s.evg)*10 + 
		(s.gwg)*3 + 
		(s.shg)*13 + 
		(s.ppg)*8 + 
		(misc.otg)*3 + 
		COALESCE((shootout.shootout_goals), 0)*2 + 
		COALESCE((st.g_wrap), 0)*2 + 
		COALESCE((misc.first_goal), 0)*1 + 
		COALESCE((st.g_slap), 0)*1 + 
		COALESCE((shootout.game_deciding_goal), 0)*1 + 
		(misc.eng)*0 + 
		COALESCE((st.g_wrist), 0)*0 + 
		COALESCE((st.g_snap), 0)*0 + 
		COALESCE((st.g_back), 0)*0 + 
		COALESCE((st.g_tip), 0)*0 + 
		COALESCE((st.g_def), 0)*0 + 
		COALESCE((fo.faceoffs), 0)*0 + 
		COALESCE((fo.faceoffs_won), 0)*0.5 + 
		COALESCE((fo.dz_fow), 0)*0.1 + 
		COALESCE((fo.dz_faceoffs), 0)*0 + 
		COALESCE((fo.pp_faceoffs), 0)*0 + 
		COALESCE((fo.oz_fow), 0)*0.1 + 
		COALESCE((fo.pp_fow), 0)*0.1 + 
		COALESCE((fo.sh_faceoffs), 0)*0 + 
		COALESCE((fo.sh_fow), 0)*0.1 + 
		COALESCE((fo.faceoff_win_pct), 0)*0 + 
		COALESCE((fo.ev_faceoffs), 0)*0 + 
		COALESCE((fo.ev_fow), 0)*0 + 
		COALESCE((fo.ev_fol), 0)*-0.1 + 
		COALESCE((fo.nz_faceoffs), 0)*0 + 
		COALESCE((fo.nz_fow), 0)*0 + 
		COALESCE((fo.nz_fol), 0)*-0.1 + 
		COALESCE((fo.oz_faceoffs), 0)*0 + 
		COALESCE((fo.sh_fol), 0)*-0.1 + 
		COALESCE((fo.oz_fol), 0)*-0.1 + 
		COALESCE((fo.pp_fol), 0)*-0.1 + 
		COALESCE((fo.dz_fol), 0)*-0.1 + 
		COALESCE((fo.faceoffs_lost), 0)*0.5 + 
		(s.shp - s.shg)*9 + 
		(s.assists - (s.ppp-s.ppg) - (s.shp - s.shg))*7 + 
		(pp.ppa1)*6 + 
		(pp.ppa2)*5 + 
		(misc.ena)*0   
	) as skater_score,

max(
		(cast(extract(hour from toi.ev_toi) + extract(minute from toi.ev_toi/60) as numeric) )*0.1 + 
		(cast(extract(hour from toi.sh_toi) + extract(minute from toi.sh_toi/60) as numeric) )*0.2 + 
		(cast(extract(hour from toi.pp_toi) + extract(minute from toi.pp_toi/60)  as numeric) )*0.075 + 
		(pp.pp_toi_pct*100)*0.0001 + 
		(cast(extract(hour from toi.ot_toi) + extract(minute from toi.ot_toi/60) as numeric) )*0.2 + 
		(cast(extract(hour from s.toi) + extract(minute from s.toi/60) as numeric) )*0 + 
		(toi.shifts)*0 
	) as toi_pts, 
	
max(
		(s.shots)*3 + 
		(misc.shots_missed_post)*2 + 
		(misc.shots_missed_crossbar)*2 + 
		COALESCE((st.s_slap), 0)*1 + 
		COALESCE((shootout.shootout_attempts), 0)*1 + 
		(misc.shots_missed_wide)*1 + 
		(misc.shots_missed_over)*1 + 
		COALESCE((st.s_def), 0)*0 + 
		COALESCE((st.s_wrist), 0)*0 + 
		COALESCE((st.s_snap), 0)*0 + 
		COALESCE((st.s_back), 0)*0 + 
		COALESCE((st.s_tip), 0)*0 + 
		COALESCE((st.s_wrap), 0)*0 + 
		(pp.pp_shots)*0.5 + 
		(misc.shots_missed)*0
) as shot_points, 

max(
		(sat.sat_relative)*0.1 + 
		(sat.sat_net)*0.3 + 
		(sat.usat_relative)*0.1 + 
		(sat.sat_ahead)*0 + 
		(sat.usat_ahead)*0 + 
		(sat.sat_for)*0 + 
		(sat.usat_for)*0 + 
		(sat.sat_against)*0 + 
		(sat.usat_against)*0 + 
		(sat.sat_behind)*0 + 
		(sat.usat_behind)*0 + 
		(sat.sat_close)*0 + 
		(sat.usat_close)*0 + 
		COALESCE((pp.pp_isat_for), 0)*0 + 
		(sat.usat_net)*0.5
) as sat_points, 

max(
		(s.evg)*10 + 
		(s.gwg)*3 + 
		(s.shg)*13 + 
		(s.ppg)*8 + 
		(misc.otg)*3 + 
		COALESCE((shootout.shootout_goals), 0)*2 + 
		COALESCE((st.g_wrap), 0)*2 + 
		COALESCE((misc.first_goal), 0)*1 + 
		COALESCE((st.g_slap), 0)*1 + 
		COALESCE((shootout.game_deciding_goal), 0)*1 + 
		(misc.eng)*0 + 
		COALESCE((st.g_wrist), 0)*0 + 
		COALESCE((st.g_snap), 0)*0 + 
		COALESCE((st.g_back), 0)*0 + 
		COALESCE((st.g_tip), 0)*0 + 
		COALESCE((st.g_def), 0)*0
) as goal_points, 

max(
		COALESCE((fo.faceoffs), 0)*0 + 
		COALESCE((fo.faceoffs_won), 0)*0.5 + 
		COALESCE((fo.dz_fow), 0)*0.1 + 
		COALESCE((fo.dz_faceoffs), 0)*0 + 
		COALESCE((fo.pp_faceoffs), 0)*0 + 
		COALESCE((fo.oz_fow), 0)*0.1 + 
		COALESCE((fo.pp_fow), 0)*0.1 + 
		COALESCE((fo.sh_faceoffs), 0)*0 + 
		COALESCE((fo.sh_fow), 0)*0.1 + 
		COALESCE((fo.faceoff_win_pct), 0)*0 + 
		COALESCE((fo.ev_faceoffs), 0)*0 + 
		COALESCE((fo.ev_fow), 0)*0 + 
		COALESCE((fo.ev_fol), 0)*-0.1 + 
		COALESCE((fo.nz_faceoffs), 0)*0 + 
		COALESCE((fo.nz_fow), 0)*0 + 
		COALESCE((fo.nz_fol), 0)*-0.1 + 
		COALESCE((fo.oz_faceoffs), 0)*0 + 
		COALESCE((fo.sh_fol), 0)*-0.1 + 
		COALESCE((fo.oz_fol), 0)*-0.1 + 
		COALESCE((fo.pp_fol), 0)*-0.1 + 
		COALESCE((fo.dz_fol), 0)*-0.1 + 
		COALESCE((fo.faceoffs_lost), 0)*0.5
) as fo_points,

max(
		(s.shp - s.shg)*9 + 
		(s.assists - (s.ppp-s.ppg) - (s.shp - s.shg))*7 + 
		(pp.ppa1)*6 + 
		(pp.ppa2)*5 + 
		(misc.ena)*0
) as specTeams_points, 

--TOP LINE
max(s.goals) goals,
max(s.assists) assists,
max(s.plus_minus) plus_minus, 
max(pen.pim) pim , 
max(extract(hour from toi.toi)) mins , 

--GOALS
max(s.evg)  evg, 
max(s.shg) shg , 
max(s.ppg)  ppg, 
max(s.gwg)  gwg,
max(misc.otg) otg , 
max(COALESCE((misc.first_goal), 0)) first_goal , 
max(misc.eng) eng , 

--ASSISTS
max((s.assists) - (s.ppp-s.ppg) - (s.shp - s.shg))  ev_a, 
max(s.shp - s.shg) sh_a , 
max(pp.ppa1 + pp.ppa2) pp_a, 

--SHOTS
max(s.shots)  shots, 
max(pp.pp_shots)  pp_shots, 
max(sat.sat_relative) sat_relative , 
max(sat.usat_net) usat_net , 
max(sat.usat_relative) usat_relative , 
max(sat.sat_net) sat_net , 
max(misc.shots_missed) shots_missed, 
max(misc.shots_missed_crossbar) shots_missed_crossbar, 
max(misc.shots_missed_over) shots_missed_over, 
max(misc.shots_missed_post) shots_missed_post, 
max(misc.shots_missed_wide) shots_missed_wide, 
max(COALESCE((shootout.shootout_attempts), 0)) shootout_attempts, 
max(COALESCE((shootout.shootout_goals), 0)) shootout_goals, 

--OTHER
max(misc.blocked_shots) blocked_shots , 
max(misc.takeaways) takeaways, 
max(pen.penalties_drawn) penalties_drawn , 
max(misc.hits)  hits, 
max(misc.giveaways) giveaways, 
max(pen.misconduct) misconduct , 
max(pen.game_misconduct) game_misconduct,

--TOI
max(extract(hour from toi.ev_toi)) ev_mins , 
max(extract(hour from toi.sh_toi))  sh_mins  , 
max(extract(hour from toi.pp_toi))  pp_mins  , 
max(extract(hour from toi.ot_toi))  ot_mins  , 
max(pp.pp_toi_pct)  pp_toi_pct, 
max(toi.shifts) shifts , 
	
--Faceoffs
max(COALESCE((fo.faceoffs), 0)) faceoffs , 
max(COALESCE((fo.faceoffs_won), 0)) faceoffs_won , 
max(COALESCE((fo.faceoffs_lost), 0)) faceoffs_lost , 
max(COALESCE((fo.faceoff_win_pct), 0))  faceoff_win_pct, 

--Shot Type
max(COALESCE((st.g_slap), 0))  g_slap, 
max(COALESCE((st.g_wrap), 0)) g_wrap, 
max(COALESCE((st.g_wrist), 0)) g_wrist , 
max(COALESCE((st.g_snap), 0))  g_snap, 
max(COALESCE((st.g_back), 0)) g_back, 
max(COALESCE((st.g_tip), 0))  g_tip, 
max(COALESCE((st.g_def), 0))  g_def, 
max(COALESCE((st.s_def), 0)) s_def , 
max(COALESCE((st.s_wrist), 0)) s_wrist , 
max(COALESCE((st.s_snap), 0))  s_snap, 
max(COALESCE((st.s_back), 0))  s_back, 
max(COALESCE((st.s_tip), 0))  s_tip, 
max(COALESCE((st.s_wrap), 0))  s_wrap 			

from nhl_game_stats.skaters s
    left join nhl_game_stats.skaters_misc misc
     	on s.playername = misc.playername
     	and s.team = misc.team
     	and s.game_date = misc.game_date
    left join nhl_game_stats.skaters_sat sat
     	on s.playername = sat.playername
     	and s.game_date = sat.game_date
		and s.opponent = sat.opponent
    left join nhl_game_stats.skaters_shottype st
     	on s.playername = st.playername
     	and s.team = st.team
     	and s.game_date = st.game_date
    left join nhl_game_stats.skaters_faceoffs fo
     	on s.playername = fo.playername
     	and s.team = fo.team
     	and s.game_date = fo.game_date
	left join nhl_game_stats.skaters_pp pp
     	on s.playername = pp.playername
     	and s.team = pp.team
     	and s.game_date = pp.game_date		
	left join nhl_game_stats.skaters_penalties pen
     	on s.playername = pen.playername
     	and s.team = pen.team
     	and s.game_date = pen.game_date	
	left join nhl_game_stats.skaters_shootout shootout
     	on s.playername = shootout.playername
     	and s.team = shootout.team
     	and s.game_date = shootout.game_date	
	left join nhl_game_stats.skaters_toi toi
     	on s.playername = toi.playername
     	and s.team = toi.team
     	and s.game_date = toi.game_date			
    left join nhl_game_stats.playerid_xwalk pid_xwalk
        on s.playername = pid_xwalk.nhl_playername   
        and s.team = pid_xwalk.nhl_team
        and s.season = pid_xwalk.season
    where s.game_date >= '%s'
    group by pid_xwalk.playerid, 
    s.playername, s.game_date, s.team, s.opponent, s.pos, s.season, s.game_type
    order by skater_score desc, game_date desc	
    """%(game_date)
    
    fantasy_s = pd.read_sql_query(skater_score, con=pg_db.engine)

    return (fantasy_s)


def goalie_score_by_date(season, game_date): 
    
    goalie_score = """ 
 
 select g.playername
 , g.game_date
 , g.team
 , g.opponent
 , g.position 
 , g.season 
 , g.game_type
 
, sum(
	g.wins*5 + g.ot_losses*2 + g.sv_pct*10 + g.shots_against*0.1 + 
	g.shutouts*10 + g.saves*0.5 + g.assists*12 + g.goals*50 + g.game_gaa*(-3)
) AS goalie_score
, max(g.starts) starts
, max(g.wins) wins
, max(g.losses) losses
, max(g.ot_losses) ot_losses
, max(g.shots_against) shots_against
, max(g.saves) saves
, max(g.toi/60) toi 
, max(g.goals_against) goals_against
, max(g.sv_pct) sv_pct
, max(g.game_gaa) game_gaa
, max(g.shutouts) shutouts
, max(g.assists) assists
, max(g.goals) goals
, max(g.pim) pim
, max(pid_xwalk.playerid) playerid

from nhl_game_stats.goalies g
left join nhl_game_stats.playerid_xwalk pid_xwalk
        on g.playername = pid_xwalk.nhl_playername   
        and g.team = pid_xwalk.nhl_team
        and g.season = pid_xwalk.season
where g.game_date >= '%s' 
group by pid_xwalk.playerid, g.playername, g.game_date
    , g.team, g.opponent, g.position, g.season, g.game_type
    """%(game_date)
    
    fantasy_g = pd.read_sql_query(goalie_score, con=pg_db.engine)
    return(fantasy_g)


### Season Stats from NHL Game


##############

# for k, l in gameday_dict.items():
#     ############
#     t1 = l[0].replace("'","''")
#     t2 = l[1].replace("'","''")
#     t1_abbv = k[:3]
#     t2_abbv = k[4:]
    
#     #Team Standings
#     t1_stand = full_standings[full_standings['team'] == t1]\
#         [['team','division', 'div_pos','gp', 'points','ppg', 'wins', 'losses','otw', 'otl', 'goals', 'goals_against', 'goal_diff']]
#     t2_stand = full_standings[full_standings['team'] == t2]\
#         [['team','division', 'div_pos','gp', 'points','ppg', 'wins', 'losses','otw', 'otl', 'goals', 'goals_against', 'goal_diff']]
#     x_stand = pd.DataFrame(['x','x','x','x','x','x','x','x','x','x','x','x'])
#     match = pd.concat([t1_stand, t2_stand])      
#     matchup_stand = pd.concat([matchup_stand, match, pd.DataFrame()])
        
#     #Team Rosters
#     r1 = roster(t1, league, season)
#     r1['pucknight_team'] = t1
#     r1['team_abbv'] = t1_abbv
#     r2 = roster(t2, league, season)
#     r2['pucknight_team'] = t2
#     r2['team_abbv'] = t2_abbv
#     lineup = r1.append(r2).reset_index()
#     lineup['game'] = k
#     gameday_roster = pd.concat([gameday_roster, lineup])    
         
#     # Player Careers
#     for pid in lineup['playerid'].to_list():
#         pc = player_career(pid)
#         pc['game'] = k
#         lineup_careers = lineup_careers.append(pc)     
            



            
#         if league == 'NHL': 
                
#             # h2h_totals_q = """select season, team, opponent, game_type
#             # , count(game_date) as games, sum(win) as wins
#             # , sum(goals_for) goals, sum(goals_against) goals_against
#             # from nhl_game_stats.nhl_games
#             # where team = '%s'
#             # and opponent = '%s'
#             # group by season, team, opponent, game_type
#             # order by season desc, game_type asc
#             # """%(t1,t2_abbv)
#             # h2h_szn_summ = pd.read_sql_query(h2h_totals_q, con=pg_db.engine)   
#             # season_h2h = pd.concat([season_h2h,h2h_szn_summ])
            
#             h2h_games_q = """select team, opponent, game_type
#             , game_date, home_away
#             , season, game_type, win, reg_win, goals_for, goals_against, shots, shots_against
#             from nhl_game_stats.nhl_games
#             where team = '%s'
#             and opponent = '%s'
#             order by game_date desc
#             """%(t1,t2_abbv)
#             h2h_all = pd.read_sql_query(h2h_games_q, con=pg_db.engine)  
#             games_h2h = pd.concat([games_h2h,h2h_all])
                
#             # for plr in r1['playername'].to_list():  
#             #     plr = plr.replace("'","''")
#             #     h2h_player_stats_q = """select playername, team, opponent, season, game_type
#             #     , count(game_date) gp, sum(goals) g, sum(assists) a, sum(points) tp, sum(plus_minus) plus_minus
#             #     , sum(pim) pim, sum(gwg) gwg, sum(otg) otg, sum(shots) shots, avg(toi/60) avg_toi
#             #     from nhl_game_stats.skaters
#             #     where 
#             #     opponent = '%s'
#             #     and playername = '%s'
#             #     group by playername, team, opponent,season, game_type
#             #     order by season desc, game_type asc
#             #     """%(t2_abbv,plr)
#             #     plyr_game_lines = pd.read_sql_query(h2h_player_stats_q, con=pg_db.engine)    
#             #     plyr_v_opp = pd.concat([plyr_v_opp, plyr_game_lines])
                
#             # for plr in r2['playername'].to_list(): 
#             #     plr = plr.replace("'","''")            
#             #     h2h_player_stats_q = """select playername, team, opponent, season, game_type
#             #     , count(game_date) gp, sum(goals) g, sum(assists) a, sum(points) tp, sum(plus_minus) plus_minus
#             #     , sum(pim) pim, sum(gwg) gwg, sum(otg) otg, sum(shots) shots, avg(toi/60) avg_toi
#             #     from nhl_game_stats.skaters
#             #     where 
#             #     opponent = '%s'
#             #     and playername = '%s'
#             #     group by playername, team, opponent,season, game_type
#             #     order by season desc, game_type asc
#             #     """%(t1_abbv,plr)
#             #     plyr_game_lines = pd.read_sql_query(h2h_player_stats_q, con=pg_db.engine)    
#             #     plyr_v_opp = pd.concat([plyr_v_opp, plyr_game_lines]) 
            
#             #Player Inividual Game Lines
#             for plr in r1['playername'].to_list():  
#                 plr = plr.replace("'","''")
#                 h2h_player_stats_q = """select playername, team, opponent, season, game_type
#                 , game_date, goals , assists, points, plus_minus
#                 , pim, gwg, otg, shots, toi
#                 from nhl_game_stats.skaters
#                 where 
#                 opponent = '%s'
#                  and playername = '%s'             
#                  order by game_date desc, game_type asc
#                 """%(t2_abbv,plr)
#                 plyr_game_lines = pd.read_sql_query(h2h_player_stats_q, con=pg_db.engine)    
#                 plyr_v_opp = pd.concat([plyr_v_opp, plyr_game_lines])
                
#             for plr in r2['playername'].to_list(): 
#                 plr = plr.replace("'","''")
#                 h2h_player_stats_q = """select playername, team, opponent, season, game_type
#                 , game_date, goals , assists, points, plus_minus
#                 , pim, gwg, otg, shots, toi
#                 from nhl_game_stats.skaters
#                 where 
#                 opponent = '%s'
#                  and playername = '%s'             
#                  order by game_date desc, game_type asc
#                 """%(t1_abbv,plr)
#                 plyr_game_lines = pd.read_sql_query(h2h_player_stats_q, con=pg_db.engine)    
#                 plyr_v_opp = pd.concat([plyr_v_opp, plyr_game_lines])                  
           
#     #### END OF TEST-INDENT LOOP




# ## too many variables

def pn_matchup(gameday_dict,league,season, file_date,stat_ymd,full_standings, full_stats):
    stats_date=stat_ymd
    matchup_stand = pd.DataFrame()  
    gameday_roster = pd.DataFrame()
    lineup_careers = pd.DataFrame()
    season_h2h = pd.DataFrame()
    games_h2h = pd.DataFrame()
    plyr_v_opp = pd.DataFrame()
    
    for k, l in gameday_dict.items():
        ############
        t1 = l[0].replace("'","''")
        t2 = l[1].replace("'","''")
        t1_abbv = k[:3]
        t2_abbv = k[4:]
        
        #Team Standings
        t1_stand = full_standings[full_standings['team'] == t1]\
            [['team','division', 'div_pos','gp', 'points','ppg', 'wins', 'losses','otw', 'otl', 'goals', 'goals_against', 'goal_diff']]
        t2_stand = full_standings[full_standings['team'] == t2]\
            [['team','division', 'div_pos','gp', 'points','ppg', 'wins', 'losses','otw', 'otl', 'goals', 'goals_against', 'goal_diff']]
        x_stand = pd.DataFrame(['x','x','x','x','x','x','x','x','x','x','x','x'])
        match = pd.concat([t1_stand, t2_stand])      
        matchup_stand = pd.concat([matchup_stand, match, pd.DataFrame()])
            
        #Team Rosters
        r1 = roster(t1, league, season)
        r1['pucknight_team'] = t1
        r1['team_abbv'] = t1_abbv
        r2 = roster(t2, league, season)
        r2['pucknight_team'] = t2
        r2['team_abbv'] = t2_abbv
        lineup = r1.append(r2).reset_index()
        lineup['game'] = k
        gameday_roster = pd.concat([gameday_roster, lineup])    
             
        # Player Careers
        for pid in lineup['playerid'].to_list():
            pc = player_career(pid)
            pc['game'] = k
            lineup_careers = lineup_careers.append(pc)     
            
            
        if league == 'NHL': 
                
            # h2h_totals_q = """select season, team, opponent, game_type
            # , count(game_date) as games, sum(win) as wins
            # , sum(goals_for) goals, sum(goals_against) goals_against
            # from nhl_game_stats.nhl_games
            # where team = '%s'
            # and opponent = '%s'
            # group by season, team, opponent, game_type
            # order by season desc, game_type asc
            # """%(t1,t2_abbv)
            # h2h_szn_summ = pd.read_sql_query(h2h_totals_q, con=pg_db.engine)   
            # season_h2h = pd.concat([season_h2h,h2h_szn_summ])
            
            h2h_games_q = """select team, opponent, game_type
            , game_date, home_away
            , season, game_type, win, reg_win, goals_for, goals_against, shots, shots_against
            from nhl_game_stats.nhl_games
            where team = '%s'
            and opponent = '%s'
            order by game_date desc
            """%(t1,t2_abbv)
            h2h_all = pd.read_sql_query(h2h_games_q, con=pg_db.engine)  
            games_h2h = pd.concat([games_h2h,h2h_all])
                
            # for plr in r1['playername'].to_list():  
            #     plr = plr.replace("'","''")
            #     h2h_player_stats_q = """select playername, team, opponent, season, game_type
            #     , count(game_date) gp, sum(goals) g, sum(assists) a, sum(points) tp, sum(plus_minus) plus_minus
            #     , sum(pim) pim, sum(gwg) gwg, sum(otg) otg, sum(shots) shots, avg(toi/60) avg_toi
            #     from nhl_game_stats.skaters
            #     where 
            #     opponent = '%s'
            #     and playername = '%s'
            #     group by playername, team, opponent,season, game_type
            #     order by season desc, game_type asc
            #     """%(t2_abbv,plr)
            #     plyr_game_lines = pd.read_sql_query(h2h_player_stats_q, con=pg_db.engine)    
            #     plyr_v_opp = pd.concat([plyr_v_opp, plyr_game_lines])
                
            # for plr in r2['playername'].to_list(): 
            #     plr = plr.replace("'","''")            
            #     h2h_player_stats_q = """select playername, team, opponent, season, game_type
            #     , count(game_date) gp, sum(goals) g, sum(assists) a, sum(points) tp, sum(plus_minus) plus_minus
            #     , sum(pim) pim, sum(gwg) gwg, sum(otg) otg, sum(shots) shots, avg(toi/60) avg_toi
            #     from nhl_game_stats.skaters
            #     where 
            #     opponent = '%s'
            #     and playername = '%s'
            #     group by playername, team, opponent,season, game_type
            #     order by season desc, game_type asc
            #     """%(t1_abbv,plr)
            #     plyr_game_lines = pd.read_sql_query(h2h_player_stats_q, con=pg_db.engine)    
            #     plyr_v_opp = pd.concat([plyr_v_opp, plyr_game_lines]) 
            
            #Player Inividual Game Lines
            for plr in r1['playername'].to_list():  
                plr = plr.replace("'","''")
                h2h_player_stats_q = """select playername, team, opponent, season, game_type
                , game_date, goals , assists, points, plus_minus
                , pim, gwg, otg, shots, toi
                from nhl_game_stats.skaters
                where 
                opponent = '%s'
                 and playername = '%s'             
                 order by game_date desc, game_type asc
                """%(t2_abbv,plr)
                plyr_game_lines = pd.read_sql_query(h2h_player_stats_q, con=pg_db.engine)    
                plyr_v_opp = pd.concat([plyr_v_opp, plyr_game_lines])
                
            for plr in r2['playername'].to_list(): 
                plr = plr.replace("'","''")
                h2h_player_stats_q = """select playername, team, opponent, season, game_type
                , game_date, goals , assists, points, plus_minus
                , pim, gwg, otg, shots, toi
                from nhl_game_stats.skaters
                where 
                opponent = '%s'
                 and playername = '%s'             
                 order by game_date desc, game_type asc
                """%(t1_abbv,plr)
                plyr_game_lines = pd.read_sql_query(h2h_player_stats_q, con=pg_db.engine)    
                plyr_v_opp = pd.concat([plyr_v_opp, plyr_game_lines])                  
           
    #### END OF TEST-INDENT LOOP
    ###  Using Salary Data - filter out players that are not longer rostered
    
    # if league == 'NHL': 
    #     cf_q = """
    #     select * from players.v_contracts
    #     where season = '2021'
    #     and xwalk_playername is not null
    #     """
    #     sal = pd.read_sql_query(cf_q, con=pg_db.engine)
        
    #     gameday_roster = gameday_roster.merge(sal,
    #                                           left_on=['playername','team_abbv'],
    #                                           right_on=['playername','team'],
    #                                           suffixes=['','_cf'])
        
    #####
    
    lineup_careers = lineup_careers.merge(gameday_roster[['playerid','pucknight_team','gp','tp_svpct']],
                              on=['playerid'],
                              suffixes=['','_szn']) 
    lineup_careers=lineup_careers[lineup_careers['gp_szn']>0]
    
    nations = gameday_roster[gameday_roster['gp']>0].groupby(['nation','game','pucknight_team']).\
        agg({'playername':'count','playername': pd.Series.nunique}).reset_index()
    nations = nations.pivot(index='nation', columns=['game','pucknight_team'])
    
    league_alums = lineup_careers[lineup_careers['league'] != league].\
        groupby(['league','season','game','team','pucknight_team','playername']).\
            agg({'gp':'max','tp_svpct':'max','postseason':'max'
                 #'szn_age':'max'
                 ,'gp_szn':'max', 'tp_svpct_szn':'max'})
            
    team_alum = lineup_careers[lineup_careers['league'] != league].\
        groupby(['league','team','postseason','game','pucknight_team','teamid','season','playername']).\
            agg({'gp':'max','tp_svpct':'max'
                 ,'gp_szn':'max', 'tp_svpct_szn':'max'})
            
    # lineup_careers.iloc[12]        
    
    all_awards = pd.DataFrame()
    for pid in gameday_roster['playerid'].to_list():
        awards_q = """ select * 
        from teams.award_winners aw    
        where playerid = '%s'    
        """%(pid)
        p_aw = pd.read_sql_query(awards_q, con=pg_db.engine)
        all_awards = pd.concat([all_awards, p_aw])
        
    awards = pd.merge(lineup_careers\
                      [['playerid','playername','team','league','season','gp','g_gaa','tp_svpct'
                        ,'szn_age','nation'
                        ,'position','game','pucknight_team','gp_szn','tp_svpct_szn']],
                      all_awards, on=['playerid','league','season']
                      , suffixes=['','_aw'])
        
    award_summ = awards.groupby(['award','league','game','season','pucknight_team','team','playername']).\
        agg({#'szn_age':'max',
             'gp':'max', 'tp_svpct':'max'
             ,'gp_szn':'max','tp_svpct_szn':'max'
             })
        
    with pd.ExcelWriter('/users/jdifrisco/desktop/PuckNight/Matchup_Files/Matchups'+league+file_date+'.xlsx', engine='xlsxwriter') as writer:
        full_standings.to_excel(writer,sheet_name='Standings')
        full_stats.to_excel(writer,sheet_name='Top Scoring')
        if league == 'NHL':
            fantasy_s.to_excel(writer, sheet_name = 'Skater Scores') 
            fantasy_g.to_excel(writer, sheet_name = 'Goalie Scores') 
            season_h2h.to_excel(writer, sheet_name = 'H2H By Season') 
            games_h2h.to_excel(writer, sheet_name = 'H2H Games') 
            plyr_v_opp.to_excel(writer, sheet_name = 'Player v Opp') 
        matchup_stand.T.to_excel(writer, sheet_name='Team Compare')
        nations.to_excel(writer,sheet_name='Nations')
        award_summ.to_excel(writer,sheet_name='Awards')
        league_alums.to_excel(writer,sheet_name='League Alums')
        team_alum.to_excel(writer,sheet_name='Team Alums')
        print('Workbook Complete for ' + league + ': ' + file_date)        
        
    return(matchup_stand, 
    gameday_roster,
    lineup_careers,
    season_h2h,
    games_h2h,
    plyr_v_opp,
    nations,
    league_alums,
    team_alum,
    award_summ)
    
            