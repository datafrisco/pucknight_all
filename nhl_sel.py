#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  5 19:11:46 2021

@author: jdifrisco


NHL.com has different stat layouts by Report Type:
    aggregate=0 turns the summing feature off
    gameType = Regular Season: 2, Playoffs:3
    Max return of table for a URL is 10k - each regular season would have 50k entries... depending on teams    
    
Summary:           [done] http://www.nhl.com/stats/skaters?aggregate=0&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=points,goals,assists&page=0&pageSize=50
Game by Game:       [done]
Bios:               http://www.nhl.com/stats/skaters?aggregate=0&report=bios&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=a_skaterFullName&page=0&pageSize=50
Faceoff %:         [only need to get Faceoff Totals] http://www.nhl.com/stats/skaters?aggregate=0&report=faceoffpercentages&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=totalFaceoffs&page=0&pageSize=50
Faceoff Totals:     [done]  http://www.nhl.com/stats/skaters?aggregate=0&report=faceoffwins&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=totalFaceoffWins,faceoffWinPct&page=0&pageSize=50
Goals For & Ag:     http://www.nhl.com/stats/skaters?aggregate=0&report=goalsForAgainst&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=evenStrengthGoalDifference&page=0&pageSize=50
Misc:              [done]  http://www.nhl.com/stats/skaters?aggregate=0&report=realtime&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=hits&page=0&pageSize=50
Penalties:         [done] http://www.nhl.com/stats/skaters?aggregate=0&report=penalties&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=penaltyMinutes&page=0&pageSize=50
Penalty Kill:       http://www.nhl.com/stats/skaters?aggregate=0&report=penalties&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=penaltyMinutes&page=0&pageSize=50
Penalty Shots:      http://www.nhl.com/stats/skaters?aggregate=0&report=penaltyShots&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=penaltyShotsGoals&page=0&pageSize=50
Power Play:         [done] http://www.nhl.com/stats/skaters?aggregate=0&report=powerplay&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=ppTimeOnIce&page=0&pageSize=50
Puck Possession:    [percentages based on shots and toi] http://www.nhl.com/stats/skaters?aggregate=0&report=puckPossessions&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=satPct&page=0&pageSize=50
Shot Attempts:      [done] http://www.nhl.com/stats/skaters?aggregate=0&report=summaryshooting&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=satTotal,usatTotal&page=0&pageSize=50
Shot Att %:         http://www.nhl.com/stats/skaters?aggregate=0&report=percentages&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=satPercentage&page=0&pageSize=50
Shootout:           http://www.nhl.com/stats/skaters?aggregate=0&report=shootout&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=shootoutGamesPlayed,gte,1&sort=shootoutGoals&page=0&pageSize=50
Shots by Type:      [done] http://www.nhl.com/stats/skaters?aggregate=0&report=shottype&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=shots,shootingPct&page=0&pageSize=50
Time on Ice:        [done] http://www.nhl.com/stats/skaters?aggregate=0&report=timeonice&reportType=game&dateFrom=2021-01-21&dateTo=2021-01-21&gameType=2&filter=gamesPlayed,gte,1&sort=timeOnIce&page=0&pageSize=50
Scoring per 60
Scoring per Game
"""
from bs4 import BeautifulSoup as soup
from selenium import webdriver
import pandas as pd

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException


### ChromeDriver needs to be compatable with Chrome version
##      check Chrome version: chrome://settings/help
##      https://chromedriver.chromium.org/downloads  - find the appropriate driver version
##      Move Unzipped ChromeDriver to Applications - '/Applications/Chromedriver'

# CURRENT: ChromeDriver 96.0.4664.45 - 12/14/21

# from webdriver_manager.chrome import ChromeDriverManager
# driver =  webdriver.Chrome(ChromeDriverManager(version='95.0.4638.17').install())
# driver.get("https://www.google.com")



def url_to_soup(url, timeout_wait = 5):
    driver = webdriver.Chrome('/Applications/chromedriver')
    driver.get(url)
    timeout = timeout_wait
    try:
        element_present = EC.presence_of_element_located((By.ID, '<span class="-totalInfo">180 records returned</span>'))
        WebDriverWait(driver, timeout).until(element_present)
    except TimeoutException:
        pass
    finally:
        pass        
    source = driver.page_source
    url_soup = soup(source,'lxml')
    driver.close()
    return(url_soup)        


def nhl_daily_stats_skaters(start_date, end_date=None, season = '2021-22', game_type='regular_season'):
    """
    Pulls skater game stats from NHL.com for a given date range.

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    season : str, YYYY-YY
        Season the game stats are from - hardcode requires pull to be from a single season
    game_type : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    stats_df : df
        Top line stats per player, per game, in the range. 

    """
    if end_date is None:
        end_date = start_date
    ## STATS SUMMARY PAGE
    stats_df=pd.DataFrame()    
    if game_type == 'regular_season':
        nhl_stats_page = 'http://www.nhl.com/stats/skaters?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=points,goals,assists&page=0&pageSize=100'
    elif game_type == 'playoffs':
        nhl_stats_page = 'http://www.nhl.com/stats/skaters?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=points,goals,assists&page=0&pageSize=100'
    else:
        print('Invalid Game Type')        
            
    nhl_soup = url_to_soup(nhl_stats_page)   
    records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    pages = records//100
    ##
    stat_lines=[]
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()

        if game_type == 'regular_season':
            nhl_stats_page = 'http://www.nhl.com/stats/skaters?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=points,goals,assists&page='+str(i)+'&pageSize=100'
        elif game_type == 'playoffs':
            nhl_stats_page = 'http://www.nhl.com/stats/skaters?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=points,goals,assists&page='+str(i)+'&pageSize=100'
        else:
            print('Invalid Game Type')                
        nhl_soup_i = url_to_soup(nhl_stats_page)  

        try:
            table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        except Exception:
            continue
        data1 = []
        for rows in table_body.find_all(attrs='rt-tr-group'):
            row_data = []
            for td in rows.find_all(attrs='rt-td'):
                row_data.append(td.text)
            data1.append(row_data)
        for i in range(0,len(data1)):
            stat_line = data1[i]
            playername = stat_line[1]
            if '@' in stat_line[2]:
                game_date = stat_line[2].split('@')[0]
                home_away = 'away'
                opponent = stat_line[2].split('@')[1].strip()
            elif 'vs' in stat_line[2]:
                game_date = stat_line[2].split('vs')[0]
                home_away = 'home'
                opponent = stat_line[2].split('vs')[1].strip()
            else:
                game_date = stat_line[2]
                home_away = 'fix NHL Stats scrape code'
                opponent = 'fix NHL Stats scrape code'
            team = stat_line[3]
            position = stat_line[5]
            goals = stat_line[7]
            assists = stat_line[8]
            points = stat_line[9]
            plus_minus = stat_line[10]
            pim = stat_line[11]
            evg = stat_line[13]
            evp = stat_line[14]
            ppg = stat_line[15]
            ppp = stat_line[16]
            shg = stat_line[17]
            shp = stat_line[18]
            otg = stat_line[19]
            gwg = stat_line[20]
            shots = stat_line[21]
            toi = stat_line[23]
            player_line = [playername,game_date,home_away,opponent,team,position,goals,assists,
                           points,plus_minus,pim,evg,evp,ppg,ppp,shg,shp,otg,gwg,shots,toi]
            stat_lines.append(player_line)


    stats_df = pd.DataFrame(stat_lines, columns = ['playername','game_date','home_away',
                                                    'opponent','team','position','goals','assists',
                                                    'points','plus_minus','pim','evg','evp','ppg','ppp',
                                                    'shg','shp','otg','gwg','shots','toi'])
    stats_df['game_type'] = game_type
    stats_df['season'] = season    
    stats_df.drop(stats_df[stats_df['playername'] == '\xa0'].index, inplace=True)
    stats_df.replace('--',0, inplace=True)
    stats_df['plus_minus'] = stats_df['plus_minus'].apply(lambda row: int(row))    
    del nhl_soup
    del stat_lines
    return(stats_df) 
  
    
def nhl_daily_stats_goalies(start_date, end_date=None, season = '2021-22',game_type='regular_season'):
    """
    Pulls goalie game stats from NHL.com for a given date range.

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    reg_ploff : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    stats_df : df
        Top line stats per player, per game, in the range. 

    """
    if end_date is None:
        end_date = start_date
    stats_df=pd.DataFrame()
    
    if game_type == 'regular_season':
        nhl_stats_daily = 'http://www.nhl.com/stats/goalies?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=wins,savePct&page=0&pageSize=100'
    elif game_type == 'playoffs':
        nhl_stats_daily = 'http://www.nhl.com/stats/goalies?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=wins,savePct&page=0&pageSize=100'
    else:
        print('Invalid Game Type. Enter \'regular_season\' or \'postseason\'. Thanks, asshole.')        
   
    nhl_soup = url_to_soup(nhl_stats_daily)   
    # ADD PAGE ITERATION FOR DAILY STATS WEBPAGE        
    records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    pages = records//100
    ##
    #data1 = []
    stat_lines=[]
    #Iterate pages and pull stats into stats_df
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()        
        if game_type == 'regular_season':
            nhl_stats_pages = 'http://www.nhl.com/stats/goalies?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=wins,savePct&page='+str(i)+'&pageSize=100'
        elif game_type == 'playoffs':
            nhl_stats_pages = 'http://www.nhl.com/stats/goalies?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=wins,savePct&page='+str(i)+'&pageSize=100'
        else:
            print('Invalid Game Type. Enter \'regular_season\' or \'postseason\'. Thanks, asshole.')        
               
        try:
            nhl_soup_i = url_to_soup(nhl_stats_pages)  
        except Exception:
            continue
        table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        data1 = []
        
        """
        Previous to 2017 Playoffs, OTL is not a field for playoff games
        
        Case to handle:
        """
        szn = int(season[:4])
        
        if szn<2018 and game_type=='playoffs': 
            for rows in table_body.find_all(attrs='rt-tr-group'):
                row_data = []
                for td in rows.find_all(attrs='rt-td'):
                    row_data.append(td.text)
                data1.append(row_data)
            for i in range(0,len(data1)):
                stat_line = data1[i]
                playername = stat_line[1]
                if '@' in stat_line[2]:
                    game_date = stat_line[2].split('@')[0]
                    home_away = 'away'
                    opponent = stat_line[2].split('@')[1].strip()
                elif 'vs' in stat_line[2]:
                    game_date = stat_line[2].split('vs')[0]
                    home_away = 'home'
                    opponent = stat_line[2].split('vs')[1].strip()
                else:
                    game_date = stat_line[2]
                    home_away = 'fix NHL Stats scrape code'
                    opponent = 'fix NHL Stats scrape code'
                team = stat_line[3]
                catches = stat_line[4]
                starts = stat_line[6]
                wins = stat_line[7]
                losses = stat_line[8]
                ot_losses = 0
                shots_against = stat_line[10]
                saves = stat_line[11]
                goals_against = stat_line[12]
                sv_perc = stat_line[13]
                game_gaa = stat_line[14]
                toi = stat_line[15]
                shutouts = stat_line[16]
                goals = stat_line[17]
                assists = stat_line[18]
                points = stat_line[19]
                pim = stat_line[20]

                player_line = [playername,game_date,home_away,opponent,team,'G',catches,starts,
                                wins,losses,ot_losses,shots_against,saves,goals_against,sv_perc,
                                game_gaa,toi,shutouts,goals,assists,points,pim]
                stat_lines.append(player_line)
        
        
        else:            
            for rows in table_body.find_all(attrs='rt-tr-group'):
                row_data = []
                for td in rows.find_all(attrs='rt-td'):
                    row_data.append(td.text)
                data1.append(row_data)
            for i in range(0,len(data1)):
                stat_line = data1[i]
                playername = stat_line[1]
                if '@' in stat_line[2]:
                    game_date = stat_line[2].split('@')[0]
                    home_away = 'away'
                    opponent = stat_line[2].split('@')[1].strip()
                elif 'vs' in stat_line[2]:
                    game_date = stat_line[2].split('vs')[0]
                    home_away = 'home'
                    opponent = stat_line[2].split('vs')[1].strip()
                else:
                    game_date = stat_line[2]
                    home_away = 'fix NHL Stats scrape code'
                    opponent = 'fix NHL Stats scrape code'
                team = stat_line[3]
                catches = stat_line[4]
                starts = stat_line[6]
                wins = stat_line[7]
                losses = stat_line[8]
                ot_losses = stat_line[10]
                shots_against = stat_line[11]
                saves = stat_line[12]
                goals_against = stat_line[13]
                sv_perc = stat_line[14]
                game_gaa = stat_line[15]
                toi = stat_line[16]
                shutouts = stat_line[17]
                goals = stat_line[18]
                assists = stat_line[19]
                points = stat_line[20]
                pim = stat_line[21]
                player_line = [playername,game_date,home_away,opponent,team,'G',catches,starts,
                                wins,losses,ot_losses,shots_against,saves,goals_against,sv_perc,
                                game_gaa,toi,shutouts,goals,assists,points,pim]
                stat_lines.append(player_line)            

    #format stats_df
    stats_df = pd.DataFrame(stat_lines, columns = ['playername','game_date','home_away',
                                                    'opponent','team','position','catches','starts',
                           'wins','losses','ot_losses','shots_against','saves','goals_against','sv_perc',
                           'game_gaa','toi','shutouts','goals','assists','points','pim'])
    stats_df['game_type'] = game_type  
    stats_df['season'] = season  
    stats_df.drop(stats_df[stats_df['playername'] == '\xa0'].index, inplace=True)
    stats_df.replace('--',0, inplace=True)
    del nhl_soup
    del stat_lines
    return(stats_df)   


def nhl_game_results(start_date, end_date=None, season='2021-22', game_type='regular_season'):
    """
    Pulls game result stats from NHL.com for a given date range.

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    season : str, formatted YYYY-YY
    game_type : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    games_df : df
        Game result stats for the date range

    """
    if end_date is None:
        end_date = start_date
    games_df=pd.DataFrame()
    
    if game_type == 'regular_season':
        nhl_game_page = 'http://www.nhl.com/stats/teams?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=points,wins&page='+'0'+'&pageSize=100'
    elif game_type == 'playoffs':
        nhl_game_page = 'http://www.nhl.com/stats/teams?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=points,wins&page='+'0'+'&pageSize=100'
    else:
        print('Invalid Game Type')        
        
    
    nhl_game_soup = url_to_soup(nhl_game_page)
    
    records = int(nhl_game_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    pages = records//100
    ##
    game_lines=[]
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()

        if game_type == 'regular_season':
            nhl_game_page = 'http://www.nhl.com/stats/teams?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=points,wins&page='+str(i)+'&pageSize=100'
        elif game_type == 'playoffs':
            nhl_game_page = 'http://www.nhl.com/stats/teams?aggregate=0&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=points,wins&page='+str(i)+'&pageSize=100'
        else:
            print('Invalid Game Type')            
        nhl_soup_i = url_to_soup(nhl_game_page) 
        try:
            table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        except Exception:
            print(Exception)
        data1 = []
        for row in table_body.find_all(attrs='rt-tr-group'):
            row_data = []
            for td in row.find_all(attrs='rt-td'):
                row_data.append(td.text)
            data1.append(row_data)
                            
        for i in range(0,len(data1)):
            stat_line = data1[i]
            team = stat_line[1]
            if '@' in stat_line[2]:
                game_date = stat_line[2].split('@')[0]
                home_away = 'away'
                opponent = stat_line[2].split('@')[1].strip()
            elif 'vs' in stat_line[2]:
                game_date = stat_line[2].split('vs')[0]
                home_away = 'home'
                opponent = stat_line[2].split('vs')[1].strip()
            else:
                game_date = stat_line[2]
                home_away = 'fix NHL Games scrape code'
                opponent = 'fix NHL Games scrape code'
            gp = stat_line[3].split('.')[0]
            win = stat_line[4].split('.')[0]
            loss = stat_line[5].split('.')[0]
            tie = stat_line[6].split('.')[0]
            # otl = stat_line[7].split('.')[0]
            # points = stat_line[8].split('.')[0]
            # reg_win = stat_line[10].split('.')[0]
            # reg_ot_win = stat_line[11].split('.')[0]
            # so_win = stat_line[12].split('.')[0]
            # goals_for = stat_line[13].split('.')[0]
            # goals_against = stat_line[14].split('.')[0]
            # pp_prc = stat_line[17]
            # pk_prc = stat_line[18]
            # shots = stat_line[21].split('.')[0]
            # shots_against = stat_line[22].split('.')[0]
            # faceoff_win_prc = stat_line[23]             
            
            """
            NHL Page changes in 2018 - including an OTL column for
            playoff games after 2018. 
            """
            if game_type == 'regular_season':
                otl = stat_line[7].split('.')[0]
                points = stat_line[8].split('.')[0]
                reg_win = stat_line[10].split('.')[0]
                reg_ot_win = stat_line[11].split('.')[0]
                so_win = stat_line[12].split('.')[0]
                goals_for = stat_line[13].split('.')[0]
                goals_against = stat_line[14].split('.')[0]
                pp_prc = stat_line[17]
                pk_prc = stat_line[18]
                shots = stat_line[21].split('.')[0]
                shots_against = stat_line[22].split('.')[0]
                faceoff_win_prc = stat_line[23]                
            elif game_type == 'playoffs':
                otl = 0
                points = stat_line[7].split('.')[0]
                reg_win = stat_line[9].split('.')[0]
                reg_ot_win = stat_line[10].split('.')[0]
                so_win = stat_line[11].split('.')[0]
                goals_for = stat_line[12].split('.')[0]
                goals_against = stat_line[13].split('.')[0]
                pp_prc = stat_line[16]
                pk_prc = stat_line[17]                
                shots = stat_line[20].split('.')[0]
                shots_against = stat_line[21].split('.')[0]
                faceoff_win_prc = stat_line[22]
                

            team_line = [team, game_date, home_away, opponent, gp, win, loss, tie,
                         otl, points, reg_win, reg_ot_win, so_win, goals_for, 
                         goals_against,pp_prc, pk_prc, shots, shots_against,
                         faceoff_win_prc]
                
            game_lines.append(team_line)
    
    
    games_df = pd.DataFrame(game_lines, columns = ['team', 'game_date', 'home_away',
                                                   'opponent', 'gp', 'win', 'loss', 'tie',
                                                   'otl', 'points', 'reg_win', 'reg_ot_win',
                                                   'so_win', 'goals_for', 'goals_against',
                                                   'pp_prc', 'pk_prc', 'shots', 'shots_against',
                                                   'faceoff_win_prc'])
    games_df.drop(games_df[games_df['team'] == '\xa0'].index, inplace=True)
    
    games_df['season'] = season
    games_df['game_type'] = game_type
    
    games_df.replace('--',0, inplace=True)

    return(games_df)


def nhl_daily_stats_faceoffs(start_date, end_date=None, season = '2021-22', game_type='regular_season'):
    """
    Pulls faceoff stats from NHL.com for a given date range.

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    season : str, YYYY-YY        
    reg_ploff : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    faceoff_df : df
        Faceoff per player, per game, in the range. 

    """    
    if end_date is None:
        end_date = start_date
    faceoff_df=pd.DataFrame()
    
    if game_type == 'regular_season':
        nhl_faceoffs_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=faceoffwins&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=totalFaceoffs,gte,1&sort=totalFaceoffs&page=0&pageSize=100'                             
    elif game_type == 'playoffs':
        nhl_faceoffs_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=faceoffwins&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=totalFaceoffs,gte,1&sort=totalFaceoffs&page=0&pageSize=100'
    else:
        print('Invalid Game Type')        
    
    
    nhl_soup = url_to_soup(nhl_faceoffs_daily)   
    # ADD PAGE ITERATION FOR DAILY STATS WEBPAGE        
    try:
        records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    except Exception:
        pass 
    pages = records//100
    ##
    combined_lines = []
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()

        if game_type == 'regular_season':
            nhl_faceoffs_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=faceoffwins&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=totalFaceoffs,gte,1&sort=totalFaceoffs&page='+str(i)+'&pageSize=100'                                
        elif game_type == 'playoffs':
            nhl_faceoffs_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=faceoffwins&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=totalFaceoffs,gte,1&sort=totalFaceoffs&page='+str(i)+'&pageSize=100'
                
        nhl_soup_i = url_to_soup(nhl_faceoffs_daily)  
        try:
            table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        except Exception:
            continue
        data1 = []
        for rows in table_body.find_all(attrs='rt-tr-group'):
            row_data = []
            for td in rows.find_all(attrs='rt-td'):
                row_data.append(td.text)
            data1.append(row_data)
        stat_line=[]
        for i in range(0,len(data1)):
            stat_line = data1[i]
            playername = stat_line[1]
            if '@' in stat_line[2]:
                game_date = stat_line[2].split('@')[0]
                home_away = 'away'
                opponent = stat_line[2].split('@')[1].strip()
            elif 'vs' in stat_line[2]:
                game_date = stat_line[2].split('vs')[0]
                home_away = 'home'
                opponent = stat_line[2].split('vs')[1].strip()
            else:
                game_date = stat_line[2]
                home_away = 'fix NHL Stats scrape code'
                opponent = 'fix NHL Stats scrape code'
            team = stat_line[3]
            #position = stat_line[5]
            faceoffs = stat_line[6]
            faceoffs_won = stat_line[7]
            faceoffs_lost = stat_line[8]
            faceoff_winpct = stat_line[9]
            ev_faceoffs = stat_line[10]
            ev_fow = stat_line[11]
            ev_fol = stat_line[12]
            pp_faceoffs = stat_line[13]
            pp_fow = stat_line[14]
            pp_fol = stat_line[15]
            sh_fo = stat_line[16]
            sh_fow = stat_line[17]
            sh_fol = stat_line[18]
            oz_fo = stat_line[19]
            oz_fow = stat_line[20]
            oz_fol = stat_line[21]
            nz_fo = stat_line[22]
            nz_fow = stat_line[23]
            nz_fol = stat_line[24]
            dz_fo = stat_line[25]
            dz_fow = stat_line[26]
            dz_fol = stat_line[27]
        
            player_line = [playername,game_date, home_away,opponent,team,
                            faceoffs, faceoffs_won ,faceoffs_lost,faceoff_winpct,
                            ev_faceoffs,ev_fow,ev_fol,pp_faceoffs,pp_fow,pp_fol,
                            sh_fo,sh_fow,sh_fol,oz_fo,oz_fow,oz_fol,nz_fo,nz_fow,nz_fol, 
                            dz_fo, dz_fow, dz_fol
                            ]
            combined_lines.append(player_line)
    
    faceoff_df = pd.DataFrame(combined_lines, columns = ['playername','game_date','home_away','opponent','team',
                        'faceoffs', 'faceoffs_won','faceoffs_lost','faceoff_winpct',
                        'ev_faceoffs','ev_fow','ev_fol','pp_faceoffs','pp_fow','pp_fol',
                        'sh_fo','sh_fow','sh_fol','oz_fo','oz_fow','oz_fol','nz_fo','nz_fow','nz_fol', 
                        'dz_fo', 'dz_fow', 'dz_fol'])
      
    faceoff_df.drop(faceoff_df[faceoff_df['playername'] == '\xa0'].index, inplace=True)
   
    faceoff_df['season'] = season
    faceoff_df['game_type'] = game_type
    
    faceoff_df.replace('--',0, inplace=True)

    return(faceoff_df)

   
def nhl_daily_stats_misc(start_date, end_date=None, season = '2021-22', game_type='regular_season'):
    """
    Pulls misc. stats from NHL.com for a given date range.
    Misc. Stats include: hits, blocked_shots, giveaways, takeaways,
                        first_goal, otg, eng, shots_missed (type)            

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    season : str, YYYY-YY        
    reg_ploff : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    misc_df : df
        Misc. per player, per game, in the range. 

    """    
    if end_date is None:
        end_date = start_date
    misc_df=pd.DataFrame()
    
    if game_type == 'regular_season':
        nhl_misc_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=realtime&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=hits&page=0&pageSize=100'
    elif game_type == 'playoffs':
        nhl_misc_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=realtime&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=hits&page=0&pageSize=100'
    else:
        print('Invalid Game Type')            
    
    nhl_soup = url_to_soup(nhl_misc_daily)   
    # ADD PAGE ITERATION FOR DAILY STATS WEBPAGE        
    try:
        records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    except Exception:
        pass 
    pages = records//100
    ##
    combined_lines = []
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()

        if game_type == 'regular_season':
            nhl_misc_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=realtime&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=hits&page='+str(i)+'&pageSize=100'
        elif game_type == 'playoffs':
            nhl_misc_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=realtime&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=hits&page='+str(i)+'&pageSize=100'
                
        nhl_soup_i = url_to_soup(nhl_misc_daily)  
        try:
            table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        except Exception:
            continue
        data1 = []
        for rows in table_body.find_all(attrs='rt-tr-group'):
            row_data = []
            for td in rows.find_all(attrs='rt-td'):
                row_data.append(td.text)
            data1.append(row_data)
        stat_line=[]

        for i in range(0,len(data1)):
            stat_line = data1[i]
            playername = stat_line[1]
            if '@' in stat_line[2]:
                game_date = stat_line[2].split('@')[0]
                home_away = 'away'
                opponent = stat_line[2].split('@')[1].strip()
            elif 'vs' in stat_line[2]:
                game_date = stat_line[2].split('vs')[0]
                home_away = 'home'
                opponent = stat_line[2].split('vs')[1].strip()
            else:
                game_date = stat_line[2]
                home_away = 'fix NHL Stats scrape code'
                opponent = 'fix NHL Stats scrape code'
            team = stat_line[3]
            hits = stat_line[8]
            blocked_shots = stat_line[10]
            giveaways = stat_line[12] 
            takeaways = stat_line[14] 
            first_goal = stat_line[16]
            otg = stat_line[18] 
            eng = stat_line[19] 
            ena = stat_line[20] 
            shots_missed = stat_line[21] 
            shots_missed_wide = stat_line[22]
            shots_missed_over = stat_line[23]
            shots_missed_post = stat_line[24] 
            shots_missed_crossbar = stat_line[25]
        
            player_line = [playername,game_date,home_away,opponent,team,
                            hits, blocked_shots, giveaways, takeaways, first_goal,
                            otg, eng, ena, shots_missed, shots_missed_wide,
                            shots_missed_over,
                            shots_missed_post, shots_missed_crossbar
                            ]
            combined_lines.append(player_line)
    misc_df = pd.DataFrame(combined_lines, columns = ['playername','game_date',
                        'home_away','opponent','team',
                        'hits', 'blocked_shots', 'giveaways', 'takeaways', 
                        'first_goal',
                        'otg', 'eng', 'ena', 'shots_missed', 'shots_missed_wide',
                        'shots_missed_over',
                        'shots_missed_post', 'shots_missed_crossbar'])
   
    misc_df.drop(misc_df[misc_df['playername'] == '\xa0'].index, inplace=True)
   
    
    misc_df.drop(misc_df[misc_df['team'] == '\xa0'].index, inplace=True)
    
    misc_df['season'] = season
    misc_df['game_type'] = game_type    
    misc_df.replace('--',0, inplace=True)

    return(misc_df)    
    
        
def nhl_daily_stats_sat(start_date, end_date=None, season = '2021-22', game_type='regular_season'):
    """
    Pulls Shooting stats from NHL.com for a given date range.

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    season : str, YYYY-YY        
    game_type : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    sat_df : df
        Shooting stats per player, per game, in the range. 

    """    

    if end_date is None:
        end_date = start_date
    
    sat_df=pd.DataFrame()
    
    if game_type == 'regular_season':
        nhl_sat_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=summaryshooting&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=satTotal,usatTotal&page=0&pageSize=100'            
    elif game_type == 'playoffs':
        nhl_sat_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=summaryshooting&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=satTotal,usatTotal&page=0&pageSize=100'            
    else:
        print('Invalid Game Type')        
    
    
    nhl_soup = url_to_soup(nhl_sat_daily)   
    try:
        records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    except Exception:
        pass 
    pages = records//100
    ##
    combined_lines = []
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()

        if game_type == 'regular_season':
            nhl_sat_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=summaryshooting&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=satTotal,usatTotal&page='+str(i)+'&pageSize=100'            
        elif game_type == 'playoffs':
            nhl_sat_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=summaryshooting&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=satTotal,usatTotal&page='+str(i)+'&pageSize=100'            
                
        nhl_soup_i = url_to_soup(nhl_sat_daily)  
        try:
            table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        except Exception:
            continue
        data1 = []

        for rows in table_body.find_all(attrs='rt-tr-group'):
            row_data = []
            for td in rows.find_all(attrs='rt-td'):
                row_data.append(td.text)
            data1.append(row_data)
        stat_line=[]
        for i in range(0,len(data1)):
            stat_line = data1[i]
            playername = stat_line[1]
            if '@' in stat_line[2]:
                game_date = stat_line[2].split('@')[0]
                home_away = 'away'
                opponent = stat_line[2].split('@')[1].strip()
            elif 'vs' in stat_line[2]:
                game_date = stat_line[2].split('vs')[0]
                home_away = 'home'
                opponent = stat_line[2].split('vs')[1].strip()
            else:
                game_date = stat_line[2]
                home_away = 'fix NHL Stats scrape code'
                opponent = 'fix NHL Stats scrape code'
            sat_for = stat_line[7]
            sat_against = stat_line[8]
            sat_net = stat_line[9]
            sat_ahead = stat_line[10]
            sat_tied = stat_line[11]
            sat_behind = stat_line[12]
            sat_close = stat_line[13]
            sat_relative = stat_line[14]
            usat_for = stat_line[15]
            usat_against = stat_line[16]
            usat_net = stat_line[17]
            usat_ahead = stat_line[18]
            usat_tied = stat_line[19]
            usat_behind = stat_line[20]
            usat_close = stat_line[21]
            usat_relative = stat_line[22]
            player_line = [playername, game_date, home_away, opponent, 
                            sat_for,sat_against,sat_net, sat_ahead, sat_tied,
            sat_behind, sat_close, sat_relative, usat_for, usat_against, usat_net,
            usat_ahead, usat_tied, usat_behind, usat_close, usat_relative]
                
            combined_lines.append(player_line)
    
    sat_df = pd.DataFrame(combined_lines, columns = ['playername','game_date','home_away',
                                                    'opponent','sat_for','sat_against','sat_net',
                                                    'sat_ahead', 'sat_tied',
            'sat_behind', 'sat_close', 'sat_relative', 'usat_for', 'usat_against', 'usat_net',
            'usat_ahead', 'usat_tied', 'usat_behind', 'usat_close', 'usat_relative'])
    sat_df.drop(sat_df[sat_df['playername'] == '\xa0'].index, inplace=True)
    
    sat_df['season'] = season
    sat_df['game_type'] = game_type
    
    sat_df.replace('--',0, inplace=True)
           
    return(sat_df)
    
    

def nhl_daily_stats_shottype(start_date, end_date=None, season = '2021-22', game_type='regular_season'):
    """
    Pulls Shot stats from NHL.com for a given date range.

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    season : str, YYYY-YY        
    reg_ploff : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    sat_df : df
        Shot stats per player, per game, in the range. 

    """    
    shottype_df=pd.DataFrame()
    
    if end_date is None:
        end_date = start_date
    
    if game_type == 'regular_season':
        nhl_shottype_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=shottype&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=shots,gte,1&sort=shots,shootingPct&page=0&pageSize=100'                            
    elif game_type == 'playoffs':
        nhl_shottype_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=shottype&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=shots,gte,1&sort=shots,shootingPct&page=0&pageSize=100'                            
    else:
        print('Invalid Game Type')    
    
    nhl_soup = url_to_soup(nhl_shottype_daily)   
    
    try:
        records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    except Exception:
        pass 
    pages = records//100

    combined_lines=[]
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()
        if game_type == 'regular_season':
            nhl_shottype_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=shottype&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=shots,gte,1&sort=shots,shootingPct&page='+str(i)+'&pageSize=100'
                                 
        elif game_type == 'playoffs':
            nhl_shottype_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=shottype&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=shots,gte,1&sort=shots,shootingPct&page='+str(i)+'&pageSize=100'
        
        nhl_soup_i = url_to_soup(nhl_shottype_daily)  
        try:
            table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        except Exception:
            pass
        data1 = []
        for rows in table_body.find_all(attrs='rt-tr-group'):
            row_data = []
            for td in rows.find_all(attrs='rt-td'):
                row_data.append(td.text)
            data1.append(row_data)
        stat_line=[]
        for i in range(0,len(data1)):
            stat_line = data1[i]
            playername = stat_line[1]
            if '@' in stat_line[2]:
                game_date = stat_line[2].split('@')[0]
                home_away = 'away'
                opponent = stat_line[2].split('@')[1].strip()
            elif 'vs' in stat_line[2]:
                game_date = stat_line[2].split('vs')[0]
                home_away = 'home'
                opponent = stat_line[2].split('vs')[1].strip()
            else:
                game_date = stat_line[2]
                home_away = 'fix NHL Stats scrape code'
                opponent = 'fix NHL Stats scrape code'
            team = stat_line[3]
            goals = stat_line[5]
            g_wrist = stat_line[6]
            g_snap = stat_line[7]
            g_slap = stat_line[8]
            g_back = stat_line[9]
            g_tip = stat_line[10]
            g_def = stat_line[11]
            g_wrap = stat_line[12]
            shots = stat_line[13]
            s_wrist = stat_line[14]
            s_snap = stat_line[15]
            s_slap = stat_line[16]
            s_back = stat_line[17]
            s_tip = stat_line[18]
            s_def = stat_line[19]
            s_wrap = stat_line[20]
            player_line = [playername, game_date, home_away, opponent, team,
                            goals, g_wrist, g_snap, g_slap, g_back, g_tip, g_def,
                            g_wrap, shots, s_wrist, s_snap, s_slap, s_back, s_tip, s_def,
                            s_wrap]
        
                
            combined_lines.append(player_line)

    shottype_df = pd.DataFrame(combined_lines, columns = ['playername','game_date','home_away',
                                                    'opponent','team',
                            'goals', 'g_wrist', 'g_snap', 'g_slap', 'g_back', 'g_tip', 'g_def',
                            'g_wrap', 'shots', 's_wrist', 's_snap', 's_slap', 's_back', 's_tip', 's_def',
                            's_wrap'])
    shottype_df.drop(shottype_df[shottype_df['playername'] == '\xa0'].index, inplace=True)
         
    shottype_df['season'] = season
    shottype_df['game_type'] = game_type    
    shottype_df.replace('--',0, inplace=True)
           
    return(shottype_df)           


def nhl_daily_stats_powerplay(start_date, end_date=None, season = '2021-22', game_type='regular_season'):
    """
    Pulls PowerPlay stats from NHL.com for a given date range.

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    season : str, YYYY-YY        
    reg_ploff : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    pp_df : df
        XX. 

    """    
    pp_df=pd.DataFrame()
    
    if end_date is None:
        end_date = start_date
    
    if game_type == 'regular_season':
        nhl_pp_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=powerplay&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=ppTimeOnIce&page=0&pageSize=100'
    elif game_type == 'playoffs':
        nhl_pp_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=powerplay&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=ppTimeOnIce&page=0&pageSize=100'
    else:
        print('Invalid Game Type')    
    
    nhl_soup = url_to_soup(nhl_pp_daily)   
    
    try:
        records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    except Exception:
        pass 
    pages = records//100

    combined_lines=[]
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()
        if game_type == 'regular_season':
            nhl_pp_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=powerplay&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=ppTimeOnIce&page='+str(i)+'&pageSize=100'                                 
        elif game_type == 'playoffs':
            nhl_pp_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=powerplay&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=ppTimeOnIce&page='+str(i)+'&pageSize=100'
        
        nhl_soup_i = url_to_soup(nhl_pp_daily)  
        try:
            table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        except Exception:
            pass
        data1 = []
        for rows in table_body.find_all(attrs='rt-tr-group'):
            row_data = []
            for td in rows.find_all(attrs='rt-td'):
                row_data.append(td.text)
            data1.append(row_data)
        stat_line=[]
        for i in range(0,len(data1)):
            stat_line = data1[i]
            playername = stat_line[1]
            if '@' in stat_line[2]:
                game_date = stat_line[2].split('@')[0]
                home_away = 'away'
                opponent = stat_line[2].split('@')[1].strip()
            elif 'vs' in stat_line[2]:
                game_date = stat_line[2].split('vs')[0]
                home_away = 'home'
                opponent = stat_line[2].split('vs')[1].strip()

            team = stat_line[3]
            ppg = stat_line[6]
            ppa = stat_line[7]
            ppa1 = stat_line[8]
            ppa2 = stat_line[9]            
            pp_isat_for = stat_line[11]
            pp_shots = stat_line[12]
            pp_toi = stat_line[21]
            pp_toi_pct = stat_line[23]
            
            player_line = [playername, game_date, home_away, opponent, team,
                            ppg, ppa, ppa1, ppa2, pp_isat_for, pp_shots, pp_toi, pp_toi_pct]
        
                
            combined_lines.append(player_line)

    pp_df = pd.DataFrame(combined_lines, columns = ['playername', 'game_date', 'home_away',
                                                    'opponent', 'team', 'ppg', 'ppa', 'ppa1', 'ppa2', 
                                                    'pp_isat_for', 'pp_shots', 'pp_toi', 'pp_toi_pct'])
    pp_df.drop(pp_df[pp_df['playername'] == '\xa0'].index, inplace=True)
         
    pp_df['season'] = season
    pp_df['game_type'] = game_type    
    pp_df.replace('--',0, inplace=True)
           
    return(pp_df)      



def nhl_daily_stats_penalties(start_date, end_date=None, season = '2021-22', game_type='regular_season'):
    """
    Pulls Penalty stats from NHL.com for a given date range.

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    season : str, YYYY-YY        
    reg_ploff : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    pens_df : df
        XX. 

    """    
    pens_df=pd.DataFrame()
    
    if end_date is None:
        end_date = start_date
    
    if game_type == 'regular_season':
        nhl_pp_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=penalties&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=penaltyMinutes&page=0&pageSize=100'
    elif game_type == 'playoffs':
        nhl_pp_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=penalties&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=penaltyMinutes&page=0&pageSize=100'
    else:
        print('Invalid Game Type')    
    
    nhl_soup = url_to_soup(nhl_pp_daily)   
    
    try:
        records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    except Exception:
        pass 
    pages = records//100

    combined_lines=[]
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()
        if game_type == 'regular_season':
            nhl_pp_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=penalties&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=penaltyMinutes&page='+str(i)+'&pageSize=100'
        elif game_type == 'playoffs':
            nhl_pp_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=penalties&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=penaltyMinutes&page='+str(i)+'&pageSize=100'
        
        nhl_soup_i = url_to_soup(nhl_pp_daily)  
        try:
            table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        except Exception:
            pass
        data1 = []
        for rows in table_body.find_all(attrs='rt-tr-group'):
            row_data = []
            for td in rows.find_all(attrs='rt-td'):
                row_data.append(td.text)
            data1.append(row_data)
        stat_line=[]
        for i in range(0,len(data1)):
            stat_line = data1[i]
            playername = stat_line[1]
            if '@' in stat_line[2]:
                game_date = stat_line[2].split('@')[0]
                home_away = 'away'
                opponent = stat_line[2].split('@')[1].strip()
            elif 'vs' in stat_line[2]:
                game_date = stat_line[2].split('vs')[0]
                home_away = 'home'
                opponent = stat_line[2].split('vs')[1].strip()

            team = stat_line[3]
            pim = stat_line[9]            
            penalties_drawn = stat_line[13]
            penalties_taken = stat_line[14]
            net_penalties = stat_line[15]
            minor_pens = stat_line[19]
            major_pens = stat_line[20]
            match_pens = stat_line[21]
            misconduct = stat_line[22]
            game_misconduct = stat_line[22]
            
            player_line = [playername, game_date, home_away, opponent, team,
                            pim, penalties_drawn, penalties_taken, net_penalties,
                            minor_pens, major_pens, match_pens, misconduct, game_misconduct]
        
                
            combined_lines.append(player_line)

    pens_df = pd.DataFrame(combined_lines, columns = ['playername','game_date','home_away',
                                                    'opponent','team','pim','penalties_drawn','penalties_taken','net_penalties',
                                                    'minor_pens','major_pens','match_pens','misconduct','game_misconduct'])
    pens_df.drop(pens_df[pens_df['playername'] == '\xa0'].index, inplace=True)
         
    pens_df['season'] = season
    pens_df['game_type'] = game_type    
    pens_df.replace('--',0, inplace=True)
           
    return(pens_df)    



def nhl_daily_stats_toi(start_date, end_date=None, season = '2021-22', game_type='regular_season'):
    """
    Pulls Time on Ice stats from NHL.com for a given date range.

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    season : str, YYYY-YY        
    reg_ploff : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    pens_df : df
        XX. 

    """    
    toi_df=pd.DataFrame()
    
    if end_date is None:
        end_date = start_date
    
    if game_type == 'regular_season':
        nhl_toi_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=timeonice&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=timeOnIce&page=0&pageSize=100'
    elif game_type == 'playoffs':
        nhl_toi_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=timeonice&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=timeOnIce&page=0&pageSize=100'
    else:
        print('Invalid Game Type')    
    
    nhl_soup = url_to_soup(nhl_toi_daily)   
    
    try:
        records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    except Exception:
        pass 
    pages = records//100

    combined_lines=[]
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()
        if game_type == 'regular_season':
            nhl_toi_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=timeonice&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=gamesPlayed,gte,1&sort=timeOnIce&page='+str(i)+'&pageSize=100'
        elif game_type == 'playoffs':
            nhl_toi_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=timeonice&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=gamesPlayed,gte,1&sort=timeOnIce&page='+str(i)+'&pageSize=100'
        
        nhl_soup_i = url_to_soup(nhl_toi_daily)  
        try:
            table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        except Exception:
            pass
        data1 = []
        for rows in table_body.find_all(attrs='rt-tr-group'):
            row_data = []
            for td in rows.find_all(attrs='rt-td'):
                row_data.append(td.text)
            data1.append(row_data)
        stat_line=[]
        for i in range(0,len(data1)):
            stat_line = data1[i]
            playername = stat_line[1]
            if '@' in stat_line[2]:
                game_date = stat_line[2].split('@')[0]
                home_away = 'away'
                opponent = stat_line[2].split('@')[1].strip()
            elif 'vs' in stat_line[2]:
                game_date = stat_line[2].split('vs')[0]
                home_away = 'home'
                opponent = stat_line[2].split('vs')[1].strip()

            team = stat_line[3]
            toi = stat_line[7]            
            ev_toi = stat_line[8]            
            pp_toi = stat_line[9]
            sh_toi = stat_line[10]
            ot_toi = stat_line[15]
            shifts = stat_line[17]
            toi_per_shift = stat_line[18]
            
            player_line = [playername, game_date, home_away, opponent, team,
                           toi, ev_toi, pp_toi, sh_toi, ot_toi, shifts, toi_per_shift]
        
                
            combined_lines.append(player_line)

    toi_df = pd.DataFrame(combined_lines, columns = ['playername', 'game_date', 'home_away',
                                                    'opponent','team', 'toi', 'ev_toi', 'pp_toi', 
                                                    'sh_toi', 'ot_toi', 'shifts', 'toi_per_shift'])
    toi_df.drop(toi_df[toi_df['playername'] == '\xa0'].index, inplace=True)
         
    toi_df['season'] = season
    toi_df['game_type'] = game_type    
    toi_df.replace('--',0, inplace=True)
           
    return(toi_df)    


def nhl_daily_stats_shootout(start_date, end_date=None, season = '2021-22', game_type='regular_season'):
    """
    Pulls Time on Ice stats from NHL.com for a given date range.

    Parameters
    ----------
    start_date : str, formatted YYYY-MM-DD
        Date for the span to start
    end_date : str, formatted YYYY-MM-DD
        Date for the span to go through. 
    season : str, YYYY-YY        
    reg_ploff : str, optional
        Specify needed for Playoff games. The default is 'regular_season'.
        Interpolation: {'regular_season','playoffs'}

    Returns
    -------
    pens_df : df
        XX. 

    """    
    shootout_df=pd.DataFrame()
    
    if end_date is None:
        end_date = start_date
    
    if game_type == 'regular_season':
        nhl_shootout_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=shootout&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=shootoutGamesPlayed,gte,1&sort=shootoutGoals&page=0&pageSize=100'        
    elif game_type == 'playoffs':
        nhl_shootout_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=shootout&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=shootoutGamesPlayed,gte,1&sort=shootoutGoals&page=0&pageSize=100'
    else:
        print('Invalid Game Type')    
    
    nhl_soup = url_to_soup(nhl_shootout_daily)   
    
    try:
        records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
    except Exception:
        pass 
    pages = records//100

    combined_lines=[]
    for i in range(0,pages+1):
        if i>0:
            nhl_soup_i.clear()
        if game_type == 'regular_season':
            nhl_shootout_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=shootout&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=2&filter=shootoutGamesPlayed,gte,1&sort=shootoutGoals&page='+str(i)+'&pageSize=100'        
        elif game_type == 'playoffs':
            nhl_shootout_daily = 'http://www.nhl.com/stats/skaters?aggregate=0&report=shootout&reportType=game&dateFrom='+start_date+'&dateTo='+end_date+'&gameType=3&filter=shootoutGamesPlayed,gte,1&sort=shootoutGoals&page='+str(i)+'&pageSize=100'        
        
        nhl_soup_i = url_to_soup(nhl_shootout_daily)  
        try:
            table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
        except Exception:
            pass
        data1 = []
        for rows in table_body.find_all(attrs='rt-tr-group'):
            row_data = []
            for td in rows.find_all(attrs='rt-td'):
                row_data.append(td.text)
            data1.append(row_data)
        stat_line=[]
        for i in range(0,len(data1)):
            stat_line = data1[i]
            playername = stat_line[1]
            if '@' in stat_line[2]:
                game_date = stat_line[2].split('@')[0]
                home_away = 'away'
                opponent = stat_line[2].split('@')[1].strip()
            elif 'vs' in stat_line[2]:
                game_date = stat_line[2].split('vs')[0]
                home_away = 'home'
                opponent = stat_line[2].split('vs')[1].strip()

            team = stat_line[3]
            shootout_games = stat_line[6]
            shootout_goals = stat_line[7]
            shootout_attempts = stat_line[8]
            game_deciding_goal = stat_line[10]
            
            player_line = [playername, game_date, home_away, opponent, team,
                           shootout_games, shootout_goals, shootout_attempts, game_deciding_goal]        
                
            combined_lines.append(player_line)

    shootout_df = pd.DataFrame(combined_lines, columns = ['playername', 'game_date', 'home_away','opponent','team'
                                                          ,'shootout_games', 'shootout_goals', 'shootout_attempts', 'game_deciding_goal'])
    shootout_df.drop(shootout_df[shootout_df['playername'] == '\xa0'].index, inplace=True)
         
    shootout_df['season'] = season
    shootout_df['game_type'] = game_type    
    shootout_df.replace('--',0, inplace=True)
           
    return(shootout_df)    

#GOALIE STATS PAGES
"""
Summary: [done]         http://www.nhl.com/stats/goalies?aggregate=0&reportType=game&dateFrom=2021-02-01&dateTo=2021-02-12&gameType=2&filter=gamesPlayed,gte,1&sort=wins,savePct&page=0&pageSize=50
Advanced:               http://www.nhl.com/stats/goalies?aggregate=0&report=advanced&reportType=game&dateFrom=2021-02-01&dateTo=2021-02-12&gameType=2&filter=gamesPlayed,gte,1&sort=qualityStart,a_goalsAgainstAverage&page=0&pageSize=50
Days Rest:              http://www.nhl.com/stats/goalies?aggregate=0&report=daysrest&reportType=game&dateFrom=2021-02-01&dateTo=2021-02-12&gameType=2&filter=gamesPlayed,gte,1&sort=wins,savePct&page=0&pageSize=50
Penalty Shots:          http://www.nhl.com/stats/goalies?aggregate=0&report=penaltyShots&reportType=game&dateFrom=2021-02-01&dateTo=2021-02-12&gameType=2&filter=gamesPlayed,gte,1&sort=penaltyShotsSaves,penaltyShotSavePct&page=0&pageSize=50
Saves by Strength:      http://www.nhl.com/stats/goalies?aggregate=0&report=savesByStrength&reportType=game&dateFrom=2021-02-01&dateTo=2021-02-12&gameType=2&filter=gamesPlayed,gte,1&sort=wins,savePct&page=0&pageSize=50
Shootout:               http://www.nhl.com/stats/goalies?aggregate=0&report=shootout&reportType=game&dateFrom=2021-02-01&dateTo=2021-02-12&gameType=2&filter=gamesPlayed,gte,1&sort=shootoutWins,shootoutSavePct&page=0&pageSize=50

"""



# #   GOALIE DATA BOILERPLATE
# def nhl_daily_stats_goalies(start_date, end_date=None, season = '2021-22',game_type='regular_season'):
#     if end_date is None:
#         end_date = start_date
#     stats_df=pd.DataFrame()
    
#     if game_type == 'regular_season':
#         nhl_stats_daily = 
#     elif game_type == 'playoffs':
#         nhl_stats_daily = 
#     else:
#         print('Invalid Game Type. Enter \'regular_season\' or \'postseason\'. Thanks, asshole.')        
   
#     nhl_soup = url_to_soup(nhl_stats_daily)   
#     # ADD PAGE ITERATION FOR DAILY STATS WEBPAGE        
#     records = int(nhl_soup.find('span',{'class':'-totalInfo'}).text.strip('records returned'))
#     pages = records//100
#     ##
#     #data1 = []
#     stat_lines=[]
#     #Iterate pages and pull stats into stats_df
#     for i in range(0,pages+1):
#         if i>0:
#             nhl_soup_i.clear()        
#         if game_type == 'regular_season':
#             nhl_stats_daily = 
#         elif game_type == 'playoffs':
#             nhl_stats_daily =                
#         try:
#             nhl_soup_i = url_to_soup(nhl_stats_pages)  
#         except Exception:
#             continue
#         table_body = nhl_soup_i.find(attrs='rt-table').find(attrs='rt-tbody')
#         data1 = []
        
#         """
#         Previous to 2017 Playoffs, OTL is not a field for playoff games
        
#         Case to handle:
#         """
#         szn = int(season[:4])
        
#         if szn<2018 and game_type=='playoffs': 
#             for rows in table_body.find_all(attrs='rt-tr-group'):
#                 row_data = []
#                 for td in rows.find_all(attrs='rt-td'):
#                     row_data.append(td.text)
#                 data1.append(row_data)
#             for i in range(0,len(data1)):
#                 stat_line = data1[i]
#                 playername = stat_line[1]
#                 if '@' in stat_line[2]:
#                     game_date = stat_line[2].split('@')[0]
#                     home_away = 'away'
#                     opponent = stat_line[2].split('@')[1].strip()
#                 elif 'vs' in stat_line[2]:
#                     game_date = stat_line[2].split('vs')[0]
#                     home_away = 'home'
#                     opponent = stat_line[2].split('vs')[1].strip()
#                 else:
#                     game_date = stat_line[2]
#                     home_away = 'fix NHL Stats scrape code'
#                     opponent = 'fix NHL Stats scrape code'
#                 team = stat_line[3]                
                
#                 ###
#                 # specify for the page of stats
#                 ###
#                 player_line = [playername,game_date,home_away,opponent,team,'G',catches,starts,
#                                 wins,losses,ot_losses,shots_against,saves,goals_against,sv_perc,
#                                 game_gaa,toi,shutouts,goals,assists,points,pim]
#                 stat_lines.append(player_line)
        
        
#     #format stats_df
#     stats_df = pd.DataFrame(stat_lines, columns = ['playername','game_date','home_away',
#                                                     'opponent','team',
#                                                     #specify columns)
#     stats_df['game_type'] = game_type  
#     stats_df['season'] = season  
#     stats_df.drop(stats_df[stats_df['playername'] == '\xa0'].index, inplace=True)
#     stats_df.replace('--',0, inplace=True)
#     return(stats_df)   

