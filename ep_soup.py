#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  5 12:12:19 2021

@author: jdifrisco
"""
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup as soup
import requests
import time
from psycopg2 import Error
from datetime import datetime

#Move to Utils.py ??li
def tableDataText(table):
    """A function embedded simple EP scrapes. Finds the table data
    of an HTML page.

    Parameters
    ----------
    table : page.find( "table": {...} ) where page is requests.get(url)
        -> soup(page.content,'html.parser')

    Returns
    -------
    df_rows : df of the table data
    
    """
    rows = []
    trs = table.find_all('tr')
    headerow = [td.get_text(strip=True) for td in trs[0].find_all('th')] # header row
    if headerow: # if there is a header row include first
        rows.append(headerow)
        trs = trs[1:]
    for tr in trs: # for every table row
        rows.append([td.get_text(strip=True) for td in tr.find_all('td')]) # data row    
    df_rows = pd.DataFrame(rows[1:], columns=rows[0])
    return df_rows




def playersbyseason(league, year, playoffs=False):  
    """Get all player stats for league-season
    Tables include team and player information links and IDs

    Parameters
    ----------
    league : String of the league title
        
    year : String 'YYYY-YY' where YYYY is start year, YY is end year
        Each Season is formatted this way, even if took place in a single
        calendar year
    playoffs : Playoffs and Regular Season are different tables.
        (Default value = False)

    Returns
    -------
    df_skaters : df of skater stats for the league-season
    df_goalies : df of goalie stats for the league-season
    
    """
    if playoffs is True:
        url = 'https://www.eliteprospects.com/league/' + league + '/stats/' + year + '/playoffs' + '?page='
    else: 
        url = 'https://www.eliteprospects.com/league/' + league + '/stats/' + year + '?page='
    
    
    # Return list with all players for season in link     
    skaters = []
    for i in range(1,20):
        try:
            page = requests.get(url+str(i))    
        except Exception:
            print("No skater data for " + league + ' ' + year + 'page ' + str(i))
            continue
    
        page = soup(page.content, "html.parser")
        # Get data for players table
        if playoffs is True:
            skater_table = page.find( "table", {"class":"table table-striped table-sortable player-stats highlight-stats season"} )
        else: 
            skater_table = page.find( "table", {"class":"table table-striped table-sortable player-stats highlight-stats season"} )
    
        
        #if skater_table is not None: 
        df_skaters = tableDataText(skater_table)
        if df_skaters['#'].count()>0:
            # Remove empty rows
            df_skaters['Player'] = df_skaters['Player'].replace('',np.nan).ffill(axis=0)                
            df_skaters = df_skaters[df_skaters['Team']!=''].reset_index(drop=True)
            # Extract href links in table
        
            link_df=pd.DataFrame(columns=['item','link'])        
            for link in skater_table.find_all('a'):
                row = [link.text,link.attrs['href']]
                row[0] = row[0].strip()
                srs = pd.Series(row, index=link_df.columns)               
                link_df = link_df.append(srs, ignore_index=True)
            
            link_df.drop_duplicates(inplace=True)
            df_skaters = df_skaters.merge(link_df,left_on='Player', right_on='item', suffixes=['','_player'])
            df_skaters = df_skaters.merge(link_df, left_on='Team', right_on='item',suffixes=['','_team'])
            df_skaters.drop(columns=['item','item_team'], inplace=True)
    
            skaters.append(df_skaters)
            time.sleep(3)                
    
    try:
        df_skaters = pd.concat(skaters).reset_index()
    except Exception:
        print("No df_skaters for " + league + ' ' + year)
        pass
    
    
    df_skaters = df_skaters[['Player','Team','GP','G','A', 'TP','PPG','PIM','+/-','link','link_team']]
    df_skaters.columns = map(str.lower, df_skaters.columns)
    df_skaters['playername'] = df_skaters['player'].str.replace(r"\(.*\)","")
    df_skaters['playername'] = df_skaters['playername'].str.strip()
    df_skaters['position'] = df_skaters['player'].str.extract('.*\((.*)\).*')
    df_skaters['playerid'] = df_skaters['link'].apply(lambda row: row.split('/')[4])
    df_skaters['team_linkid'] = df_skaters['link_team'].apply(lambda row: row.split('/')[4])
    df_skaters['season'] = year
    df_skaters['league'] = league
    df_skaters = df_skaters.drop(['player'], axis=1)
    df_skaters['teamid'] = (df_skaters.team_linkid +'/'+df_skaters.league +'/'+df_skaters.season)        
    df_skaters.replace('-',0,inplace=True)
    df_skaters.drop_duplicates(subset=['playerid','teamid'], inplace=True)
    
    """
    ADD GOALIES
    """
    goalies = []
    
    if playoffs is True:
        url = 'https://www.eliteprospects.com/league/'+league+'/stats/'+year+ '/playoffs' +'?page-goalie='
    else: 
        url = 'https://www.eliteprospects.com/league/'+league+'/stats/'+year+'?page-goalie='
    
    
    for i in range(1,10):
        try:
            page = requests.get(url+str(i)+'#goalies')    
        except Exception:
            print("No goalie page for " + league + ' ' + year)
            
        page = soup(page.content, "html.parser")
        # Get data for goalie table
        goalie_table = page.find( "table", {"class":"table table-striped table-sortable goalie-stats highlight-stats season"} ) 
        if goalie_table is not None: 
            df_goalies = tableDataText(goalie_table)
        
            if df_goalies['#'].count()>0:
                # Remove empty rows
                df_goalies['Player'] = df_goalies['Player'].replace('',np.nan).ffill(axis=0)                
                df_goalies = df_goalies[df_goalies['Team']!=''].reset_index(drop=True)                
                # Extract href links in table
                link_df=pd.DataFrame(columns=['item','link'])        
                for link in goalie_table.find_all('a'):
                    row = [link.text,link.attrs['href']]
                    row[0] = row[0].strip()
                    srs = pd.Series(row, index=link_df.columns)               
                    link_df = link_df.append(srs, ignore_index=True)
                
                link_df.drop_duplicates(inplace=True)
                df_goalies = df_goalies.merge(link_df,left_on='Player', right_on='item', suffixes=['','_player'])
                df_goalies = df_goalies.merge(link_df, left_on='Team', right_on='item',suffixes=['','_team'])
                df_goalies.drop(columns=['item','item_team'], inplace=True)
    
                goalies.append(df_goalies)
                # Wait 3 seconds before going to next
                time.sleep(3)                
    
    try:
        df_goalies = pd.concat(goalies).reset_index()
    except Exception:
        print("No df_goalies for " + league + ' ' + year)
        pass
    df_goalies = df_goalies[['Player','Team','GP', 'GAA', 'SV%','link', 'link_team']]
    df_goalies.columns = map(str.lower, df_goalies.columns)
    df_goalies.player = df_goalies.player + " (G)"
    df_goalies['playername'] = df_goalies['player'].str.replace(r"\(.*\)","")
    df_goalies['playername'] = df_goalies['playername'].str.strip()
    df_goalies['position'] = df_goalies['player'].str.extract('.*\((.*)\).*')
    df_goalies['playerid'] = df_goalies['link'].apply(lambda row: row.split('/')[4])
    df_goalies['team_linkid'] = df_goalies['link_team'].apply(lambda row: row.split('/')[4])
    df_goalies['season'] = year
    df_goalies['league'] = league
    df_goalies = df_goalies.drop(['player'], axis=1)
    df_goalies['teamid'] = (df_goalies.team_linkid +'/'+df_goalies.league +'/'+df_goalies.season)    
    df_goalies.replace('-',0,inplace=True)
    df_goalies.drop_duplicates(subset=['playerid','teamid'], inplace=True)

    return df_skaters, df_goalies


def link_dict(df):
    """
    Creates a dictionary used for player-specific functions.
    
    Parameters
    ----------
    dfs : dataframe
        Skater DF generated by PlayerBySeason() 
    dfg : dataframe
        Goalie DF generated by PlayerBySeason()         

    Returns
    -------
    s_dict : dictionary, skaters {'Player Name' : 'eplink.com'}
    g_dict : dictionary, goalies {'Player Name' : 'eplink.com'}

    """
    link_dict = {}
    if 'gaa' in df.columns: #Dict of Goalies
        for line in df.itertuples():
            link_dict[line[9]] = line[5]   
    else:
        for line in df.itertuples():
            link_dict[line[13]] = line[9]   
        
    return(link_dict)


def captain(row):
    """Used in PlayerPage() to strip any Captain indicators from Team name.

    Parameters
    ----------
    row : list, data row from EP Player Page
        

    Returns
    -------
    
    
    """
    if row['cap']==-1:
        return 'none'
    else:
        return row['Team'][-2]


                
def playerpage(link_dict):
    """Scrapes individual player pages from EP

    Parameters
    ----------
    link_dict : dict of {'Player Name': 'ep.com/player/123/player-name'}
        Link Dict is generated by calling Link_Dict() on a PlayersBySeason() DF

    Returns
    -------
    skater_data : df of player's career stats 
    goalie_data : df of goalie's career stats
    
    """
    skater_data={}
    goalie_data = {}
    error_records = []
    
    records_remaining = len(link_dict)
    for playerid, link in link_dict.items():
        page = requests.get(link)    
        # Get data for player stats
        try:
            page = soup(page.content, "html.parser")    
        except Exception:
            continue
        
        table_type = page.find_all( "section", {"class": "player-statistics mrg-bottom regular postseason"} )    
        header_rows = -2
        
    ### Above is all input from the specific primary function inplace BEFORE
    ### the tableData() function is called
        try:
            row_data = []
            row_place = len(row_data)
            row_links = {}
            for table in table_type:
                trs = table.find_all('tr') # TRS is all rows for each table, starting with Header
                headerow = [td.get_text(strip=True) for td in trs[0].find_all('th')] # header row    
                if headerow: # if there is a header row include first
                    row_data.append(headerow)
                    trs = trs[1:]
                for tr in trs:                
                    row = []        
                    for r in tr.find_all('td'): 
                        row.append(r.get_text(strip=True))
                        link_list=[]
                        for r in tr.find_all('a'):    
                            link_list.append(r.attrs['href'])
                    row_data.append(row)
                    row_place = len(row_data)
                    row_links[row_place+header_rows] = link_list        
                    #print(row_place, header_rows, row, link_list)
            df_rows = pd.DataFrame(row_data[1:], columns=row_data[0])
            df_rows = df_rows.loc[df_rows['Team'] != 'Team']
            df_rows = df_rows.loc[df_rows['Team'] != '']
            df_rows = df_rows.loc[df_rows['League'] != '']
            df_rows['S'] = df_rows['S'].replace('', np.nan).ffill(axis=0)
            
            link_cols = 0
            for key,rl in row_links.items():
                cols = len(rl)
                if cols>link_cols:
                    link_cols = len(rl)    
            col_list = ['row_'+str(i) for i in range(0,link_cols)]
            
            link_as_df = pd.DataFrame.from_dict(row_links, orient='index',
                                         columns=[col_list])
            
            full_df = df_rows.join(link_as_df,on=None) #For some reason, new cols return as tuples
            full_df.columns = full_df.columns.map(''.join)
        
            #Customize columns to return
            full_df['org_id'] = full_df['row_0'].apply(lambda x: x.split('/')[4])
            full_df['league_id'] = full_df['row_1'].apply(lambda x: x.split('/')[4])
            full_df['cap'] = full_df['Team'].apply(lambda row: row.find('“'))
            full_df['captain'] = full_df.apply(lambda row: captain(row), axis=1)
            full_df['Team'] = full_df['Team'].apply(lambda x: x.split('“')[0])    
            full_df['teamid'] = (full_df.org_id +'/'+full_df.League +'/'+full_df.S)            
            full_df['playerid'] = playerid
            
            full_df.drop(columns=['','POST','cap'], inplace=True)
            [full_df.drop(columns=cl, inplace=True) for cl in col_list]    
            full_df.replace(['-',''],0,inplace=True)
            
            ###### DIFFERENTIATE BETWEEN GOALIE AND SKATER TABLE DATA
            if 'GAA' in full_df.columns:
                full_df.columns = ['season','team','league','gp','gd','gaa','sv%','ga',
                                   'sv','so','wlt','toi','post_gp','post_gd','post_gaa','post_sv%',
                                   'post_ga','post_sv','post_so','post_wlt','post_toi',
                                   'org_id','league_id','captain','teamid','playerid']
                full_df.drop_duplicates(subset=['teamid','playerid'],inplace=True)
                goalie_data[playerid]= full_df
                    
            else:
                
                full_df.columns = ['season','team','league','gp','g','a','tp','pim',
                                   '+/-','post_gp','post_g','post_a','post_tp','post_pim','post_+/-',
                                   'org_id','league_id','captain','teamid','playerid']
                full_df.drop_duplicates(subset=['teamid','playerid'],inplace=True)
                skater_data[playerid] = full_df
        except(Exception, Error) as error:
            print("Player Page inavalid for "+playerid, error)
            error_records.append(playerid)
            
        #playerStats.drop_duplicates(subset=['playerid','teamid'], inplace=True)
        records_remaining -= 1
        print('Player Stats received for '+str(playerid)+'. '+ str(records_remaining)+' records remaining.')
        time.sleep(3)
             
    return(skater_data, goalie_data)




def draft_elig(dob):
    """Year the player was draft-eligible, based on a 9/15 cutoff
    Used to populate 'draft_year' field in player_info df when player 
    is undrafted.

    Parameters
    ----------
    dob :
        

    Returns
    -------
    draft_year : str, YYYY
    
    """
    try:
        dtdob = datetime.strptime(dob, '%b %d, %Y')
        bd18 = dtdob.replace(year=dtdob.year+18)
        draft_cutoff = datetime.strptime(str(bd18.year) + ' Sep 15', '%Y %b %d')  
        if bd18<=draft_cutoff:
            draft_year = draft_cutoff.year
        else:
            draft_year = draft_cutoff.year + 1
        return( str(draft_year)) 
    except Exception:
            print("Draft Eligibility Info not found for " + dob)


def cardDataText(ep_card):  
    """Function for reading EP's player 'header' info.
    Used in PlayerInfo().

    Parameters
    ----------
    ep_card : soup object generated within PlayerInfo()
        

    Returns
    -------

    
    """
    divs = ep_card.find_all('div')


    card_rows1 = divs[0].find_all('div', attrs={"class":"col-xs-12 col-6 p-0"})
    
    card_vals1 = divs[0].find_all('div', attrs={"class":"col-xs-12 col-18 text-right p-0 ep-text-color--black"})
    card_vals1a = divs[0].find_all('div', attrs={"class":"col-xs-12 col-18 text-right p-0"})
    
    card_rows2 = divs[0].find_all('div', attrs={"class":"col-xs-12 col-7 p-0"})
    card_vals2 = divs[0].find_all('div', attrs={"class":"col-xs-12 col-17 text-right p-0 ep-text-color--black"})
    card_rows1.extend(card_rows2)
    card_vals1.extend(card_vals1a)
    card_vals1.extend(card_vals2)
    
    headerrow = []
    for cr in card_rows1:
        headerrow.append(cr.get_text(strip=True))
    card_detail = []
    for cv in card_vals1:
        card_detail.append(cv.get_text(strip=True))
    
    card_df = pd.DataFrame(columns=['position', 'height','weight','nation',
                                    'shoots_catches', 'youth_team','contract','cap_hit',
                                    'nhl_rights','drafted','date_of_birth', 
                                    'place_of_birth','playerid'])
    card_df['position'] = [card_detail[0]]
    card_df['age'] = [card_detail[1]]
    card_df['height'] = [card_detail[2].split('/')[0].strip()]
    card_df['weight'] = [card_detail[3].split('lbs')[0].strip()]
    card_df['nation'] = [card_detail[4]]
    card_df['shoots_catches'] = [card_detail[5]]
    card_df['youth_team'] = [card_detail[6]]
    card_df['contract'] = [card_detail[7]]
    for i,x in enumerate(headerrow):
        if x == 'Date of Birth':
            card_df['date_of_birth'] = [card_detail[i]]
        elif x == 'Place of Birth':
            card_df['place_of_birth'] = [card_detail[i]]
        elif x == 'Cap Hit':
            card_df['cap_hit'] = [card_detail[i].split('Powered by')[0].strip()]
        elif x == 'NHL Rights':
            card_df['nhl_rights'] = [card_detail[i]]
        elif x == 'Drafted':
            card_df['drafted'] = [card_detail[i]]
    #if 'drafted' not in card_df.columns:
    #    for i,x in enumerate(headerrow):
    #        if x == 'Date of Birth':
    #            try:
    #                card_df['drafted'] = card_df['Date of Birth'].apply(lambda row: 'Draft Eligible: ' + draft_elig(card_detail[row]), 
    #                                                                    axis=1)
    #            except Exception:
    #                card_df['drafted'] = 'Undrafted'
    card_df.drop(columns='age',inplace=True)
    card_df['cap_hit'] = card_df['cap_hit'].fillna(0)
       
    return(card_df)




def playerinfo(link_dict): 
    """Takes Dictionary of Player:Link values, pulls EP header info.

    Parameters
    ----------
    link_dict : dict of {'Player Name': 'ep.com/player/123/player-name'}
        Link Dict is generated by calling Link_Dict() on a PlayersBySeason() DF

    Returns
    -------
    player_info : dict of single-row dfs, keys are playernames
    
    """
    # Name of HTML element to get data from
    divname = "ep-card__body ep-card__body--unpadded"
    
    player_info = {}
    
    #playerinfo_lst_of_dfs=[]
    
    ###    
    # Loop over all players
    records_remaining=len(link_dict)
    for pid,link in link_dict.items():

        page = requests.get(link)
        
        # Get data for player card
        page_soup = soup(page.content, "html.parser")    
        ep_card = page_soup.find( "div", {"class": divname})

        # If html-parser didnt work  -try again
        if ep_card is None:
            page_soup = soup(page.content, "html.parser")    
            ep_card = page_soup.find( "div", {"class":divname} )

        if ep_card is not None: 
            try:
                info_df = cardDataText(ep_card)
            except Exception:
                print("No EP Card for " + pid + ' '+ link)
        try:
            info_df['playerid'] = pid
            info_df.replace('- / -',0, inplace=True)
            info_df['date_of_birth'].replace('-','1/1/1900', inplace=True)
            player_info[pid] = info_df
            
        except Exception:
            print("No Info DF for  " + pid + ' '+ link)

        records_remaining -= 1            
        print('Player Info received for '+ str(pid)+'. '+str(records_remaining)+' records remaining.')
        
        # for row in player_info_df.itertuples():
        #     if '#' in row.drafted:
        #         player_info_df['draft_year']  = str(pit.drafted.iloc[2]).split(' ',maxsplit=1)[0]
        #         player_info_df['draft_round'] = str(pit.drafted.iloc[2]).split(' ',maxsplit=1)[1].split('#')[0].strip()
        #         player_info_df['draft_overall_pick'] = str(pit.drafted.iloc[2]).split('#',maxsplit=1)[1].split('overall')[0].strip()
        #         player_info_df['drafted_by'] = str(pit.drafted.iloc[2]).split('by',maxsplit=1)[1].strip()
        #     else:
        #         player_info_df['draft_year']  = pit.drafted
        #         player_info_df['draft_round'] = 'undrafted'
        #         player_info_df['draft_overall_pic'] = 'undrafted'
        #         player_info_df['drafted_by'] = 'undrafted'

    # Wait 3 seconds before going to next
        time.sleep(3)
      
    return player_info


# Scrapes the EP league standings table 
def league_standings(league, season):
    """
    Looks at season standings for a given league-season

    Parameters
    ----------
    league : str, league key eg. 'ECHL'
        
    season : YYYY-YY
        

    Returns
    -------
    df_standings : df of placement and postseason result
        Contains TeamID - orgID/League/Season
    """
    url = 'https://www.eliteprospects.com/league/'+league.replace(' ','-')+'/'+season
    page = requests.get(url)    
    page = soup(page.content, "html.parser")
        
    # Identify the Standings table from the League page
    standings_table = page.find( "table", {"class":"table standings table-sortable"} )
    
    # Get standings stable into a DF
    df_standings = tableDataText(standings_table)
    
    pos_list = df_standings.loc[~df_standings.Team.isnull(),['#']]['#'].to_list() #
    df_standings['Division'] = df_standings['#'].replace(pos_list,np.nan).ffill(axis=0)   
    df_standings['Season'] = season
    df_standings['league'] = league
        
    # Create a DF of links from the BS4 object
    link_df=pd.DataFrame(columns=['item','link'])        
    for link in standings_table.find_all('a'):
        row = [link.text,link.attrs['href']]
        row[0] = row[0].strip()
        srs = pd.Series(row, index=link_df.columns)               
        link_df = link_df.append(srs, ignore_index=True)
    link_df.drop_duplicates(inplace=True)
        
    # Create a columns for Division, which comes over as a header    
    df_standings = df_standings.merge(link_df,left_on='Team', right_on='item', suffixes=['','_teams'])
    df_standings['team_linkid'] = df_standings['link'].apply(lambda row: row.split('/')[4])
    df_standings['teamid'] = (df_standings.team_linkid +'/'+league +'/'+season)        
    df_standings.drop(columns=['item','team_linkid'], inplace=True)
    df_standings.replace('-',0, inplace=True)
    df_standings['#'] = df_standings['#'].apply(lambda row: row.split('.')[0])
    
    df_standings.columns=['div_pos', 'team', 'gp', 'w', 't','l','otw', 'otl',
                          'goals', 'goals_against', 'goal_diff', 'points','ppg',
                          'postseason', 'division', 'season','league','link','teamid']    
    
    return(df_standings)



def award_links(league):
    """
    Gets the names and links for awards in the given league

    Parameters
    ----------
    league : str, 'NHL'

    Returns
    -------
    award_df : df of the awards given in the league - individual and team.

    """
    
    url = 'https://www.eliteprospects.com/awards/'+league.replace(' ','-')    
    award_df = pd.DataFrame()
    page = requests.get(url)
    page = soup(page.content, "html.parser")
    lac_table = page.find_all( "div", {"class":"list-as-columns"} )
    for link in lac_table[0].find_all('a'):
        row = [link.text,link.attrs['href']]
        row[0] = row[0].strip()
        srs = pd.Series(row, index=['award','link'])               
        award_df = award_df.append(srs, ignore_index=True)    
    
    award_df.columns = ['award', 'link']
    award_df['league'] = league

    return(award_df)


def award_winners(award,award_link,league): 
    """
    Takes info from award_links() and returns all winners
        - Currently doesn't handle coach/management awards

    Parameters
    ----------
    award : str
        The award name
    award_link : str
        Award link
    league : str
        League the award is given in. 

    Returns
    -------
    df_winners : df
        All recipients - name, season, id

    """
    winners = []    
    for i in range(1,30):
        time.sleep(3)        
        try: 
            url = award_link+'&sort-players=season&players-page='+str(i)+'#all-players-awards'
            page = requests.get(url)
            page = soup(page.content, 'html.parser')
            players_table = page.find('div', {'id': 'all-players-awards'}).find( "table", {"class":"table table-striped awards table-sortable"} )
            tdt = tableDataText(players_table)
            
            link_df=pd.DataFrame(columns=['item','link'])        
            for link in players_table.find_all('a'):
                row = [link.text,link.attrs['href']]
                row[0] = row[0].strip()
                srs = pd.Series(row, index=link_df.columns)               
                link_df = link_df.append(srs, ignore_index=True)
            
            link_df.drop_duplicates(inplace=True)
            tdt = tdt.merge(link_df,left_on='Award winner', right_on='item', suffixes=['','_player'])
            tdt['playerid'] = tdt['link'].apply(lambda row: row.split('/')[4])
            tdt['Season'] = tdt['Season'].replace('',np.nan).ffill(axis=0)
            tdt['Season'] = tdt['Season'].apply(lambda row: row[:5]+row[-2:])
            tdt['League'] = league
            tdt['award'] = award
            tdt.drop(columns=['', 'Born','Birthplace','Drafted by','item'], inplace=True)
            
            winners.append(tdt)
        
        except Exception:
            #print(str(len(winners[0])) + " records collected on " + str(i) + " page(s)")
            continue

    df_winners = pd.concat(winners).reset_index()
    df_winners.drop(columns='index', inplace=True)

    return df_winners


######## CAP FRIENDLY

def salary(year, season, status): 
      
    cf_link = 'https://www.capfriendly.com/browse/'+status+'/'+year+'?display=country,waivers-exempt,signing-status,expiry-year,performance-bonus,signing-bonus,caphit-percent,aav,length,minors-salary,base-salary,arbitration-eligible,type,signing-age,signing-date,arbitration,extension&hide=handed,skater-stats,goalie-stats'
    
    page_req = requests.get(cf_link)
    page = soup(page_req.content, "html.parser")
    divs = page.find_all('div')
    browse = page.find("div",{"id":"browse_a"})
    stort=browse.find('h5').text.find('(')+1
    fin=browse.find('h5').text.find(')')
    records = int(browse.find('h5').text[stort:fin].replace(',',''))
    pages = records//50+1
    
    link_df=pd.DataFrame(columns=['playername','url_ext','full_url'])        
    players= []
    for i in range(1,pages+1):
        time.sleep(3)
        cf_link = cf_link+'&pg='+str(i)
        page_req = requests.get(cf_link)
        page_content = soup(page_req.content, 'html.parser')
        tbl = page_content.find( "table", {"class":"tbl sortable"} )
        cf_players = tableDataText(tbl)        
        
        for link in tbl.find_all('a'):
            row = [link.text,link.attrs['href']]
            row.append('https://www.capfriendly.com/transactions/' + row[1])
            srs = pd.Series(row, index=link_df.columns)               
            link_df = link_df.append(srs, ignore_index=True)        
        
        l = len(cf_players)
        for plr in range(0,l):
            row = cf_players.iloc[plr]
            try: 
                row.PLAYER = row['PLAYER'].split('.',1)[1]
            except(Exception,Error) as error:    
                pass
                     
        cf_players = cf_players.merge(link_df, left_on='PLAYER', right_on='playername')
        players.append(cf_players)
        ###
    df_salary = pd.concat(players)
    
    df_salary.columns = ['playername', 'team', 'age', 'country', 'pos','waivers_exempt'
                     ,'arb_elig_rfa', 'type', 'extension', 'signing_date','signing_age'
                     ,'arb_elig', 'arb_requested','clause','length_yrs','signing','expiry'
                     ,'exp_year','cap_hit', 'cap_hit_pct', 'aav','salary','base_salary'
                     ,'minors', 's_bonus', 'p_bonus','playername_link','url_ext','full_url']

    df_salary['season'] = season
    # df_salary['playername'] = df_salary['playername'].apply(lambda x: x.split('.')[1])
    
    df_salary['cap_hit'] = df_salary['cap_hit'].apply(lambda x: x.split('$')[1])
    df_salary['cap_hit'] = df_salary['cap_hit'].apply(lambda x: x.replace(',',''))

    df_salary['cap_hit_pct'] = df_salary['cap_hit_pct'].apply(lambda x: x.split('%')[0])
    
    df_salary['aav'] = df_salary['aav'].apply(lambda x: x.split('$')[1])
    df_salary['aav'] = df_salary['aav'].apply(lambda x: x.replace(',',''))
    
    df_salary['salary'] = df_salary['salary'].apply(lambda x: x.split('$')[1])
    df_salary['salary'] = df_salary['salary'].apply(lambda x: x.replace(',',''))

    df_salary['base_salary'] = df_salary['base_salary'].apply(lambda x: x.split('$')[1])
    df_salary['base_salary'] = df_salary['base_salary'].apply(lambda x: x.replace(',',''))
    
    df_salary['minors'] = df_salary['minors'].apply(lambda x: x.split('$')[1])
    df_salary['minors'] = df_salary['minors'].apply(lambda x: x.replace(',',''))
    
    df_salary['s_bonus'] = df_salary['s_bonus'].apply(lambda x: x.split('$')[1])
    df_salary['s_bonus'] = df_salary['s_bonus'].apply(lambda x: x.replace(',',''))

    df_salary['p_bonus'] = df_salary['p_bonus'].apply(lambda x: x.split('$')[1])
    df_salary['p_bonus'] = df_salary['p_bonus'].apply(lambda x: x.replace(',',''))
    
    df_salary.drop_duplicates(inplace=True)
    
    return df_salary

    

 
# def flashscores(date):

# fs_link = 'https://www.flashscore.com/hockey/'
# page_req = requests.get(fs_link)
# page = soup(page_req.content, "html.parser")
# divs = page.find_all('div')

# browse = page.find("div",{"id":"live-table"})

# page.find("div", {"class": "event__header top"})

# divs.find_all('leagues--live')
