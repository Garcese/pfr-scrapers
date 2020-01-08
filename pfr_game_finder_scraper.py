#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  7 17:44:21 2020

@author: adam sonty

TODO: description
TODO: detailed commenting of more involved code segments
"""

#==============================================================================
# import dependencies
#==============================================================================
from bs4 import BeautifulSoup as Soup
import pandas as pd
import requests

#==============================================================================
# primary functions
#==============================================================================

#------------------------------------------------------------------------------
# pfr_scrape_all_offensive_data: scrapes all data for all offensive stat
#                                categories within designated year range
#    params:
#        min_year  => first year in range of seasons scraped
#        min_year  => last year in range of seasons scraped
#    returns:
#        all_offensive_df => Pandas DataFrame containing all (weekly) offensive 
#                            data for all players within designated year range
#------------------------------------------------------------------------------
def pfr_scrape_all_offensive_data(min_year, max_year):
    print('begin scraping...')
    
    print('    scraping passing data', end='')
    pass_data = pfr_scrape_player_game_finder('pass', min_year, max_year)
    
    print('    scraping rushing data', end='')
    rush_data = pfr_scrape_player_game_finder('rush', min_year, max_year)
    
    print('    scraping receiving data', end='')
    rece_data = pfr_scrape_player_game_finder('rece', min_year, max_year)
    
    print('    scraping scoring data', end='')
    scor_data = pfr_scrape_player_game_finder('scor', min_year, max_year)
    
    print('    scraping fumbling data', end='')
    fumb_data = pfr_scrape_player_game_finder('fumb', min_year, max_year)

    print('    merging data & formatting dataset')
    pass_df = format_scraped_data(pass_data)
    rush_df = format_scraped_data(rush_data)
    rece_df = format_scraped_data(rece_data)
    scor_df = format_scraped_data(scor_data)
    fumb_df = format_scraped_data(fumb_data)
    
    join_columns = list(pass_df.columns[0:12])
    
    all_offensive_df = pd.merge(pass_df, rush_df,  how='left', 
                                left_on=join_columns, right_on=join_columns)
    all_offensive_df = pd.merge(all_offensive_df, rece_df,  how='left', 
                                left_on=join_columns, right_on=join_columns)
    all_offensive_df = pd.merge(all_offensive_df, scor_df,  how='left', 
                                left_on=join_columns, right_on=join_columns)
    all_offensive_df = pd.merge(all_offensive_df, fumb_df,  how='left', 
                                left_on=join_columns, right_on=join_columns)
    
    all_offensive_df = format_all_offensive_df(all_offensive_df)

    print('scraping complete.')
    return all_offensive_df

#------------------------------------------------------------------------------
# format_all_fantasy_df: reorders columns in table DataFrame of all 
#                        offensive stats, removes extra columns, and sorts
#    params:
#        all_rantasy_df => Pandas DataFrame of all offensive stats
#    returns:
#        formatted_all_fantasy_df => formatted Pandas DataFrame of all fantasy 
#                                    stats relevant to skill position scoring
#------------------------------------------------------------------------------
def format_all_fantasy_df(all_offensive_df):
    cols_of_int = ['_player_id', 
                   '_player',
                   '_pos',
                   '_tm',
                   '_opp',
                   '_result',
                   '_week',
                   'passing_cmp',
                   'passing_yds',
                   'passing_td',
                   'passing_int',
                   'passing_sk',
                   'rushing_att',
                   'rushing_yds',
                   'rushing_td',
                   'receiving_rec',
                   'receiving_yds',
                   'receiving_td',
                   'scoring_2pm',
                   'scoring_td',
                   'fumbles_fl',
                   'fumbles_td']
    
    all_fantasy_df = all_offensive_df[cols_of_int]
    
    formatted_all_fantasy_df = all_fantasy_df.sort_values(['_week', 
                                                           '_tm', 
                                                           '_pos',
                                                           '_player_id'])
    
    return formatted_all_fantasy_df


#==============================================================================
# helper functions
#==============================================================================

#------------------------------------------------------------------------------
# get_url: gets url containing stat table to be scraped
#    params:
#        stat_type => determines which stat category to scrape stats for
#                     valid values: 'pass', 'rush', 'rece', 'scor', 'fumb'
#        min_year  => first year in range of seasons scraped
#        min_year  => last year in range of seasons scraped
#        player_ct => PFR tables display 100 players at a time, player_ct 
#                       is used to move to the next table
#    returns:
#        url => url containing table to be scraped
#------------------------------------------------------------------------------
def get_url(stat_type, min_year, max_year, player_ct):
    if stat_type == 'pass':
        base_url = 'https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&is_starter=E&game_type=R&career_game_num_min=1&career_game_num_max=400&qb_start_num_min=1&qb_start_num_max=400&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c5val=1.0&order_by=pass_cmp'.format(min_year, max_year)
    elif stat_type == 'rush':
        base_url = 'https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&is_starter=E&game_type=R&career_game_num_min=1&career_game_num_max=400&qb_start_num_min=1&qb_start_num_max=400&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c5val=1.0&order_by=rush_att'.format(min_year, max_year)
    elif stat_type == 'rece':
        base_url = 'https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&is_starter=E&game_type=R&career_game_num_min=1&career_game_num_max=400&qb_start_num_min=1&qb_start_num_max=400&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c5val=1.0&order_by=targets'.format(min_year, max_year)
    elif stat_type == 'scor':
        base_url = 'https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&is_starter=E&game_type=R&career_game_num_min=1&career_game_num_max=400&qb_start_num_min=1&qb_start_num_max=400&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c5val=1.0&order_by=all_td'.format(min_year, max_year)
    elif stat_type == 'fumb':
        base_url = 'https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&is_starter=E&game_type=R&career_game_num_min=1&career_game_num_max=400&qb_start_num_min=1&qb_start_num_max=400&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c5val=1.0&order_by=fumbles'.format(min_year, max_year)
    else:
        print('Error: improper stat_type')
        return False

    offset = '&offset={}'
    url = base_url + offset.format(player_ct)
    return url

#------------------------------------------------------------------------------
# find_header_row_ind: finds index of header row (last row before data rows)
#    params:
#        rows => BeautifulSoup object, all rows in the table of interest
#    returns:
#        header_row_ind => index of 'rows' that the header occupies
#------------------------------------------------------------------------------
def find_header_row_ind(rows):
    header_found = False
    header_row_ind = 0
    
    while header_found == False:
        temp_hd = rows[header_row_ind].find_all('th')
        if len(temp_hd) > 1 and temp_hd[0].string == 'Rk':
            header_found = True
        else:
            header_row_ind += 1
            
    return header_row_ind

#------------------------------------------------------------------------------
# get_header: returns the header of stats table to be scraped
#    params:
#        url => url containing table to be scraped
#    returns:
#        header => header row in the form of a list of strings
#------------------------------------------------------------------------------
def get_header(url):
    preformatted_header = pd.read_html(url)[0].columns.values
    header = format_header(preformatted_header)
    
    return header
           
#------------------------------------------------------------------------------
# format_header: flattens PFR tables' multi-indexed headers,
#                formats flattened header, makes 1st column name '_player_id'
#    params:
#        preformatted_header => PFR tables' multi-index header, pandas object
#    returns:
#        formatted_header => properly formatted header as list of strings
#------------------------------------------------------------------------------
def format_header(preformatted_header):
    for x in range(len(preformatted_header)):
        
        if 'Unnamed' in preformatted_header[x][0]:
            preformatted_header[x] = ('', 
                                      str(preformatted_header[x][1]).lower())
        else:
            preformatted_header[x] = (str(preformatted_header[x][0]).lower(),
                                      str(preformatted_header[x][1]).lower())
            
        if 'unnamed' in preformatted_header[x][1]:
            preformatted_header[x] = ('', 'loc')
            
    formatted_header = ['_'.join(col).strip() for col in preformatted_header]
    formatted_header[0] = '_player_id'
    
    return formatted_header

#------------------------------------------------------------------------------
# get_data: scrapes stats from the PFR stat table of interest,
#           ignores the mid-table header rows
#    params:
#        rows => BeautifulSoup object, all rows in the table of interest
#        header_row_ind => index of 'rows' that the header occupies
#    returns:
#        data => stats from PFR stat table of interest in the form of a
#                list of lists
#------------------------------------------------------------------------------
def get_data(rows, header_row_ind):
    data = []
    
    for row in rows[header_row_ind+1:]:
        row_data = row.find_all('td')
        if row_data != []:
            row_data = format_row_data(row)
            data.append(row_data)
        
    return data

#------------------------------------------------------------------------------
# format_row_data: formats all data in a row, 
#                  inserts player's name & PFR unique ID
#    params:
#        row => Beautiful Soup object, row in table of interest
#    returns:
#        row_data => properly formatted data row as list of strings
#------------------------------------------------------------------------------
def format_row_data(row):
    row_data = [str(x.string) for x in row.find_all('td')]
    player_name = row.find('td', attrs={'data-stat' : 'player'}).string
    player_id = row.find('td', attrs={'data-stat' : 'player'})['data-append-csv']
    row_data[0] = player_name
    row_data.insert(0, player_id)
    
    return row_data

#------------------------------------------------------------------------------
# pfr_scrape_player_game_finder: scrapes all player game finder data for a 
#                                specified stat category, within a specified
#                                range of years
#    params:
#        stat_type => determines which stat category to scrape stats for
#                     valid values: 'pass', 'rush', 'rece', 'scor', 'fumb'
#        min_year  => first year in range of seasons scraped
#        min_year  => last year in range of seasons scraped
#    returns:
#        scraped_data => all player game finder data for a specified stat 
#                        category, within a specified range of years
#                        formatted as a list of lists
#                        1st 'row' is the header
#------------------------------------------------------------------------------
def pfr_scrape_player_game_finder(stat_type, min_year, max_year):
    scraped_data = []
    
    row_count = 0
    end_reached = False
    need_header_row_ind = True
    need_header = True
    
    while end_reached == False:
        url = get_url(stat_type, min_year, max_year, row_count)
        
        if url == False:
            break
            
        try:
            page_response = requests.get(url)
        except:
            print('ERROR: page unresponsive.')
            break
        
        page_content = Soup(page_response.content, 'html.parser')
        
        tables = page_content.find_all('table')
        
        if len(tables) > 0:
            table = tables[0]
        else:
            print('no table on page.')
            break 
        
        rows = table.find_all('tr')
        
        if need_header_row_ind == True:
            header_row_ind = find_header_row_ind(rows)
            need_header_row_ind = False
        
        if len(rows) <= header_row_ind+1:
            #print('empty table, end of data reached.')
            print()
            print('        total # of ' + stat_type + ' data rows scraped:', 
                  row_count)
            end_reached = True
            break
            
        if need_header == True:    
            header = get_header(url)
            scraped_data.append(header)
            need_header = False
            
        data = get_data(rows, header_row_ind)
        scraped_data.extend(data)
        
        row_count += len(data)
        if row_count % 500 == 0:
            print('.', end='')
                
    return scraped_data

#------------------------------------------------------------------------------
# format_scraped_data: converts scraped data to Pandas DataFrame
#                      properly formats the '_loc' column (home/away),
#                      replaces all 'None' values with 0's
#    params:
#        scraped_data => all player game finder data for a specified stat 
#                        category, within a specified range of years
#                        formatted as a list of lists
#                        1st 'row' is the header
#    returns:
#        formatted_df => properly formated scraped_data as a Pandas DataFrame
#------------------------------------------------------------------------------
def format_scraped_data(scraped_data):
    formatted_df = pd.DataFrame(scraped_data[1:][:], columns=scraped_data[0][:])
    formatted_df['_loc'].replace('None', 'H', inplace=True)
    formatted_df['_loc'].replace('@', 'A', inplace=True)
    formatted_df.replace('None', 0, inplace=True)
    
    return formatted_df

#------------------------------------------------------------------------------
# format_all_offensive_df: reorders columns in table DataFrame of all 
#                          offensive stats, removes extra columns, and sorts
#    params:
#        all_offensive_df => Pandas DataFrame of all offensive stats
#    returns:
#        formatted_all_offensive_df => formatted Pandas DataFrame of all 
#                                      offensive stats
#------------------------------------------------------------------------------
def format_all_offensive_df(all_offensive_df):
    cols_of_int = ['_player_id', 
                   '_player',
                   '_pos',
                   '_tm',
                   '_opp',
                   '_result',
                   '_week',
                   '_day',
                   'passing_cmp',
                   'passing_att',
                   'passing_cmp%',
                   'passing_yds',
                   'passing_td',
                   'passing_int',
                   'passing_rate',
                   'passing_sk',
                   'passing_yds.1',
                   'passing_y/a',
                   'passing_ay/a',
                   'rushing_att',
                   'rushing_yds',
                   'rushing_y/a',
                   'rushing_td',
                   'receiving_tgt',
                   'receiving_rec',
                   'receiving_yds',
                   'receiving_y/r',
                   'receiving_td',
                   'receiving_ctch%',
                   'receiving_y/tgt',
                   'scoring_xpm',
                   'scoring_xpa',
                   'scoring_xp%',
                   'scoring_fgm',
                   'scoring_fga',
                   'scoring_fg%',
                   'scoring_2pm',
                   'scoring_sfty',
                   'scoring_td',
                   'scoring_pts',
                   'fumbles_fmb',
                   'fumbles_fl',
                   'fumbles_ff',
                   'fumbles_fr',
                   'fumbles_yds',
                   'fumbles_td']
    
    all_offensive_df = all_offensive_df[cols_of_int]
    
    formatted_all_offensive_df = all_offensive_df.sort_values(['_week',
                                                               '_tm', 
                                                               '_pos',
                                                               '_player_id'])
    
    return formatted_all_offensive_df

#==============================================================================
# example of use
#==============================================================================
    
from timeit import default_timer as timer

start = timer()

all_offensive_df = pfr_scrape_all_offensive_data(2019, 2019)

end_scrape = timer()

all_fantasy_df = format_all_fantasy_df(all_offensive_df)

all_offensive_df.to_csv('./data/2019_all_offensive_data.csv', index=False)
all_fantasy_df.to_csv('./data/2019_all_fantasy_data.csv', index=False)

end = timer()
print(end_scrape - start)



