import pandas as pd
import datetime as dt


# Columns to select on:
h2h_schema = ['site_key', 'site_nice', 'last_update', 'odds.h2h', 'id', 'sport_key', 'sport_nice', 'teams', 'commence_time', 'home_team', 'sites_count']
spreads_schema = ['site_key', 'site_nice', 'last_update', 'odds.spreads.odds', 'odds.spreads.points', 'id', 'sport_key', 'sport_nice', 'teams', 'commence_time', 'home_team', 'sites_count']

# Functions
def normalize_odds_api_data(data_list: list):
    data_normalized = []
    for data in data_list:
        unpack = pd.json_normalize(data, record_path=['sites'], meta=['id', 'sport_key', 'sport_nice', 'teams', 'commence_time', 'home_team', 'sites_count'], errors='ignore')
        data_normalized.append(unpack)
    df = pd.concat(data_normalized)
    return df

def split_spreads_list_cols(data: pd.DataFrame):
    df = data
    cols_to_split = ['odds.spreads.odds', 'odds.spreads.points', 'teams']
    
    assert set(cols_to_split).issubset(df.columns), f'Columns {cols_to_split} not found in DataFrame'

    # Split columns into new columns
    df[['spread_odds_left', 'spread_odds_right']]     = df['odds.spreads.odds'].apply(pd.Series)
    df[['spread_points_left', 'spread_points_right']] = df['odds.spreads.points'].apply(pd.Series)
    df[['team_left', 'team_right']]                   = df['teams'].apply(pd.Series)

    # Drop the original columns
    df.drop(columns=cols_to_split, inplace=True)
    return df

def split_h2h_list_cols(data: pd.DataFrame):
    df = data
    cols_to_split = ['odds.h2h', 'teams']
    
    assert set(cols_to_split).issubset(df.columns), f'Columns {cols_to_split} not found in DataFrame'

    # Split columns into new columns
    df[['h2h_odds_left', 'h2h_odds_right']]     = df['odds.h2h'].apply(pd.Series)
    df[['team_left', 'team_right']]             = df['teams'].apply(pd.Series)

    # Drop the original columns
    df.drop(columns=cols_to_split, inplace=True)
    return df

def convert_unix_to_date(data: pd.DataFrame, date_cols_list: list):
    df = data
    for col in date_cols_list:
        df[col] = pd.to_datetime(df[col], unit='s')
        df[col] = df[col].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        df[col] = df[col].dt.strftime('%Y-%m-%d')
    return df

def aggregate_site_counts(data: pd.DataFrame):
    df = data
    
    # Count the number of sites for each home team
    df_grouped = df['home_team'].value_counts().reset_index()
    df_grouped.columns = ['home_team', 'total_sites_count']
    
    # Merge the site counts back into the original DataFrame
    df = pd.merge(df, df_grouped, on='home_team')
    
    # Drop old column and remove duplicates
    df.drop(columns=['sites_count'], inplace=True)
    df = df.drop_duplicates()

    return df