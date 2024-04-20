import pandas as pd
import datetime as dt

def normalize_odds_api_data(data_list: list):
    data_normalized = []
    for data in data_list:
        unpack = pd.json_normalize(data, record_path=['sites'], meta=['id', 'sport_key', 'sport_nice', 'teams', 'commence_time', 'home_team', 'sites_count'], errors='ignore')
        data_normalized.append(unpack)
    df = pd.concat(data_normalized)
    return df

def split_odds_list_cols(data: pd.DataFrame):
    df = data
    cols_to_split = ['odds.spreads.odds', 'odds.spreads.points', 'teams']
    
    assert set(cols_to_split).issubset(df.columns), f'Columns {cols_to_split} not found in DataFrame'

    # Split columns into new columns
    df[['odds_away', 'odds_home']]     = df['odds.spreads.odds'].apply(pd.Series)
    df[['points_away', 'points_home']] = df['odds.spreads.points'].apply(pd.Series)
    df[['team_away', 'team_home']]     = df['teams'].apply(pd.Series)

    # Drop the original columns
    df.drop(columns=['teams', 'odds.spreads.odds', 'odds.spreads.points'], inplace=True)
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