import nest_asyncio
import odds_helper as oh
import boto3
import pandas as pd
import os

from oddsapi import OddsApiClient
from botocore.exceptions import ClientError
from io import StringIO

# Needed for Odds API
nest_asyncio.apply()

# API keys
ODDS_API_KEY          = os.getenv('ODDS_API_KEY')
AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


# Connect to the Odds API and retreive spreads data:
client = OddsApiClient(api_key=ODDS_API_KEY)
uk = client.retrieve_odds(sport_key='americanfootball_ncaaf', region='uk', mkt='h2h')
us = client.retrieve_odds(sport_key='americanfootball_ncaaf', region='us', mkt='h2h')

# Convert data to JSON for each market:
uk_json = uk.json['data']
us_json = us.json['data']

# Normalize and concatenate the data:
json_list = [uk_json, us_json]
df = oh.normalize_odds_api_data(json_list)

# Split columns with list elements:
df = oh.split_h2h_list_cols(df)

# Convert Unix timestamps to date:
date_cols = ['commence_time', 'last_update']
df = oh.convert_unix_to_date(df, date_cols)

# Aggregate site counts:
df = oh.aggregate_site_counts(df)

# Check that there are no duplicates in the DataFrame:
assert not df.duplicated().any(), 'Duplicates found in DataFrame'

# ===== Connect to S3 ============================================================
s3 = boto3.client(
    's3', 
    aws_access_key_id=AWS_ACCESS_KEY_ID, 
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Check if the bucket and flatfile exist:
bucket = 'spreads-bucket'
file = 'cfb_oapi_h2h.csv'

try:
    print(f'{file} exists... appending updates and de-duplicating')
    s3_file = s3.get_object(Bucket=bucket, Key=file)
    s3_df = pd.read_csv(s3_file['Body'])

    # Combine and deduplicate the DataFrames:
    combined_df = pd.concat([df, s3_df]).drop_duplicates().reset_index(drop=True)

    # Push back to S3:
    csv_buffer = StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=file, Body=csv_buffer.getvalue())

except ClientError as e:
    # If the file doesn't exist, upload the DataFrame
    print(f'{file} not found')
    print(f'Error: {e}')

print('... h2h data push complete')