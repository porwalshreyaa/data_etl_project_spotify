import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "user_id"

# access token found after a lot of hardwork and chatgpt
token_url = 'https://accounts.spotify.com/api/token'
token_type="Bearer"
expires_in = 3600
scope = "user-read-recently-played"
client_id = "client_id"
client_secret = "client_secret"
redirect_uri = 'https://localhost:5000/'

# data = {
#     'grant_type': 'refresh_token',
#     'refresh_token': refresh_token,
#     'client_id': client_id,
#     'client_secret': client_secret,
# }

TOKEN = 'AQBdgPGvrHVFLNVnl2USfKpHSn71Q2Z7YSAImlv5M26U25vNrEYCG5pwS3Unn8L14-7209X1OQDXDNLJLvwUmCLZ7sUPRjZYuF1qwdl_mTx0U9WIL5cpr11JV8m01_Rqz7bS4fjMWOUO8jIOAoLsYaho5SelKxcxxaVLFeuF-x5e81QA6AvYKUtshKzwHPv_i7iRDKA'

data = {
    'grant_type': 'authorization_code',
    'code': TOKEN,
    'redirect_uri': redirect_uri,
    'client_id': client_id,
    'client_secret': client_secret,
}


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution.")
        return False
    # Primary key check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null value found")
    # Check that all the timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()

    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")
        
    return True

if __name__ == "__main__":
    # response = requests.post(token_url, data=data, headers={'Content-Type': 'application/x-www-form-urlencoded'})
    # if response.status_code == 200:# Successful request
    #     token_data = response.json()
    #     access_token = token_data.get('access_token')
    #     refresh_token = token_data.get('refresh_token')
    #     print(refresh_token)
    #     expires_in = token_data.get('expires_in')

    # TOKEN = access_token
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token= TOKEN)
    }
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # r = requests.get("https://api.spotify.com/v1/me", headers = headers)
    # code = AQBdgPGvrHVFLNVnl2USfKpHSn71Q2Z7YSAImlv5M26U25vNrEYCG5pwS3Unn8L14-7209X1OQDXDNLJLvwUmCLZ7sUPRjZYuF1qwdl_mTx0U9WIL5cpr11JV8m01_Rqz7bS4fjMWOUO8jIOAoLsYaho5SelKxcxxaVLFeuF-x5e81QA6AvYKUtshKzwHPv_i7iRDKA
    
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)


    data = r.json()
    print(data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data['items']:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
    
    song_dict= {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])


    if check_if_valid_data(song_df):
        print("Data valid, proceed to load stage")

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor= conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    cursor.execute(sql_query)
    print("OPENED DATABASE SUCCESSFULLY")
    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in Database")
    conn.close()
    print('Closed Database Successfully')
