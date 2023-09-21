import pandas as pd 
from geopy.geocoders import GoogleV3
from geopy.distance import geodesic
from datetime import datetime

def lat_lng_gen(df, userloaction):
    geolocator = GoogleV3(api_key='AIzaSyCIMKh4bchhEumjQRRWEURQ6zsQN7xFUQk')

    location = geolocator.geocode(userloaction)

    coord2 = (location.latitude, location.longitude)

    if {'LAT', 'LNG'}.issubset(df.columns):
        df['DISTANCE_TO_USER'] = df.apply(lambda row: int(geodesic((row['LAT'], row['LNG']), coord2).meters) if pd.notnull(row['LAT']) and pd.notnull(row['LNG']) else None, axis=1)
    else:
        print("'LAT', 'LNG' 컬럼 중 하나 이상이 DataFrame에 존재하지 않습니다.")
    
    return df

def create_csv(df):
    now = datetime.now()
    time_str = now.strftime("%y_%m_%d_%H%M")
    df.to_csv(f'seoul_data_{time_str}.csv', index=False, encoding='utf-8-sig') #현재 시간담긴 백업용 / 없어지는 버스노선 컬럼등의 대비하기위함

    df.to_csv('seoul_data.csv', index=False, encoding='utf-8-sig') # 갱신되면 기존건 사라지는 파일
        
def main(user_location):
    seoul_df = pd.read_csv('seoul_data_temp.csv')
    seoul_df_updated = lat_lng_gen(seoul_df,user_location)
    create_csv(seoul_df_updated)
    print("csv create!")

if __name__ == "__main__":
    user_location_input = input("Please enter your location: ")
    main(user_location_input)
