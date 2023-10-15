import pandas as pd 
from geopy.geocoders import GoogleV3
from geopy.distance import geodesic
from datetime import datetime

def lat_lng_gen(df, userloaction):
    apiKey = os.getenv('GOOGLE_API_KEY')
    geolocator = GoogleV3(api_key=apiKey)


    location = geolocator.geocode(userloaction)

    coord2 = (location.latitude, location.longitude)

    if {'LAT', 'LNG'}.issubset(df.columns):
        df['DISTANCE_TO_USER'] = df.apply(lambda row: int(geodesic((row['LAT'], row['LNG']), coord2).meters) if pd.notnull(row['LAT']) and pd.notnull(row['LNG']) else None, axis=1)
    else:
        print("'LAT', 'LNG' 컬럼 중 하나 이상이 DataFrame에 존재하지 않습니다.")
    
    return df
#-----------------------------------------------------------
# 거리상관없음일때
def non_distance_create_model_point(df):
    
    # 모델점수부분
    df['NON_DIS_MODEL_POINT'] = (1000 - ((15 * df['AREA_CONGEST_INT']) + (20 * df['ROAD_TRAFFIC_INT'])) + df['RTE_COUNT'] + (50 * df['SUB_STN_INT']))
    
    # DISTANCE_TO_USER 값이 '500m' 이내인 경우에는 model_point 값을 '0'으로 설정합니다.(본인지역 바로앞을 갈때 이걸 사용하진않기때문)
    df.loc[df['DISTANCE_TO_USER'] <= 500, 'NON_DIS_MODEL_POINT'] = 0

    return df



#-----------------------------------------------------------
#거리가 상관있을때
    # 모델점수부분
def use_distance_create_model_point(df):
    #등수 * 7 점수 가중치 / 값이 높아질수록 가깝다는게 중요시하게됨
    weight = 7
    df['DISTANCE_RANK'] = df['DISTANCE_TO_USER'].rank(method='min', ascending=True) #가까울수록 순위가 높게 
    # 모델점수부분
    df['USE_DIS_MODEL_POINT'] = (1000 - ((weight * df['DISTANCE_RANK'])+(15 * df['AREA_CONGEST_INT']) + (20 * df['ROAD_TRAFFIC_INT'])) + df['RTE_COUNT'] + (50 * df['SUB_STN_INT']))
    
    # DISTANCE_TO_USER 값이 '500m' 이내인 경우에는 model_point 값을 '0'으로 설정합니다.(본인지역 바로앞을 갈때 이걸 사용하진않기때문)
    df.loc[df['DISTANCE_TO_USER'] <= 500, 'USE_DIS_MODEL_POINT'] = 0

    return df






#-----------------------------------------------------------
# non_dis 와 use_dis 둘다
def lanked_location(df):
    df['NON_DIS_MODEL_RANK'] = df['NON_DIS_MODEL_POINT'].rank(method='min', ascending=False)
    df['USE_DIS_MODEL_RANK'] = df['USE_DIS_MODEL_POINT'].rank(method='min', ascending=False)
    return df


def create_csv(df):
    now = datetime.now()
    time_str = now.strftime("%y_%m_%d_%H%M")
    df.to_csv(f'seoul_data_{time_str}.csv', index=False, encoding='utf-8-sig') #현재 시간담긴 백업용 / 없어지는 버스노선 컬럼등의 대비하기위함

    df.to_csv('seoul_data.csv', index=False, encoding='utf-8-sig') # 갱신되면 기존건 사라지는 파일
        
def main(user_location):
    url = 'https://raw.githubusercontent.com/sunwoo209/capstone/main/seoul_data_temp.csv'
    seoul_df = pd.read_csv(url)
    seoul_df.fillna(0, inplace=True) #결측값을 모두 0으로
    seoul_df_updated = lat_lng_gen(seoul_df,user_location)
    seoul_df_updated = use_distance_create_model_point(seoul_df_updated)
    seoul_df_updated = non_distance_create_model_point(seoul_df_updated)
    seoul_df_updated = lanked_location(seoul_df_updated)


    #csv 만들기

    seoul_df_updated = create_csv(seoul_df_updated)
    
    print("csv create!")

if __name__ == "__main__":
    user_location_input = sys.argv[1]
    main(user_location_input)
