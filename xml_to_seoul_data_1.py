import os 
import requests
import pandas as pd
from bs4 import BeautifulSoup
from functools import partial
import re

def create_df():
    url = 'http://openapi.seoul.go.kr:8088/725069536873756e313030706965574d/xml/citydata/1/5/POI'
    area = ["{:03}".format(n) for n in range(1, 114)]#POI1 이 아닌 POI001로 만들기위한 처리 113번까지 있음

    list_of_dfs = []#초기화

    for k in area:
        resp = requests.get(url + str(k))
        content = resp.text

        xml_obj = BeautifulSoup(content,'lxml-xml')

        rows = xml_obj.findAll('SeoulRtd.citydata')

        all_columns = set()  
        for row in rows:
            columns = row.find_all()
            for column in columns:
                all_columns.add(column.name)

        data_dict_row_list=[]

        for row in rows:
            data_dict_row={col:"" for col in all_columns}
            
            if 'SUB_STN_NM' in all_columns or 'SUB_STN_LINE' in all_columns:
                sub_stn_nms=[sub_stn_nm.text for sub_stn_nm in row.find_all('SUB_STN_NM')]
                sub_stn_lines=[sub_stn_line.text for sub_stn_line in row.find_all('SUB_STN_LINE')]
                
                sub_stn_nms=list(set(sub_stn_nms))
                sub_stn_lines=list(set(sub_stn_lines))
                
                data_dict_row['SUB_STN_NM']=", ".join(sub_stn_nms)
                data_dict_row['SUB_STN_LINE']=", ".join(sub_stn_lines)

            if 'RTE_NM' in all_columns: 
                rte_names=[rte_name.text for rte_name  in row.find_all('RTE_NM')]
                unique_rte_names= list(set(rte_names)) 
                data_dict_row['RTE_NM']= len(unique_rte_names) 
                
            for column  in columns:   
                        if column.name not  in ['SUB_STN_NM', 'SUB_STN_LINE', 'RTE_NM']:
                            data_dict_row[column.name]=column.text
                
            data_dict_row_list.append(data_dict_row)

        tmp_df=pd.DataFrame(data_dict_row_list)

        selected_columns=['AREA_CD','AREA_NM', 'AREA_CONGEST_LVL', 'AREA_CONGEST_MSG', 
                            'AREA_PPLTN_MAX', 'PPLTN_TIME', 'ROAD_TRAFFIC_IDX',
                            'ROAD_MSG','SUB_STN_NM',
                            'TEMP','PRECPT_TYPE',
                            'PCP_MSG','PM25','PM10']

        # DataFrame에 존재하는 컬럼만 선택
        selected_columns = [col for col in selected_columns if col in tmp_df.columns]

        tmp_df=tmp_df[selected_columns]

        congestion_mapping_dict={'여유': 1, 
                                "보통": 2,
                                "약간 붐빔":3,
                                "붐빔":4}

        traffic_mapping_dict={"원활": 1,
                            "서행":2,
                            "정체":3}

        tmp_df['AREA_CONGEST_INT']=[congestion_mapping_dict[congest] if congest else None 
                                    for congest in tmp_df['AREA_CONGEST_LVL']]

        tmp_df['ROAD_TRAFFIC_INT']=[traffic_mapping_dict[traff] if traff else None 
                                    for traff in tmp_df['ROAD_TRAFFIC_IDX']]

        # SUB_STN_NM 컬럼이 있는 경우에만 해당 연산을 진행합니다.
        if 'SUB_STN_NM' in tmp_df.columns:
            tmp_df["SUB_STN_INT"]=tmp_df["SUB_STN_NM"].apply(lambda x: len(x.split(',')) if x else None)

        list_of_dfs.append(tmp_df)
            
    seoul_df=pd.concat(list_of_dfs,axis=0, ignore_index=True)


    
    rte_counts = [68.0, 31.0, 111.0, 15.0, 101.0, 66.0, 61.0, 14.0, 93.0, 34.0, 1.0, 30.0, 32.0, 155.0, 18.0, 
                    12.0, 34.0, 45.0, 35.0, 11.0, 25.0, 13.0, 17.0, 33.0, 10.0, 41.0, 25.0, 13.0, 59.0, 23.0, 35.0, 9.0, 134.0, 55.0, 31.0, 46.0, 50.0, 39.0, 32.0, 86.0, 141.0, 55.0, 41.0, 30.0, 23.0, 31.0, 9.0, 53.0, 16.0, 28.0, 28.0, 46.0, 45.0, 31.0, 57.0, 41.0, 6.0, 69.0, 23.0, 29.0, 48.0, 1.0, 33.0, 39.0, 13.0, 21.0, 20.0, 15.0, 42.0, 22.0, 22.0, 54.0, 28.0, 60.0, 7.0, 19.0, 14.0, 57.0, 39.0, 17.0, 57.0, 9.0, 19.0, 37.0, 19.0, 20.0, 0.0, 57.0, 2.0, 10.0, 26.0, 15.0, 1.0, 45.0, 16.0, 19.0, 40.0, 40.0, 18.0, 0.0, 11.0, 1.0, 25.0, 31.0, 13.0, 35.0, 10.0, 3.0, 31.0, 7.0, 20.0, 2.0, 4.0]
    adjusted_rte_counts = rte_counts[:len(seoul_df)]
    
    seoul_df['RTE_COUNT'] = adjusted_rte_counts
    
    
    
    
    seoul_df.to_csv('seoul_data_temp.csv', index=False, encoding='utf-8-sig')

def get_lat_lng(apiKey, address):
    """
    Returns the latitude and longitude of a location using the Google Maps Geocoding API. 
    """
    url = ('https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'
            .format(address.replace(' ','+'), apiKey))
    
    try:
        response = requests.get(url)
        resp_json_payload = response.json()
        lat = resp_json_payload['results'][0]['geometry']['location']['lat']
        lng = resp_json_payload['results'][0]['geometry']['location']['lng']
    except:
        print('ERROR: {}, {}'.format(address, response.content))
        return (None, None)
    
    return (lat, lng)

def clean_area_nm(area):
    area = area.replace('관광특구', '')  # Remove '관광특구'
    area = re.sub(r'[a-zA-Z]', '', area)  # Remove English characters
    
    replace_dict = {
        '홍대': '홍익대학교',
        '고속터미널역': '고속터미널역앞',
        '4·19 카페거리': '서울 강북구 4.19로 97',
        '광장(전통)시장': '광장시장',
        '서촌': '서촌한옥마을',
        "창동 신경제 중심지": "창동역",
        "해방촌·경리단길": "해방촌오거리",
        "불광천" : "대한민국 서울특별시 은평구 은평로"
        }
    
    for old, new in replace_dict.items():
        if old in area:
            return new
    
    return area.strip() 

def process_data(df, apiKey):
    
    df['SEARCH_FOR_NM'] = df['AREA_NM'].apply(clean_area_nm)

    partial_get_lat_lng = partial(get_lat_lng, apiKey)

    df['LAT'], df['LNG'] = zip(*df['SEARCH_FOR_NM'].apply(partial_get_lat_lng))

    df.to_csv('seoul_data_temp.csv', index=False, encoding='utf-8-sig')

def main():
    create_df()
    print("dateframe create!")
    
    # Google Cloud Platform API key 입력하는부분
    apiKey = os.getenv('GOOGLE_API_KEY')

    # 데이터 로드 
    seoul_df = pd.read_csv('seoul_data_temp.csv')  
    print("csv_temp create!")
    
    process_data(seoul_df, apiKey)

if __name__ == "__main__":
    main()
