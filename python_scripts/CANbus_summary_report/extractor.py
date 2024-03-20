import pandas as pd
from pymongo import MongoClient
from datetime import datetime,timedelta
from utilities import *
from credentials import connection_string
from queries import *
################################################################################
#Connect to MongoDB


def extract_data():
    data=None
    veh_ids=[SQL_query(has_canbus_vcnnect)]
    for vehicle in veh_ids:
        try:
            client = MongoClient(connection_string)
            #veh_ids = [32054]#31514]#,32247,32265] #,32247,32265]
            start_time_str=get_previous_day()
            start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%SZ")
            end_time = start_time + timedelta(days=1)
            
            query = {"veid":{"$in":vehicle},"time": {"$gt": start_time,"$lt":end_time}}
            #query = {"veid":{"$in":veh_ids},"time": {"$gt":start_time,"$lt":end_time}}
            #query={'veid': {'$in': ['32247']}}#,'time':{'$gt':'isodate(2024-02-19T00:00:00Z)', '$lt': 'isodate(2024-02-20T00:00:00Z)'}}
            projection={ "btcc": 1,"chcc": 1,"chst": 1,"dbrg": 1,"ebsc": 1,"ecom": 1,"emfl": 1,"fllv": 1,
                "idld": 1,"obis": 1,"odom": 1,"rbar": 1,"rbat": 1,"rcht": 1,"rpm": 1,"spd": 1,"svds": 1,"tbrg": 1,"tded": 1,
                "tden": 1,"teit": 1,"tenh": 1,"tfus": 1,"time": 1,"tiot": 1,"trgc": 1,"veid": 1,"xtmp":1,
                }
            #print("Executing query:", query)

            
            #with client:
            db = client['CANbus']
            today=datetime.today()
            if today.day== 1:
                collection=db[get_previous_month()]
            else :
                collection = db[get_current_month()]
            print('collection',collection)
        
            data = pd.DataFrame(list(collection.find(query,projection)))
            #data = pd.DataFrame(list(collection.find({"veid": {"$in": veh_ids}}, {"odom": 1, "tden": 1, "time": 1})))
            #data = pd.DataFrame(list(collection.find({"veid":{"$in":veh_ids},"time": {"$gte":datetime(start_time),"$lt": datetime(end_time)}},{"odom": 1, "tden": 1, "time": 1})))
            #data = pd.DataFrame(list(collection.find({"veid":{"$in":veh_ids},"time": {"$gte":datetime(2024,2,10,0,0,0,1),"$lt": datetime(2024,2,11,0,0,0,0)}},{"odom": 1, "tden": 1, "time": 1})))
            print(data)
            print("Retrieved data:", data) 
            previous_date = get_previous_day()
            formatted_previous_date = datetime.strptime(previous_date, "%Y-%m-%dT%H:%M:%SZ").strftime('%Y.%m.%d')

            if not data.empty:

                filename = f"data_{formatted_previous_date}_{veh_ids}.csv"
                data.to_csv(filename, index=False)
            else:
                print(f"No data found for {formatted_previous_date} and vehicle IDs: {veh_ids}")
        except Exception as e:
            previous_date = get_previous_day()
            formatted_previous_date = datetime.strptime(previous_date, "%Y-%m-%dT%H:%M:%SZ").strftime('%Y.%m.%d')
            print(f"Error while processing data for {formatted_previous_date}: {e}")


    # Close the connection
    client.close()
    print('data_extract',data)
    return data




