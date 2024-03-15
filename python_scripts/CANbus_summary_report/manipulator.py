import pandas as pd
from datetime import datetime,timedelta
from extractor import *

def manipulation():

    final_columns = ['Vehicle ID', 'Vehicle Reg',
        "Date","Start Odom","End Odom",	"KM Travelled","Miles Travelled","KWH Start","KWH End","KWH Used","KWH/M"		
        ]
    
    # final_columns=[
    #     'Vehicle ID',
    #     'Vehicle Reg',
    #     'Date ',
    #     'Fuel type',
    #     'Lowest Fuel Level',
    #     'Total Distance Traveled in KMs',
    #     'Total Time Inginition On',
    #     'Toal Hours Travelled(in Minutes)',
    #     'Total Fuel Used( in Litres)',
    #     'Total % Fuel Available',
    #     'Total Battery Used',
    #     'Total Available Battery %',
    #     'Total Eco Mode Used In Total Driving(in %)',
    #     'Electric Motor Failure (in %)',
    #     'Fuels Filled(count)',
    #     'Idle Duration (in minutes)',
    #     'Average Rpm',
    #     'Maximum Rpm',
    #     'Distance For Next Service (in Meters)',
    #     'Total Time On Ev',
    #     'Total Engine Hours',
    #     'Total Regenerated Energy',
    #     'Brake Regen Distance',
    #     'Maximum Speed',
    #     ]
    final_dataframe = pd.DataFrame(columns = final_columns)

  
    
    #data=pd.read_csv('data_raw_2023_10_[32246, 32247].csv')
    data=extract_data()
    #month=get_previous_month()
    day=get_one_day()
    #####column description####################

    # final_dataframe["Date"] = date_list
    # final_dataframe["Start Odom"]= 1st odom data of that date
    # final_dataframe["End Odom"]=	last odom data of that date 
    # final_dataframe["KM Travelled"]= end odom - start odom
    # final_dataframe["Miles Travelled"]=km travelled * 0.621371
    # final_dataframe["KWH Start"]=ist tden data of that date
    # final_dataframe["KWH End"]=last tden  data of that day
    # final_dataframe["KWH Used"]=kwh end - kwh start
    # final_dataframe["KWH/M"]=if miles travelled is 0 then 0, else kwhused/miles travelled
    

    if "time" in data.columns:
        #########  Date column #########               
        start_date = datetime.strptime(day, "raw_%Y_%m")
        #print(start_date)
        end_date = (start_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        end_date
        date_list = pd.date_range(start_date, end_date).strftime("%d/%m/%Y").tolist()
        final_dataframe["Date"] = date_list
        data["time"] = pd.to_datetime(data["time"], format="%d/%m/%Y %H:%M")
        data["date"] = data["time"].dt.strftime("%d/%m/%Y")


        ####### start odom ##################

        start_odom_value=data.groupby('date')['odom'].first()
        final_dataframe=pd.merge(final_dataframe,start_odom_value,how ="left",left_on="Date", right_on='date')
        final_dataframe['Start Odom']=final_dataframe['odom']
        final_dataframe.drop(columns=['odom'],inplace=True)
        final_dataframe['Start Odom'].fillna(method='ffill', inplace=True)
        final_dataframe['Start Odom'].fillna(method='bfill', inplace=True)
        

        ###### last odom column ###########

        last_odom_value=data.groupby('date')['odom'].last()
        final_dataframe=pd.merge(final_dataframe,last_odom_value,how ="left",left_on="Date", right_on='date')
        final_dataframe['End Odom']=final_dataframe['odom']
        final_dataframe.drop(columns=['odom'],inplace=True)
        final_dataframe['End Odom'].fillna(0, inplace=True)
        final_dataframe['End Odom'].replace(0, pd.NA, inplace=True)
        final_dataframe['End Odom'].fillna(method='ffill', inplace=True)
        final_dataframe['End Odom'].fillna(method='bfill', inplace=True)

        ###### Km Travelled column ###########

        final_dataframe['KM Travelled']=final_dataframe['End Odom']-final_dataframe['Start Odom']
        final_dataframe['KM Travelled'].fillna(0, inplace=True)
        #final_dataframe['KM Travelled'].replace(0, pd.NA, inplace=True)
        #final_dataframe['KM Travelled'].fillna(method='ffill', inplace=True)

        ###### Miles Travelled column ###########

        final_dataframe["Miles Travelled"]=final_dataframe['KM Travelled'] * 0.621371

        ###### KWH Start column ###########

        start_tden_values = data.groupby("date")["tden"].first()
        final_dataframe=pd.merge(final_dataframe,start_tden_values,how ="left",left_on="Date", right_on='date')
        final_dataframe['KWH Start']=final_dataframe['tden']
        final_dataframe.drop(columns=['tden'],inplace=True)
        final_dataframe['KWH Start'].fillna(0, inplace=True)
        final_dataframe['KWH Start'].replace(0, pd.NA, inplace=True)
        final_dataframe['KWH Start'].fillna(method='ffill', inplace=True)


        ###### KWH end column ###########

        last_tden_value=data.groupby('date')['tden'].last()
        final_dataframe=pd.merge(final_dataframe,last_tden_value,how ="left",left_on="Date", right_on='date')
        final_dataframe['KWH End']=final_dataframe['tden']
        final_dataframe.drop(columns=['tden'],inplace=True)
        final_dataframe['KWH End'].fillna(0, inplace=True)
        final_dataframe['KWH End'].replace(0, pd.NA, inplace=True)
        final_dataframe['KWH End'].fillna(method='ffill', inplace=True)


        ###### KWH used column ###########

        final_dataframe["KWH Used"]=final_dataframe['KWH End'] - final_dataframe['KWH Start']
                                    
        
        ###### KWH/M column ###########

        final_dataframe.fillna(0, inplace=True)
        final_dataframe.replace(0, pd.NA, inplace=True)
        #final_dataframe.fillna(method='ffill', inplace=True)
        final_dataframe["KWH/M"] = pd.to_numeric(final_dataframe["KWH/M"], errors="coerce")

        for index, row in final_dataframe.iterrows():
            if pd.notna(row["Miles Travelled"]) and row["Miles Travelled"] == 0:
                final_dataframe.at[index, "KWH/M"] = 0
            elif pd.notna(row["Miles Travelled"]):
                final_dataframe.at[index, "KWH/M"] = row["KWH Used"] / row["Miles Travelled"]
            else:
                final_dataframe.at[index, "KWH/M"] = pd.NA

        final_dataframe.fillna(0, inplace=True)
        final_dataframe.replace('<NA>', 0, inplace=True)
        return final_dataframe
    else:
        print("Column 'time' not found in the extracted data.")
        return pd.DataFrame()