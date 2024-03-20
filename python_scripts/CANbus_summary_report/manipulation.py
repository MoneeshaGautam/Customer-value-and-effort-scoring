import pandas as pd
from datetime import datetime,timedelta
from extractor import *
import numpy as np


#to extract the dat afrom mongodb
data=extract_data()
print('data',data)
#data=pd.read_csv('data_2024.03.06_[32054].csv')
#print(data)
#data =pd.read_excel('testingfuelratio_CANbus.raw_2024_01.xlsx')#temp data file for testing for HV vehicles fuel ratao
#data =pd.read_csv('testingfuelratio_CANbus.raw_2024_01.-twocsv.csv')#temp data file for testing for HV vehicles fuel ratao
# data =pd.read_csv('eltestingCANbus.raw_2024_01_35990EL.csv')#temp data file for testing for EL vehicles
#data =pd.read_csv('data_raw_2024_02_[31514].csv')#temp data file for testing for petrol vehicles
#data =pd.read_csv('CANbus.raw_2024_01_24995DE.csv')#temp data file for testing for petrol vehicles- fuel level
#data=pd.read_csv('fuel_filled_count_testingCANbus.raw_2024_01_31370DE.csv')
          
# data=data.groupby('veid')          
# for veid, group_data in data:
#     print("Group:", veid)
#     print(group_data)
#     print()


# ---------------Vehicle ID---------------
def vehicle_id():
    vehicle_id=data['veid'].unique()
    print(vehicle_id)
    return vehicle_id

# ---------------Vehicle Reg---------------
#from pysql  query

# ---------------Date ---------------
def travel_date():
    print(data['time'].dtype) 
    print(data['time']) 
    data['time']=pd.to_datetime(data['time'], errors='coerce')
    date=data['time'].dt.strftime("%d/%m/%Y")
    date.reset_index(drop=True, inplace=True)
    return date
  


# ---------------Fuel type---------------
#from vuepoint/vueconnect

# ---------------Lowest Fuel Level---------------
# def lowest_fuel_level():
#     lowest_fuel_level=data[data['fllv']!=0].groupby(['veid','time'])['fllv'].min()
#     #print(lowest_fuel_level)
#     lowest_fuel_level = lowest_fuel_level.reset_index(drop=True)
#     return lowest_fuel_level

def lowest_fuel_level():
    lowest_fuel_level=data[data['fllv']!=0]['fllv'].min()
    #print(lowest_fuel_level)
    #lowest_fuel_level = lowest_fuel_level.reset_index(drop=True)
    return lowest_fuel_level


# ---------------Total Distance Traveled in KMs---------------
def KM_Travelled():
    data['time'] = pd.to_datetime(data['time']).dt.strftime("%d/%m/%Y")
    start_odom_value=data[data['odom']!=0].groupby(['veid','time'])['odom'].first()
    end_odom_value=data[data['odom']!=0].groupby(['veid','time'])['odom'].last()
    KM_Travelled=end_odom_value-start_odom_value
    KM_Travelled = KM_Travelled.reset_index(drop=True)
    return KM_Travelled
    
# # ---------------Total Time Inginition On---------------
   
def total_time_ignition_on():   
    #tiot

    # intial_ignition=data.groupby(['veid','time'])['tiot'].first()
    # end_ignition=data.groupby(['veid','time'])['tiot'].last()
    # total_time_ignition_on=end_ignition-intial_ignition

    # #print('intial_ignition,end_ignition,total_time_ignition_on',intial_ignition,end_ignition,total_time_ignition_on)
    # total_time_ignition_on=total_time_ignition_on.reset_index(drop=True)
    # final_dataframe['Total Time Inginition On']=total_time_ignition_on.fillna('0')

    # obis
    data['obis'] = data['obis'].astype(str)
    total_time_ignition_on=data[data['obis'].str.upper() == 'TRUE'].groupby(['veid','time'])['obis'].count()
    total_seconds = total_time_ignition_on.sum()
    time_delta = timedelta(seconds=int(total_seconds))
    hours, remainder = divmod(time_delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_time = "{:02}:{:02}:{:02}".format(hours, minutes, seconds)
    #total_time_ignition_on=formatted_time.reset_index(drop=True)
    return formatted_time

# ---------------Total Hours Travelled(Diesel/Petrol)---------------

def total_hr_travelled():
    #hours traveeled=when speed= 0, calculate .next start time of  speed <> 0 and endtime when speed =0
    total_hours_travelled_diesel=data[(data['spd']!=0) & (data['fllv']!=0)].groupby(['veid','time'])['spd'].count()
    global total_hours_diesel_int
    total_hours_diesel_int=total_hours_travelled_diesel.sum()
    time_del = timedelta(seconds=int(total_hours_diesel_int))
    hours, remainder = divmod(time_del.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_time1 = "{:02}:{:02}:{:02}".format(hours, minutes, seconds)
    return formatted_time1
# ---------------Total Hours Travelled(Electric)---------------
def total_hours_electric():
    #hours traveeled=when speed= 0, calculate .next start time of  speed <> 0 and endtime when speed =0
    total_hours_travelled_electric=data[(data['spd']!=0) & (data['fllv']==0)].groupby(['veid','time'])['spd'].count()
    global total_hours_electric_int
    total_hours_electric_int=total_hours_travelled_electric.sum()
    time_del2 = timedelta(seconds=int(total_hours_electric_int))
    hours, remainder = divmod(time_del2.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_time2 = "{:02}:{:02}:{:02}".format(hours, minutes, seconds)
    return formatted_time2
# ---------------Total Fuel Used( in Litres)---------------
def total_fuel_used():
    initial_fuel=data.groupby(['veid','time'])['tfus'].first()
    end_fuel_reading=data.groupby(['veid','time'])['tfus'].last()
    total_fuel_used=end_fuel_reading-initial_fuel
    total_fuel_used = total_fuel_used.reset_index(drop=True)
    return total_fuel_used

# ---------------Total Fuel Available(%)---------------
def avbl_fuel():   
    avbl_fuel = data[data['fllv']!=0].groupby(['veid','time'])['fllv'].last()
    avbl_fuel = avbl_fuel.reset_index(drop=True)
    return avbl_fuel

# ---------------Total Battery Used---------------(measure of feild name)
def total_battery_used():
    initial_battery=data.groupby(['veid','time'])['tden'].first()
    end_battery_reading=data.groupby(['veid','time'])['tden'].last()
    total_battery_used=end_battery_reading-initial_battery
    total_battery_used = total_battery_used.reset_index(drop=True)
    return total_battery_used

#----------------Total Battery Used while driving------------------
def total_battery_used_driving():
    initial_battery=data.groupby(['veid','time'])['tded'].first()
    end_battery_reading=data.groupby(['veid','time'])['tded'].last()
    total_battery_used_driving=end_battery_reading-initial_battery
    total_battery_used_driving = total_battery_used_driving.reset_index(drop=True)
    return total_battery_used_driving

# ---------------Total Available Battery %---------------
def avbl_Battery():
    avbl_Battery = data[data['ebsc']!=0].groupby(['veid','time'])['ebsc'].last()
    avbl_Battery = avbl_Battery.reset_index(drop=True)
    return avbl_Battery
# ---------------Total Eco Mode Used In Total Driving(in %)---------------

# ---------------Electric Motor Failure (in %)---------------
# ---------------Fuels Filled(count)---------------
def times_of_fuel_filled():
    data['fuel_fill_count'] = 0
    increased = False
    first_increase_ignored = False

    for index in range(1, len(data)):
        current_value = data.at[index, 'fllv']
        previous_value = data.at[index - 1, 'fllv']

        if pd.notna(current_value) and pd.notna(previous_value) and current_value != 0 and previous_value != 0:
            if not first_increase_ignored:
                first_increase_ignored = True
                continue  # Skip the first increase

            if current_value > previous_value and not increased:
                data.at[index, 'fuel_fill_count'] = 1
                increased = True
            elif current_value <= previous_value:
                increased = False

    times_of_fuel_filled = data.groupby(['veid', 'time'])['fuel_fill_count'].sum()
    times_of_fuel_filled = times_of_fuel_filled.groupby((times_of_fuel_filled != times_of_fuel_filled.shift()).cumsum()).cumsum()
    times_of_fuel_filled = times_of_fuel_filled.reset_index(drop=True)
    return times_of_fuel_filled




# ---------------Idle Duration (Electric)---------------
def times_of_fuel_filled():
    initial_reading=data.groupby(['veid','time'])['teit'].first()
    last_reading=data.groupby(['veid','time'])['teit'].last()
    total_idle_duration=last_reading-initial_reading
    total_idle_duration = total_idle_duration.reset_index(drop=True)
    return times_of_fuel_filled

    # Idle Duration (Electric)ration (Diesel/Petrol)'---------------
def total_idle_duration():
    initial_reading=data.groupby(['veid','time'])['idld'].first()
    last_reading=data.groupby(['veid','time'])['idld'].last()
    total_idle_duration=last_reading-initial_reading
    total_idle_duration = total_idle_duration.reset_index(drop=True)
    return total_idle_duration

# ---------------Average Rpm---------------
def avg_rpm():
    avg_rpm = data[data['rpm'] != 0].groupby(['veid', 'time'])['rpm'].mean()
    avg_rpm = avg_rpm.reset_index(drop=True)
    return avg_rpm


# ---------------Maximum Rpm---------------
def max_rpm():
    max_rpm=data.groupby(['veid','time'])['rpm'].max()
    max_rpm=max_rpm.reset_index(drop=True)
    return max_rpm
# ---------------Distance For Next Service (in Meters)---------------

# ---------------Total Time On Ev---------------
def Total_Time_El():
    print(total_hours_electric_int)
    if total_hours_electric_int!=0 & total_hours_diesel_int==0:
        Total_Time_El=100
    elif total_hours_electric_int==0 & total_hours_diesel_int!=0:
        Total_Time_El=0
    else :
        Total_Time_El=total_hours_electric_int/total_hours_diesel_int
    return Total_Time_El


        
# ---------------Total Engine Hours---------------
def total_engine_hours():
    initial_engine_hours=data.groupby(['veid','time'])['tenh'].first()
    end_engine_hours=data.groupby(['veid','time'])['tenh'].last()
    total_engine_hours=end_engine_hours-initial_engine_hours
    total_engine_hours = total_engine_hours.reset_index(drop=True)
    return total_engine_hours


# ---------------Total Regenerated Energy---------------

# ---------------Brake Regen Distance---------------


# ---------------Maximum Speed---------------
def max_speed():
    max_speed=data.groupby(['veid','time'])['spd'].max()
    max_speed=max_speed.reset_index(drop=True)
    return max_speed


