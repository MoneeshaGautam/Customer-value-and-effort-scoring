import pandas as pd
from datetime import datetime,timedelta
from extractor import *
import numpy as np


def manipulation():
    
    
    final_columns=[
        'Vehicle ID',#done
        'Vehicle Reg',#not required
        'Date ',#done
        'Fuel type',#pending
        'Lowest Fuel Level',#done
        'Total Distance Traveled in KMs',#done
        'Total Time Inginition On',#done , need to test
        'Total Hours Travelled(Electric)',#done
        'Total Hours Travelled(Petrol/Diesel)',#done
        'Total Fuel Used( in Litres)',# done
        'Total Fuel Available(%)',#done
        'Total Battery Used',#done 'Total Discharged Energy'
        'Total Battery Used while driving',#done
        'Total Available Battery %',#done
        'Total Eco Mode Used In Total Driving(in %)',
        'Electric Motor Failure (in %)',#
        'Fuels Filled(count)',#working pending
        'Idle Duration (Diesel/Petrol)',#done
        'Idle Duration (Electric)',#done
        'Average RPM',#done
        'Maximum RPM',#done 
        'Distance For Next Service (in Meters)',#svds no data found
        'Total Time On Ev',#ratio ev/petrol
        'Total Engine Hours',#done
        'Total Regenerated Energy',
        'Brake Regen Distance',#no data found
        'Maximum Speed',#done
               

        ]
    final_dataframe=pd.DataFrame(columns=final_columns)
    
    # to extract the dat afrom mongodb
    #data=extract_data()
    #data=pd.read_csv('data_2024.03.06_[32054].csv')
    #print(data)
    data =pd.read_excel('testingfuelratio_CANbus.raw_2024_01.xlsx')#temp data file for testing for HV vehicles fuel ratao
    #data =pd.read_csv('testingfuelratio_CANbus.raw_2024_01.-twocsv.csv')#temp data file for testing for HV vehicles fuel ratao
    # data =pd.read_csv('eltestingCANbus.raw_2024_01_35990EL.csv')#temp data file for testing for EL vehicles
    #data =pd.read_csv('data_raw_2024_02_[31514].csv')#temp data file for testing for petrol vehicles
    #data =pd.read_csv('CANbus.raw_2024_01_24995DE.csv')#temp data file for testing for petrol vehicles- fuel level
    #data=pd.read_csv('fuel_filled_count_testingCANbus.raw_2024_01_31370DE.csv')
    
        

    # ---------------Vehicle ID---------------
    final_dataframe['Vehicle ID']=data['veid']


    # ---------------Vehicle Reg---------------
    #from pysql  query

    # ---------------Date ---------------

    data['time']=pd.to_datetime(data['time'])
    final_dataframe['Date']=data['time'].dt.strftime("%d/%m/%Y")
    final_dataframe['Date'].reset_index()


    # ---------------Fuel type---------------
    #from vuepoint/vueconnect

    # ---------------Lowest Fuel Level---------------

    lowest_fuel_level=data[data['fllv']!=0].groupby(['veid','time'])['fllv'].min()
    #print(lowest_fuel_level)
    lowest_fuel_level = lowest_fuel_level.reset_index(drop=True)
    #print(lowest_fuel_level)
    final_dataframe['Lowest Fuel Level'] = round(lowest_fuel_level, 2).fillna(0)
    #print(final_dataframe['Lowest Fuel Level'])
    # ---------------Total Distance Traveled in KMs---------------
    #start odom
    data['time'] = pd.to_datetime(data['time']).dt.strftime("%d/%m/%Y")
    start_odom_value=data[data['odom']!=0].groupby(['veid','time'])['odom'].first()
        
    #end odom
    end_odom_value=data[data['odom']!=0].groupby(['veid','time'])['odom'].last()
    KM_Travelled=end_odom_value-start_odom_value
        
    #KM_travelled
    KM_Travelled = KM_Travelled.reset_index(drop=True)
    final_dataframe['KM Travelled']=round(KM_Travelled,2).fillna(0)



        
    # # ---------------Total Time Inginition On---------------
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
    print(total_seconds)
    time_delta = timedelta(seconds=int(total_seconds))
    hours, remainder = divmod(time_delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_time = "{:02}:{:02}:{:02}".format(hours, minutes, seconds)
    print('formatted_time',formatted_time)
    #total_time_ignition_on=formatted_time.reset_index(drop=True)
    #final_dataframe['Total Time Inginition On']=total_time_ignition_on.fillna(0)
    final_dataframe['Total Time Inginition On']=formatted_time#.fillna(0)

    # print(final_dataframe)
        




    # ---------------Total Hours Travelled(Diesel/Petrol)---------------

    #hours traveeled=when speed= 0, calculate .next start time of  speed <> 0 and endtime when speed =0
    total_hours_travelled_diesel=data[(data['spd']!=0) & (data['fllv']!=0)].groupby(['veid','time'])['spd'].count()
    total_hours_diesel=total_hours_travelled_diesel.sum()
    print(total_hours_diesel)
    #print('total_hr_travelled',total_hours)
    time_del = timedelta(seconds=int(total_hours_diesel))
    hours, remainder = divmod(time_del.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_time1 = "{:02}:{:02}:{:02}".format(hours, minutes, seconds)
    #print('total_hr_travelled',formatted_time1)
    final_dataframe['Total Hours Travelled(Petrol/Diesel)']=formatted_time1

    # ---------------Total Hours Travelled(Electric)---------------

    #hours traveeled=when speed= 0, calculate .next start time of  speed <> 0 and endtime when speed =0
    total_hours_travelled_electric=data[(data['spd']!=0) & (data['fllv']==0)].groupby(['veid','time'])['spd'].count()
    total_hours_electric=total_hours_travelled_electric.sum()
    print('total_hr_travelled_electric',total_hours_electric)
    time_del2 = timedelta(seconds=int(total_hours_electric))
    hours, remainder = divmod(time_del2.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_time2 = "{:02}:{:02}:{:02}".format(hours, minutes, seconds)
    #print('total_hr_travelled',formatted_time1)
    final_dataframe['Total Hours Travelled(Electric)']=formatted_time2
    # ---------------Total Fuel Used( in Litres)---------------

    initial_fuel=data.groupby(['veid','time'])['tfus'].first()
    end_fuel_reading=data.groupby(['veid','time'])['tfus'].last()
    total_fuel_used=end_fuel_reading-initial_fuel
    total_fuel_used = total_fuel_used.reset_index(drop=True)
    final_dataframe['Total Fuel Used( in Litres)']=round(total_fuel_used,2).fillna(0)



    # ---------------Total Fuel Available(%)---------------
    avbl_fuel = data[data['fllv']!=0].groupby(['veid','time'])['fllv'].last()
    avbl_fuel = avbl_fuel.reset_index(drop=True)
    final_dataframe['Total Fuel Available(%)'] = round(avbl_fuel, 2).fillna(0)



    # ---------------Total Battery Used---------------(measure of feild name)
    initial_battery=data.groupby(['veid','time'])['tden'].first()
    end_battery_reading=data.groupby(['veid','time'])['tden'].last()
    total_battery_used=end_battery_reading-initial_battery
    total_battery_used = total_battery_used.reset_index(drop=True)
    final_dataframe['Total Battery Used']=round(total_battery_used,2).fillna(0)

    #----------------Total Battery Used while driving------------------
    initial_battery=data.groupby(['veid','time'])['tded'].first()
    end_battery_reading=data.groupby(['veid','time'])['tded'].last()
    total_battery_used_driving=end_battery_reading-initial_battery
    total_battery_used_driving = total_battery_used_driving.reset_index(drop=True)
    final_dataframe['Total Battery Used while Driving']=round(total_battery_used_driving,2).fillna(0)

    # ---------------Total Available Battery %---------------
    avbl_Battery = data[data['ebsc']!=0].groupby(['veid','time'])['ebsc'].last()
    avbl_Battery = avbl_Battery.reset_index(drop=True)
    final_dataframe['Total Battery Available(%)'] = round(avbl_Battery, 2).fillna(0)

    # ---------------Total Eco Mode Used In Total Driving(in %)---------------

    # ---------------Electric Motor Failure (in %)---------------
    # ---------------Fuels Filled(count)---------------

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
    final_dataframe['Fuels Filled(count)'] = round(times_of_fuel_filled, 2).fillna(0)





    # ---------------Idle Duration (Electric)---------------
    initial_reading=data.groupby(['veid','time'])['teit'].first()
    last_reading=data.groupby(['veid','time'])['teit'].last()
    total_idle_duration=last_reading-initial_reading
    total_idle_duration = total_idle_duration.reset_index(drop=True)
    final_dataframe['Idle Duration (Electric)']=round(total_idle_duration,2).fillna(0)


        # Idle Duration (Electric)ration (Diesel/Petrol)'---------------
    initial_reading=data.groupby(['veid','time'])['idld'].first()
    last_reading=data.groupby(['veid','time'])['idld'].last()
    total_idle_duration=last_reading-initial_reading
    total_idle_duration = total_idle_duration.reset_index(drop=True)
    final_dataframe['Idle Duration (Diesel/Petrol)']=round(total_idle_duration,2).fillna(0)


    # ---------------Average Rpm---------------

    avg_rpm = data[data['rpm'] != 0].groupby(['veid', 'time'])['rpm'].mean()
    avg_rpm = avg_rpm.reset_index(drop=True)
    final_dataframe['Average RPM'] = round(avg_rpm, 2).fillna(0)



    # ---------------Maximum Rpm---------------
    max_rpm=data.groupby(['veid','time'])['rpm'].max()
    max_rpm=max_rpm.reset_index(drop=True)
    final_dataframe['Maximum RPM']=round(max_rpm,2).fillna(0)

    # ---------------Distance For Next Service (in Meters)---------------

    # ---------------Total Time On Ev---------------
    if total_hours_electric!=0 & total_hours_diesel==0:
        Total_Time_El=100
    elif total_hours_electric==0 & total_hours_diesel!=0:
        Total_Time_El=0
    else :
        Total_Time_El=total_hours_electric/total_hours_diesel

    #print("Ratio of Total Time Diesel vs Electric:", Total_Time_El)

            
    # ---------------Total Engine Hours---------------

    initial_engine_hours=data.groupby(['veid','time'])['tenh'].first()
    end_engine_hours=data.groupby(['veid','time'])['tenh'].last()
    total_engine_hours=end_engine_hours-initial_engine_hours
    total_engine_hours = total_engine_hours.reset_index(drop=True)
    final_dataframe['Total Engine Hours']=round(total_engine_hours,2).fillna(0)


    # ---------------Total Regenerated Energy---------------

    # ---------------Brake Regen Distance---------------


    # ---------------Maximum Speed---------------
    max_speed=data.groupby(['veid','time'])['spd'].max()
    max_speed=max_speed.reset_index(drop=True)
    final_dataframe['Maximum Speed']=round(max_speed,2).fillna(0)



    #----------------Agregated_dataframe---------------



    aggregated_dataframe = final_dataframe.groupby(['Vehicle ID', 'Date']).agg(
    Vehicle_ID=('Vehicle ID', 'first'),
    #Date1=('Date', 'first'),
    Lowest_Fuel_Level=('Lowest Fuel Level', 'min'),
    Total_Fuel_Used_Litres=('Total Fuel Used( in Litres)', 'sum'),
    Total_Distance_Traveled_KMs=('KM Travelled', 'sum'),
    Total_Time_Ignition_On=('Total Time Inginition On', 'first'),  
    Total_Hours_Travelled_Diesel_Petrol = ('Total Hours Travelled(Petrol/Diesel)','first'),
    Total_Hours_Travelled_Electric = ('Total Hours Travelled(Electric)','first'),
    Maximum_Speed=('Maximum Speed', 'max'),
    Maximum_RPM=('Maximum RPM', 'max'),
    Average_RPM=('Average RPM', 'mean'),
    Total_Fuel_Available=('Total Fuel Available(%)', 'last'),
    Total_Battery_Available=('Total Battery Available(%)', 'last'),
    Fuels_Filled_Count=('Fuels Filled(count)', 'sum'),
    Total_Engine_Hours=('Total Engine Hours', 'sum'),
    Idle_Duration_Diesel_Petrol=('Idle Duration (Diesel/Petrol)', 'sum'),
    Total_Battery_Used=('Total Battery Used', 'sum'),
    Total_Battery_Used_While_Driving=('Total Battery Used while Driving', 'sum'),
    Idle_Duration_Electric=('Idle Duration (Electric)', 'sum')
    )

    aggregated_dataframe.reset_index(inplace=True)
    print(aggregated_dataframe)
    #aggregated_dataframe.to_csv('final_rep31514.csv',  index= False)
    aggregated_dataframe.to_csv('final_rep24995.csv',  index= False) 
    return aggregated_dataframe