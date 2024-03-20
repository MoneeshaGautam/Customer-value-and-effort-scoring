import pandas as pd
from datetime import datetime,timedelta
from extractor import *
from manipulation import *
import numpy as np



def manipulation():
    
    
#     final_columns=[
#         'Vehicle ID',#done
#         'Vehicle Reg',#not required
#         'Date ',#done
#         'Fuel type',#pending
#         'Lowest Fuel Level',#done
#         'Total Distance Traveled in KMs',#done
#         'Total Time Inginition On',#done , need to test
#         'Total Hours Travelled(Electric)',#done
#         'Total Hours Travelled(Petrol/Diesel)',#done
#         'Total Fuel Used( in Litres)',# done
#         'Total Fuel Available(%)',#done
#         'Total Battery Used',#done 'Total Discharged Energy'
#         'Total Battery Used while driving',#done
#         'Total Available Battery %',#done
#         'Total Eco Mode Used In Total Driving(in %)',
#         'Electric Motor Failure (in %)',#
#         'Fuels Filled(count)',#working pending
#         'Idle Duration (Diesel/Petrol)',#done
#         'Idle Duration (Electric)',#done
#         'Average RPM',#done
#         'Maximum RPM',#done 
#         'Distance For Next Service (in Meters)',#svds no data found
#         'Total Time On Ev',#ratio ev/petrol
#         'Total Engine Hours',#done
#         'Total Regenerated Energy',
#         'Brake Regen Distance',#no data found
#         'Maximum Speed',#done
#    ]
    #final_dataframe=pd.DataFrame(columns=final_columns)

    # veh_ids = data["veid"].unique()
    # #veh_ids = [for veh_id in veh_ids]

 #for veid,group_data in grouped_data:
      

        final_dataframe=pd.DataFrame()
        final_dataframe['Vehicle ID']=vehicle_id()
        print(final_dataframe['Vehicle ID'])
        #final_dataframe['Date']=travel_date()
        final_dataframe['Lowest Fuel Level'] = round(lowest_fuel_level(), 2).fillna(0)
        final_dataframe['Lowest Fuel Level'] = round(lowest_fuel_level())
        final_dataframe['KM Travelled']=round(KM_Travelled(),2).fillna(0)
        final_dataframe['Total Time Inginition On']=total_time_ignition_on()#.fillna(0)# wrong output
        final_dataframe['Total Hours Travelled(Petrol/Diesel)']=total_hr_travelled()# wrong output
        final_dataframe['Total Hours Travelled(Electric)']=total_hours_electric()# wrong output
        final_dataframe['Total Fuel Used( in Litres)']=round(total_fuel_used(),2).fillna(0)
        final_dataframe['Total Fuel Available(%)'] = round(avbl_fuel(), 2).fillna(0)
        final_dataframe['Total Battery Used']=round(total_battery_used(),2).fillna(0)
        final_dataframe['Total Battery Used while Driving']=round(total_battery_used_driving(),2).fillna(0)
        final_dataframe['Total Battery Available(%)'] = round(avbl_Battery(), 2).fillna(0)
        #final_dataframe['Fuels Filled(count)'] = times_of_fuel_filled(), # wrong output
        final_dataframe['Idle Duration (Electric)']=round(total_idle_duration(),2).fillna(0)
        final_dataframe['Idle Duration (Diesel/Petrol)']=round(total_idle_duration(),2).fillna(0)
        final_dataframe['Average RPM'] = round(avg_rpm(), 2).fillna(0)
        final_dataframe['Maximum RPM']=round(max_rpm(),2).fillna(0)
        final_dataframe['Total Time On Ev']=Total_Time_El()# wrong output
        final_dataframe['Total Engine Hours']=round(total_engine_hours(),2).fillna(0)
        final_dataframe['Maximum Speed']=round(max_speed(),2).fillna(0)

        #----------------Agregated_dataframe---------------


        aggregated_dataframe = final_dataframe.groupby(['Vehicle ID', 'Date']).agg(
        Vehicle_ID=('Vehicle ID', 'first'),
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
        #Fuels_Filled_Count=('Fuels Filled(count)', 'sum'),
        Total_Engine_Hours=('Total Engine Hours', 'sum'),
        Idle_Duration_Diesel_Petrol=('Idle Duration (Diesel/Petrol)', 'sum'),
        Total_Battery_Used=('Total Battery Used', 'sum'),
        Total_Battery_Used_While_Driving=('Total Battery Used while Driving', 'sum'),
        Idle_Duration_Electric=('Idle Duration (Electric)', 'sum')
        )

        aggregated_dataframe.reset_index(inplace=True)
        print(aggregated_dataframe)
        aggregated_dataframe.to_csv('final_rep24995.csv',  index= False) 
        return aggregated_dataframe

        # final_dataframe.reset_index(inplace=True)
        # print(final_dataframe)
        # final_dataframe.to_csv('final_rep24995.csv',  index= False) 
        # return final_dataframe




