import pandas as pd
import pytz
import os
from avro.datafile import DataFileReader
from avro.io import DatumReader
import json
import numpy as np
from multiprocessing import Pool
from datetime import datetime

# DATA_DIR = os.environ['DATA_DIR']
# OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# Function to process and save data for adjusted tz
def process_and_save_adjusted_days(payload):
    spid,tz_str,output_folder, file_type, file1, file2 = payload
    
    try:
        df1 = pd.read_csv(file1).rename(columns = {'timestamp':'time'})
        
        if file2:
            df2 = pd.read_csv(file2).rename(columns = {'timestamp':'time'})
            df_combined = pd.concat([df1, df2], ignore_index=True)
        else:
            df_combined = df1

        df_combined.drop_duplicates(subset='time', keep='first', inplace=True)
        df_combined['date'] = pd.to_datetime(df_combined['time'], unit='s').dt.tz_localize('UTC')
        target_timezone = pytz.timezone(tz_str) 
        
        df_combined['date'] = df_combined['date'].dt.tz_convert(target_timezone)
        df_combined['day'] = df_combined['date'].dt.date

        min_date = df_combined['day'].min()  # Find the minimum date
        
        # Filter out the minimum day
        filtered_df = df_combined[df_combined['day'] != min_date]

        # Save each day's data other than the minimum date
        for datei in filtered_df['day'].unique():
            new_filename = f'empatica_{file_type}_{spid}_{datei}.csv'
            filtered_df[filtered_df['day'] == datei].to_csv(os.path.join(output_folder, new_filename), index=False)
            print(f'{spid} {target_timezone} Adjusted data for {datei} saved.')
    except Exception as e:
        print(f'{spid}::{file_type}, file1 : {file1}, file2 : {file2} : Error : {e}')


def acc_process_date_files(payload):
    date, participant = payload
    dfs = []
    user_id = participant['User ID']
    spid = participant['SPID']
    tz_str = participant['tz_str']
    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    
    participant_data_path = f'{DATA_DIR}/empatica/aws_data/1/1/participant_data/' # path to the folder that contains folder for each date
    tz_temp = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/acc/tz_temp' #output folder
    
    try:
        if (f'empatica_acc_{spid}_{date}.csv') in sorted(os.listdir(tz_temp),reverse=True):         
            print(f'file already processed for {spid} and {date}')
            return

        date_folder = os.path.join(participant_data_path+date) # list folders (for each user) within the date-folde
        for name_folder in sorted(os.listdir(date_folder)):
            if (f'{user_id}-') in name_folder:
                subfolder = os.path.join(date_folder, name_folder, 'raw_data', 'v6')
                if  subfolder != []:
                    # print(f'folder  {date}')
                    if os.path.isdir(subfolder):
                        files = os.listdir(subfolder) #list of avro files
                        files = np.sort(files).tolist() # rearrange files in a chronological manner
                        for ff in files: #loop through files to read and store data
                            avro_file = os.path.join(subfolder,ff)
                            reader = DataFileReader(open(avro_file, "rb"), DatumReader())
                            schema = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
                            data = []
                            for datum in reader:
                                try:
                                    acc = datum["rawData"]["accelerometer"]  # access specific metric
                                    startSeconds = acc["timestampStart"] / 1000000  # convert timestamp to seconds
                                    timeSeconds = list(range(0, len(acc['x'])))
                                    if acc["samplingFrequency"] == 0:
                                        acc["samplingFrequency"] = 64
                                    timeUNIX = [t / acc["samplingFrequency"] + startSeconds for t in timeSeconds]
                                    delta_physical = acc["imuParams"]["physicalMax"] - acc["imuParams"]["physicalMin"]
                                    delta_digital = acc["imuParams"]["digitalMax"] - acc["imuParams"]["digitalMin"]
                                    acc['x'] = [val * delta_physical / delta_digital for val in acc["x"]]
                                    acc['y'] = [val * delta_physical / delta_digital for val in acc["y"]]
                                    acc['z'] = [val * delta_physical / delta_digital for val in acc["z"]]

                                    df_acTot = pd.DataFrame({'time': timeUNIX, 'x': acc['x'], 'y': acc['y'], 'z': acc['z']})
                                    dfs.append(df_acTot)
                                except Exception as e:
                                    print(f'Error : {avro_file} : {e}')
                        if dfs:
                            daily_df = pd.concat(dfs, ignore_index=True)
                            daily_filename = f'{tz_temp}/empatica_acc_{spid}_{date}.csv'
                            daily_df.to_csv(daily_filename)

                            print(f'ACC Data for {spid} & {date} saved.')
                            dfs = []

    except Exception as e:
        print(f'{spid} : {e}')
        return
    pass


def acc_raw_data(participant):

    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    user_id = participant['User ID']
    spid = participant['SPID']
    tz_str = participant['tz_str']
    print(f"Processing acc raw data for {spid}::{user_id} {datetime.now()}")
                                 
    #define path, folders, uzaser SP0114386
    participant_data_path = f'{DATA_DIR}/empatica/aws_data/1/1/participant_data/' # path to the folder that contains folder for each date
    tz_temp = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/acc/tz_temp' #output folder    
    output_folder = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/acc/' #output folder

    os.makedirs(output_folder,exist_ok=True)    
    os.makedirs(tz_temp,exist_ok=True)

    dates = participant['dates']
    
    workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
    results = workers.map(acc_process_date_files, [(date, participant) for date in dates])
    workers.close()
    workers.join()

    day_files = sorted([f for f in os.listdir(tz_temp) if f.startswith('empatica_acc_') and f.endswith('.csv') and f not in os.listdir(output_folder)])

    if len(day_files) > 0:
        # Process each file, considering it and the next one for overlapping data
        workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
        results = workers.map(process_and_save_adjusted_days, [(spid,tz_str,output_folder,'acc',os.path.join(tz_temp, file), os.path.join(tz_temp, day_files[j+1])) for j, file in enumerate(day_files[:-1])])
        workers.close()
        workers.join()

    if len(day_files) > 0:
        # Process the last file separately since it has no next file to combine with
        process_and_save_adjusted_days((spid,tz_str,output_folder,'acc',os.path.join(tz_temp, day_files[-1]),None))

    print(f"Completed Processing acc raw data for {spid}::{user_id} {datetime.now()}")


# eda data
def eda_process_date_files(payload):
    date, participant = payload
    dfs = []
    user_id = participant['User ID']
    spid = participant['SPID']
    tz_str = participant['tz_str']
    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    tz_temp = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/eda/tz_temp' #output folder

    participant_data_path = f'{DATA_DIR}/empatica/aws_data/1/1/participant_data/' # path to the folder that contains folder for each date    

    if (f'empatica_eda_{spid}_{date}.csv') in sorted(os.listdir(tz_temp),reverse=True):         
        print(f'{spid}::{date} file already processed')
        return

    try:
        date_folder = os.path.join(participant_data_path+date) # list folders (for each user) within the date-folde
        for name_folder in os.listdir(date_folder):
            if (f'{user_id}-') in name_folder:
                subfolder = os.path.join(date_folder, name_folder, 'raw_data', 'v6')
                if  subfolder != []:
                    if os.path.isdir(subfolder):
                        files = os.listdir(subfolder) #list of avro files
                        files = np.sort(files).tolist() # rearrange files in a chronological manner
                        for ff in files: #loop through files to read and store data
                            avro_file = os.path.join(subfolder,ff)
                            reader = DataFileReader(open(avro_file, "rb"), DatumReader())
                            schema = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
                            data = []
                            for datum in reader:
                                data = datum
                            reader.close()
                            eda = data['rawData']['eda']
                            startSeconds = eda["timestampStart"] / 1000000
                            timeSeconds = list(range(0,len(eda['values'])))
                            #datetime_timetemp = [datetime.utcfromtimestamp(x) for x in timeUNIXtemp]
                            if eda["samplingFrequency"] == 0:
                                eda["samplingFrequency"] = 4;
                            timeUNIX = [t/eda["samplingFrequency"]+startSeconds for t in timeSeconds]
                            df_eda = pd.DataFrame({'time': timeUNIX, 'eda': eda['values']})

                            if not df_eda.empty:
                                dfs.append(df_eda)

                        if dfs:
                            daily_df = pd.concat(dfs, ignore_index=True)
                            daily_filename = f'{tz_temp}/empatica_eda_{spid}_{date}.csv'
                            daily_df.to_csv(daily_filename)
                            print(f'EDA Data for {spid}::{date} saved.')
                            dfs =[]
                            
    except Exception as e:
        print(f'{spid} : Error : {e}')


def eda_raw_data(participant):
    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    user_id = participant['User ID']
    spid = participant['SPID']
    tz_str = participant['tz_str']
    print(f"Processing eda raw data for {spid}::{user_id} {datetime.now()}")

    #define path, folders, uzaser 
    participant_data_path = f'{DATA_DIR}/empatica/aws_data/1/1/participant_data/' # path to the folder that contains folder for each date
    tz_temp = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/eda/tz_temp' #output folder    
    output_folder = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/eda/' #output folder
    
    os.makedirs(output_folder,exist_ok=True)
    os.makedirs(tz_temp,exist_ok=True)

    dates = participant['dates']
    
    workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
    results = workers.map(eda_process_date_files, [(date, participant) for date in dates])
    workers.close()
    workers.join()
    

    day_files = sorted([f for f in os.listdir(tz_temp) if f.startswith('empatica_eda_') and f.endswith('.csv') and (f not in os.listdir(output_folder))])
    # print(day_files)
    if len(day_files) > 0:
        # Process each file, considering it and the next one for overlapping data
        workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
        results = workers.map(process_and_save_adjusted_days, [(spid,tz_str,output_folder,'eda',os.path.join(tz_temp, file), os.path.join(tz_temp, day_files[j+1])) for j, file in enumerate(day_files[:-1])])
        workers.close()
        workers.join()

    if len(day_files) > 0:
        # Process the last file separately since it has no next file to combine with
        process_and_save_adjusted_days((spid,tz_str,output_folder,'eda',os.path.join(tz_temp, day_files[-1]),None))

    print(f"Completed Processing eda raw data for {spid}::{user_id} {datetime.now()}")



def bvp_process_date_files(payload):
    date, participant = payload
    dfs = []
    user_id = participant['User ID']
    spid = participant['SPID']
    tz_str = participant['tz_str']
    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    tz_temp = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/bvp/tz_temp' #output folder
    participant_data_path = f'{DATA_DIR}/empatica/aws_data/1/1/participant_data/' # path to the folder that contains folder for each date    

    try:
        if (f'empatica_bvp_{spid}_{date}.csv') in sorted(os.listdir(tz_temp),reverse=True):         
            print(f'{spid}::{date} file already processed')
            return

        # elif (f'empatica_bvp_{subject_id[i]}_{date}.csv') not in sorted(os.listdir(tz_temp),reverse=True) and ((f'empatica_bvp_{subject_id[i]}_{date}.csv') not in sorted(os.listdir(output_folder),reverse=True)):   
        date_folder = os.path.join(participant_data_path+date) # list folders (for each user) within the date-folde
        for name_folder in os.listdir(date_folder):
            if (f'{user_id}-') in name_folder:
                subfolder = os.path.join(date_folder, name_folder, 'raw_data', 'v6')
                if  subfolder != []:
                    if os.path.isdir(subfolder):
                        files = os.listdir(subfolder) #list of avro files
                        files = np.sort(files).tolist() # rearrange files in a chronological manner
                        for ff in files: #loop through files to read and store data
                            avro_file = os.path.join(subfolder,ff)
                            reader = DataFileReader(open(avro_file, "rb"), DatumReader())
                            schema = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
                            data = []
                            for datum in reader:
                                data = datum
                            reader.close()

                            bvp = data["rawData"]["bvp"] #access specific metric 
                            if len(data["rawData"]["bvp"]['values']) > 0:

                                startSeconds = bvp["timestampStart"] / 1000000 # convert timestamp to seconds
                                timeSeconds = list(range(0,len(bvp['values'])))
                                timeUNIX = [t/bvp["samplingFrequency"]+startSeconds for t in timeSeconds]
                                datetime_time = [datetime.utcfromtimestamp(x) for x in timeUNIX]

                                df_bvpTot = pd.DataFrame({'time': timeUNIX, 'bvp': bvp['values']})

                                if not df_bvpTot.empty:
                                    dfs.append(df_bvpTot)

                        if dfs:
                            daily_df = pd.concat(dfs, ignore_index=True)
                            daily_filename = f'{tz_temp}/empatica_bvp_{spid}_{date}.csv'
                            daily_df.to_csv(daily_filename)
                            print(f'{spid} BVP Data for {date} saved.')
                            dfs =[]

    except Exception as e:
        print(f'{spid} : Error : {e}')
        return


def bvp_raw_data(participant):
    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    user_id = participant['User ID']
    spid = participant['SPID']
    tz_str = participant['tz_str']
    print(f"Processing bvp raw data for {spid}::{user_id} {datetime.now()}")

    #define path, folders, uzaser 
    participant_data_path = f'{DATA_DIR}/empatica/aws_data/1/1/participant_data/' # path to the folder that contains folder for each date
    tz_temp = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/bvp/tz_temp' #output folder    
    output_folder = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/bvp/' #output folder

    os.makedirs(output_folder,exist_ok=True)        
    os.makedirs(tz_temp,exist_ok=True)


    # bvp data 

    dfs = []
    dates = participant['dates']
    
    workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
    results = workers.map(bvp_process_date_files, [(date, participant) for date in dates])
    workers.close()
    workers.join()

    day_files = sorted([f for f in os.listdir(tz_temp) if f.startswith('empatica_bvp_') and f.endswith('.csv') and (f not in os.listdir(output_folder))])    
    if len(day_files) > 0:
        # Process each file, considering it and the next one for overlapping data
        workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
        results = workers.map(process_and_save_adjusted_days, [(spid,tz_str,output_folder,'bvp',os.path.join(tz_temp, file), os.path.join(tz_temp, day_files[j+1])) for j, file in enumerate(day_files[:-1])])
        workers.close()
        workers.join()
        
    if len(day_files) > 0:
        # Process the last file separately since it has no next file to combine with
        process_and_save_adjusted_days((spid,tz_str,output_folder,'bvp',os.path.join(tz_temp, day_files[-1]),None))
    
    print(f"Completed Processing bvp raw data for {spid}::{user_id} {datetime.now()}")


def temprature_process_date_files(payload):
    date, participant = payload
    dfs = []
    user_id = participant['User ID']
    spid = participant['SPID']
    tz_str = participant['tz_str']
    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    tz_temp = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/temp/tz_temp' #output folder
    participant_data_path = f'{DATA_DIR}/empatica/aws_data/1/1/participant_data/' # path to the folder that contains folder for each date    

    try:
        date_folder = os.path.join(participant_data_path+date) # list folders (for each user) within the date-folde
        for name_folder in os.listdir(date_folder):
            if (f'{user_id}-') in name_folder:
                subfolder = os.path.join(date_folder, name_folder, 'raw_data', 'v6')
                if  subfolder != []:
                    if os.path.isdir(subfolder):
                        files = os.listdir(subfolder) #list of avro files
                        files = np.sort(files).tolist() # rearrange files in a chronological manner
                        for ff in files: #loop through files to read and store data
                            avro_file = os.path.join(subfolder,ff)
                            reader = DataFileReader(open(avro_file, "rb"), DatumReader())
                            schema = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
                            data = []
                            for datum in reader:
                                data = datum
                            reader.close()

                            temp = data["rawData"]["temperature"] #access specific metric 
                            if len(data["rawData"]["temperature"]['values']) > 0:

                                startSeconds = temp["timestampStart"] / 1000000 # convert timestamp to seconds
                                timeSeconds = list(range(0,len(temp['values'])))
                                timeUNIX = [t/temp["samplingFrequency"]+startSeconds for t in timeSeconds]
                                datetime_time = [datetime.utcfromtimestamp(x) for x in timeUNIX]

                                df_tempTot = pd.DataFrame({'time': timeUNIX, 'temp': temp['values']})

                                if not df_tempTot.empty:
                                    dfs.append(df_tempTot)

                        if dfs:

                            daily_df = pd.concat(dfs, ignore_index=True)
                            daily_filename = f'{tz_temp}/empatica_temp_{spid}_{date}.csv'
                            daily_df.to_csv(daily_filename)
                            print(f'{spid} Temprature Data for {date} saved.')
                            dfs =[]
    except Exception as e:
        print(f'{spid} : Error : {e}')
        return



def temprature_raw_data(participant):
    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    user_id = participant['User ID']
    spid = participant['SPID']
    tz_str = participant['tz_str']
    print(f"Processing temprature raw data for {spid}::{user_id} {datetime.now()}")

    #define path, folders, uzaser 
    participant_data_path = f'{DATA_DIR}/empatica/aws_data/1/1/participant_data/' # path to the folder that contains folder for each date
    tz_temp = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/temp/tz_temp' #output folder    
    output_folder = f'{OUTPUT_DATA_DIR}/{spid}/empatica/raw_data/temp/' #output folder

    os.makedirs(output_folder,exist_ok=True)
    os.makedirs(tz_temp,exist_ok=True)

    dfs = []
    dates = participant['dates']
    
    workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
    results = workers.map(temprature_process_date_files, [(date, participant) for date in dates])
    workers.close()
    workers.join()

    day_files = sorted([f for f in os.listdir(tz_temp) if f.startswith('empatica_temp_') and f.endswith('.csv') and (f not in os.listdir(output_folder))])

    if len(day_files) > 0:
        # Process each file, considering it and the next one for overlapping data
        workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
        results = workers.map(process_and_save_adjusted_days, [(spid,tz_str,output_folder,'temp',os.path.join(tz_temp, file), os.path.join(tz_temp, day_files[j+1])) for j, file in enumerate(day_files[:-1])])
        workers.close()
        workers.join()

        
    if len(day_files) > 0:
        # Process the last file separately since it has no next file to combine with
        process_and_save_adjusted_days((spid,tz_str,output_folder,'temp',os.path.join(tz_temp, day_files[-1]),None))
    
    print(f"Completed Processing temprature raw data for {spid}::{user_id} {datetime.now()}")