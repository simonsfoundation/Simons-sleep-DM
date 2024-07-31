import pandas as pd
import pytz
import os, sys
from avro.datafile import DataFileReader
from avro.io import DatumReader
import json, re
import numpy as np
from multiprocessing import Pool
from datetime import datetime, timedelta
from dateutil import parser as dt_parser
import shutil
import mne
from pathlib import Path


def adjust_date_for_recording_time(file_date_str, file_time_str):
        file_datetime = datetime.strptime(f"{file_date_str}T{file_time_str}", "%Y-%m-%dT%H:%M:%S%z")

        # Adjusting the date if the recording started before midnight.
        if file_datetime.hour < 15:  # Assuming recordings start late in the evening or just after midnight.
            adjusted_datetime = file_datetime - timedelta(days=1)
        else:
            adjusted_datetime = file_datetime
        return adjusted_datetime.date()


def find_participant_id(mapping, simons_sleep_id, file_date): 
    for entry in mapping.get(simons_sleep_id, []):
        participant_id, start_date, end_date = entry
        start_date_parsed = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").date()
        end_date_parsed = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").date()
        if start_date_parsed <= file_date <= end_date_parsed:
            return participant_id
    return None


def move_files(file_users_map, participants, source_dir):
    for file_type,dir_type in zip(['edf/Prod','hypnogram/Prod','endpoints/Prod'],['edf','hypno','dreem_reports']):
        file_dir = source_dir+file_type
        for filename in sorted(os.listdir(file_dir)):
            if not filename.endswith(('.edf', '_hypnogram.txt', '_endpoints.csv')):
                continue
            # Extract simons_sleep ID and recording start time from the filename.
            simons_sleep_id, timestamp = filename.split('@')[0], filename.split('_')[2]
            file_date_str, file_time_str = timestamp.split('T')
            file_time_str = file_time_str.split('.')[0]
            file_date = adjust_date_for_recording_time(file_date_str, file_time_str)
            users = file_users_map.get(simons_sleep_id)
            
            participant = None
            for user in users:
                p = participants.get(user)
                if p:
                    if (p['start_date'] <= file_date <= p['end_date']):
                        if participant is not None:
                            raise Exception(f"multiple users for file {filename}: {participant['user_id']}, {p['user_ud']}")
                        participant = p
                else:
                    print(f"Error : can't find  user data for {user}")

            if participant is None:
                print("Error : can't find participant for file : {file}")
            else:
                dest_dir = f"{os.environ['OUTPUT_DATA_DIR']}/{participant['spid']}/dreem/{dir_type}/" 
                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)
                if filename not in os.listdir(dest_dir):
                    shutil.copy(os.path.join(file_dir, filename), os.path.join(dest_dir, filename))
                    


def dreem_process_edf(participant):
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    user_id = participant['user_id']
    spid = participant['spid']
    tz_str = participant['tz_str']

    output_folder = f'{OUTPUT_DATA_DIR}/{spid}/dreem'
    
    edf_out = output_folder + '/edf/'
    csv_dir = output_folder + '/hypno/'

    print(f"Processing dreem_process_edf {spid}::{user_id} {datetime.now()}")

    # date_list = [re.search(r'\d{4}-\d{2}-\d{2}', file).group() for file in os.listdir(edf_out) if file.endswith(".edf") and not file.startswith("eeg") ]
    
    # Check and create directories if they don't exist
    for directory in [edf_out]:
        if not os.path.isdir(directory):
            os.makedirs(directory)
            
    # # Get list of EDF files in the directory
    path_edfs = sorted([file for file in os.listdir(edf_out) if file.endswith(".edf") and not file.startswith("dreem_eeg_") ],reverse=True)
    path_edf = None  # Initialize path_edf outside the loop
    
    for pathi in path_edfs:
        try:
            match = re.search(r'(\d{4}-\d{2}-\d{2})T', pathi)
            if match:
                file_date = re.search(r'\d{4}-\d{2}-\d{2}', pathi).group()
                if not file_date:
                    file_date = re.search(r'\d{4}:\d{2}:\d{2}', pathi).group()

        except Exception as e:
            print(f"Error : processing data for {spid} {pathi}: {e}")
            continue


        path_edf = pathi
        # file_date = datetime.strptime(file_date, '%Y-%m-%d').date()
        if path_edf:
            try:
                filename = path_edf
                datetime_str = filename.split('@')[1].split('.')[1]
                # print(datetime_str)
                datetime_str= datetime_str.split('_')[1]
                
                # Parse the datetime string into a datetime object, including the timezone
                datetime_obj = dt_parser.parse(datetime_str)
                # Define the target timezone
                target_tz = pytz.timezone(tz_str)
                utc_fix = False
                
                if datetime_obj.tzinfo != target_tz:
                    utc_fix = True
                    datetime_obj = datetime_obj.astimezone(target_tz)
                    print(f"Time adjusted to: {target_tz}")

                file_time = datetime_obj.time()
                file_tz = datetime_obj.tzinfo
                
                # Check if the time adjustment crosses over to the next day
                if file_time > datetime.strptime('19:00:00', '%H:%M:%S').time():
                    datetime_obj += timedelta(days=1)  # Add one day

                file_date = datetime_obj.date()

                path_edf = os.path.join(edf_out, path_edf)
                # Check the size of the file
                file_size = os.path.getsize(path_edf)  # Get file size in bytes
                size_limit = 10 * 1024 * 1024  # 10MB in bytes ~ 1 hour of recording

                if file_size < size_limit:
                    print(f"{spid}:: File {path_edf} is smaller than 20MB and will be deleted.")
                    os.remove(path_edf)  # Delete the file
                    continue
                else:
                    print(f'{spid}::processing {path_edf}')
                    # Proceed with loading the EDF file as it meets the size criteria
                    EEG = mne.io.read_raw_edf(path_edf, preload=True)

                # save new edf filename 
                new_edf_filename = f"dreem_eeg_{spid}_{file_date}.edf"

                #update harmonized data
                new_edf_path = os.path.join(edf_out, new_edf_filename)

                print(f"{spid}:: EDF file {new_edf_path} was recorded on {file_date.strftime('%Y-%m-%d')}")

                # stage file
                stagefile = f"dreem_hypno_{spid}_{file_date.strftime('%Y-%m-%d')}.csv"

                if not Path(f'{csv_dir}/{stagefile}').is_file():
                    print(f"Error : {spid}::{user_id} Stage file not found in {csv_dir} :", stagefile)

                # path_stages = os.path.join(csv_dir, stagefile)
                # eeg_sleep_instance = EEGSleep('Dreem')
                # epochs, croppedData = eeg_sleep_instance.preprocess_eeg_data(path_edf, path_stages, preload=True, l_freq=0.75, h_freq=20)

                # Save EEG data in EDF format
                os.rename(path_edf, new_edf_path)
                shutil.copy(new_edf_path, new_edf_path)

                print(f'saved files {spid}::{file_date}')
            except Exception as e:
                print(f"Error : {spid} processing data for subject_id {pathi}: {e}")
                continue
        else:
            print(f'no files from {file_date} for {spid}')

    print(f"Completed Processing dreem_process_edf {spid}::{user_id} {datetime.now()}")
        


def dreem_process_hypno(participant):

    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    user_id = participant['user_id']
    spid = participant['spid']
    tz_str = participant['tz_str']

    directory = f'{OUTPUT_DATA_DIR}/{spid}/dreem/hypno/'
    output_folder = f'{OUTPUT_DATA_DIR}/{spid}/dreem/hypno/'
    print(f"Processing dreem_process_hypno {spid}::{user_id} {datetime.now()}")
    if not os.path.isdir(directory):
        os.makedirs(directory)
    
    if not os.path.isdir(output_folder):
        os.makedirs(output_folder)

    
    #check folder
    os.chdir(directory)
    
    # remove old processed files 
    for file in directory:
        if file.startswith('dreem_'):
            os.remove(os.path.join(directory,file))
    
    
    date_list=[]
    date = None
    if len(os.listdir(directory)) > 0:

        for filename in os.listdir(directory):
            if 'hypnogram' in filename:
                
                try:
                
                    datetime_str = filename.split('@')[1].split('.')[1]
                    datetime_str= datetime_str.split('_')[1]
    
    
                    # Parse the datetime string into a datetime object, including the timezone
                    if '[05-00]' in datetime_str:
                        print(filename)
                        datetime_str.replace("[05-00]", "-05:00")
    
                    datetime_obj = dt_parser.parse(datetime_str)
                    # Define the target timezone
                    target_tz = pytz.timezone(tz_str)
                    utc_fix = False
    
                    if datetime_obj.tzinfo != target_tz:
                        utc_fix = True
                        datetime_obj = datetime_obj.astimezone(target_tz)
                        # print(f"Time adjusted to: {target_tz}")
    
                    file_time = datetime_obj.time()
                    file_tz = datetime_obj.tzinfo
    
    
                    # Check if the time adjustment crosses over to the next day
                    if file_time > datetime.strptime('19:00:00', '%H:%M:%S').time():
                        datetime_obj += timedelta(days=1)  # Add one day
    
                    file_date = datetime_obj.date()
    
                    if filename.endswith("hypnogram.txt"):
    
                        with open(f"{directory}/{filename}") as file:
                            lines = file.readlines()
                            # Find the line with "idorer Time" and extract the date
                            for i, line in enumerate(lines):
                                if "Scorer Time" in line:
    
                                    date = file_date
                                    time_dt = file_time
    
                                    date_list.append(date)
    
                                    new_filename = f"dreem_hypno_{spid}_{date}.txt"
                                    # Delete all lines before the table
                                    lines = lines[(i+2):]
                                    break
                            # Write the remaining lines to a new file with the new name
                            if date:
                                with open(f"{directory}/{new_filename}", "w") as new_file:
                                    new_file.writelines(lines)
    
                                # locals()[f'dreem_{spid}_{date}'] = pd.read_csv(f"{directory}/{new_filename}",sep= "\t")
                                # df = locals()[f'dreem_{spid}_{date}']
                                df = pd.read_csv(f"{directory}/{new_filename}",sep= "\t")
    
                                df.replace({'SLEEP-S0':'0','SLEEP-S1':'1','SLEEP-S2':'2','SLEEP-S3':'3','SLEEP-REM':'4','SLEEP-MT':None},inplace=True)
                                # df.rename(columns = {'Time [hh:mm:ss]':'time'},inplace=True)
    
                                df['time'] = pd.to_datetime(date.strftime('%Y-%m-%d') + ' ' + df['Time [hh:mm:ss]'], format='%Y-%m-%d %H:%M:%S')
                
                                # Adjust the time for entries after 19:00
                                df['time'] = df['time'].apply(lambda x: x - timedelta(days=1) if x.hour >= 18 else x)
                                df['time'] = df['time'].dt.tz_localize(file_tz.zone)
    
                                df.drop('Time [hh:mm:ss]', axis=1, inplace=True)
                                
                                reorder = ['Sleep Stage','time','Event','Duration[s]']
    
                                df = df[reorder]
    
                                os.remove(directory+filename) # delete the old foramt files
                                
                                new_filename =  f"dreem_hypno_{spid}_{date}.csv"
                                #df.to_csv(f'{output_folder}/dreem_{subject_id[j]}_{date}.txt', sep='\t')
                                if new_filename in os.listdir(output_folder):
                                    print(f'{spid} ::: {new_filename}')
                                # print(os.path.join(output_folder,new_filename),'>>>>>>>>>')
                                df.to_csv(os.path.join(output_folder,new_filename), index=False)
                
                except Exception as e:
                    print(f'Error : {spid} {e}')
                    continue

    print(f"Completed Processing dreem_process_hypno {spid}::{user_id} {datetime.now()}")