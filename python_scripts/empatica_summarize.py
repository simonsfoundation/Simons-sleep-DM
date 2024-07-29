import datetime as dt
import os
import glob 
import pytz
import pandas as pd
from multiprocessing import Pool
import sys
import datetime as dt
import warnings
warnings.filterwarnings("ignore")
import ast
import shutil
import argparse

measure = ['pulse-rate', 'prv','activity-counts', 'sleep','step','respiratory','wearing-detection']

def process_data(row):
    row['Starting date'] = pd.to_datetime(row['Starting date'])
    row['End Date'] = pd.to_datetime(row['End Date'], errors='coerce')
    row['dates'] = pd.date_range(start=row['Starting date'], end=row['End Date']).date.tolist()
    row['dates'] = [date.strftime('%Y-%m-%d') for date in row['dates']]
    row['User ID'] = pd.to_numeric(row['User ID'].replace('U', ''))
    return row


def process_participant_data(participant):

    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DIR = os.environ['OUTPUT_DATA_DIR']
    participant_data_path = f'{DATA_DIR}/empatica/aws_data/1/1/participant_data'
    dates = participant['dates']
    user_id = participant['User ID']
    user = participant['SubjectId']
    spid = participant['SPID']
    tz_str = participant['tz_str']
    agg_emp_all = pd.DataFrame()

    for d in dates:
        if not os.path.isdir(os.path.join(participant_data_path,d)):
            continue

        for foldername in os.listdir(os.path.join(participant_data_path,d)):
            aggregated_data_path = None
            if f'{user}-' in foldername:
                # print(f'{participant_data_path}/{d}/{user}/digital_biomarkers/aggregated_per_minute/','<<<<<<<<<<<<<<')
                try:
                    aggregated_data_path= glob.glob(f'{participant_data_path}/{d}/{user}*/digital_biomarkers/aggregated_per_minute/')[0]
                except:
                    print(f"Error : can't find digital biomarkers for user {user} in {d}")
                    continue
                
            if user_id in [104,150,151]: # correct mistake in empatica usernames lacking U

                for foldername in os.listdir(os.path.join(participant_data_path,d)):
                    if f'{user_id}-' in foldername:
                        try:
                            aggregated_data_path= glob.glob(f'{participant_data_path}/{d}/*{user_id}-3Y*/digital_biomarkers/aggregated_per_minute/')[0]
                            print(foldername)
                        except:
                            print(f"Error : can't find digital biomarkers for user {user} in {d}")
                            continue

            if user_id == 387 and d in ['2024-07-04', '2024-07-05', '2024-07-06','2024-07-07', '2024-07-08']: # correct mistake in user 187/387            
                for foldername in os.listdir(os.path.join(participant_data_path,d)):
                    if '187-' in foldername:
                        try:
                            aggregated_data_path= glob.glob(f'{participant_data_path}/{d}/*U187*/digital_biomarkers/aggregated_per_minute/')[0]
                        except:
                            continue

                            
            if aggregated_data_path and len(os.listdir(aggregated_data_path)) > 1:
                
                wear_path = glob.glob(os.path.join(aggregated_data_path, f'*{measure[6]}*'))[0]
                agg_wear = pd.read_csv(wear_path).iloc[:,3]
                agg_wear['subject'] = user

                try:
                    hrv_path = glob.glob(os.path.join(aggregated_data_path, f'*{measure[1]}*'))[0]
                    agg_hrv = pd.read_csv(hrv_path).iloc[:,3]
                    agg_hrv['subject'] = user
                    rr_path = glob.glob(os.path.join(aggregated_data_path, f'*{measure[5]}*'))[0]
                    agg_rr = pd.read_csv(rr_path).iloc[:,3]
                    agg_rr['subject'] = user

                except Exception as e:
                    agg_hrv = pd.DataFrame()
                    agg_rr = pd.DataFrame()

                try:
                    sleep_path = glob.glob(os.path.join(aggregated_data_path, f'*{measure[3]}*'))[0]
                    agg_sleep = pd.read_csv(sleep_path).iloc[:,3]
                    agg_sleep['subject'] = user

                except Exception as e:
                    agg_sleep = pd.DataFrame()

                hr_path = glob.glob(os.path.join(aggregated_data_path, f'*{measure[0]}*'))[0]
                ac_path = glob.glob(os.path.join(aggregated_data_path, f'*{measure[2]}*'))[0]
                step_path = glob.glob(os.path.join(aggregated_data_path, f'*{measure[4]}*'))[0]


                agg_hr = pd.read_csv(hr_path)
                agg_ac = pd.read_csv(ac_path).iloc[:,3]
                agg_step = pd.read_csv(step_path).iloc[:,3]

                # Add a new column 'subject' with the subject ID
                agg_hr['subject'] = user
                agg_ac['subject'] = user
                agg_step['subject'] = user
                
                df_temp = pd.concat([agg_hr,agg_hrv, agg_ac,agg_sleep,agg_step,agg_rr,agg_wear], axis = 1)
                df_temp.drop('subject',axis = 0, inplace = True)

                agg_emp_all = pd.concat([agg_emp_all,df_temp]).reset_index(drop=True)
    

    if d == sorted(dates)[-1] and not agg_emp_all.empty:


        agg_emp_all.reset_index(inplace = True, drop = True)
        agg_emp_all.sort_index(inplace=True)
        agg_emp_all.drop_duplicates('timestamp_unix', keep = 'first')
        agg_emp_all['timestamp_iso'] = pd.to_datetime(agg_emp_all['timestamp_iso'])
        target_timezone = pytz.timezone(tz_str) 
        
        print(f'{user} {target_timezone}')
        agg_emp_all['timestamp_iso'] = agg_emp_all['timestamp_iso'].dt.tz_convert(target_timezone)

        target_path = f'{OUTPUT_DIR}/{spid}/empatica/summarized_data'

        
        if os.path.isdir(target_path):
            shutil.rmtree(target_path)
    
        if not os.path.isdir(target_path):# or not os.path.isdir(target_path_share):
            os.makedirs(target_path,exist_ok=True)
            #os.makedirs(target_path_share,exist_ok=True)


        #shutil.rmtree(target_path_share)


        if not os.path.isdir(target_path):# or not os.path.isdir(target_path_share):
            os.makedirs(target_path,exist_ok=True)
            #os.makedirs(target_path_share,exist_ok=True)


        if os.path.isdir(target_path):
            # Iterate over all files and directories within 'target_path'
            for item in os.listdir(target_path):
                # Construct full path to item
                item_path = os.path.join(target_path, item)
                # Check if the item is a file and starts with the specified prefix
                if os.path.isfile(item_path) and item.startswith('empatica_measures'):
                    # Delete the file
                    os.remove(item_path)


    if not agg_emp_all.empty and 'timestamp_iso' in agg_emp_all.columns:

        for datei in agg_emp_all['timestamp_iso'].dt.date.unique():

            new_filename = f'empatica_measures_{spid}_{datei}.csv'
            agg_daily = agg_emp_all[agg_emp_all.timestamp_iso.dt.date == datei]

            agg_daily.to_csv(os.path.join(target_path,new_filename), index =False)
            
    else:
        print(f'error {agg_emp_all.columns}')    


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Process files from Empatica")

    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        metavar="WORKERS",
        help="number of parellal tasks",
        default=12,
    )

    parser.add_argument(
        "-d",
        "--data-dir",
        nargs="?",
        type=str,
        metavar="DATA_DIR",
        help="path to data directory",
        default=None,
    )

    parser.add_argument(
        "-o",
        "--output-data-dir",
        nargs="?",
        type=str,
        metavar="OUTPUT_DATA_DIR",
        help="path to output data directory",
        default=None,
    )

    parser.add_argument(
        "-i",
        "--spid",
        nargs="?",
        type=str,
        metavar="SPID",
        help="SPID",
        default=None,
    )

    args = parser.parse_args()

    if args.data_dir is None:
        sys.exit("--data-dir is mandatory")

    if args.output_data_dir is None:
        sys.exit("--output-data-dir is mandatory")

    os.environ['DATA_DIR'] = args.data_dir
    os.environ['OUTPUT_DATA_DIR'] = args.output_data_dir
    os.environ['WORKERS'] = str(args.workers)
    BASE_DIR = os.getcwd()
    os.environ["BASE_DIR"] = BASE_DIR


    data_check = pd.read_csv(f'{BASE_DIR}/Participants and devices - embrace data check.csv')[['User ID','Starting date','End Date']]
    df = pd.read_csv(f'{BASE_DIR}/subjects_ids.csv').drop_duplicates()
    
    dfSpid = pd.read_csv(f'{BASE_DIR}/sleep_subject_ids.csv').drop_duplicates()
    dfSpid['User ID'] = pd.to_numeric(dfSpid['SubjectId'].str.replace('U',''))
    dfSpid['tz_str'] = dfSpid['User ID'].apply(lambda x : df.loc[df.id == x]['tz_str'].to_list()[0])

    data_check = data_check.dropna(subset=['Starting date', 'End Date'], how='all')
    data_check.loc[data_check['End Date'].isna(), 'End Date'] = dt.date.today()
    
    data_check.dropna(subset=['Starting date', 'End Date'], inplace=True)
    data_check =data_check.apply(process_data, axis=1)
    df = dfSpid.merge(data_check, on='User ID', how = 'inner')
    
    if args.spid:
        df = df.loc[df.SPID == args.spid]

    participants = [participant for _, participant in df.iterrows()]

    workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
    results = workers.map(process_participant_data, participants)
    workers.close()
    workers.join()
