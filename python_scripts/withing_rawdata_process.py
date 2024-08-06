import os
import argparse
import sys
import pandas as pd
from datetime import datetime, timedelta
import pytz
import numpy as np
import pandas as pd
import zipfile
import glob
from multiprocessing import Pool

"""
ython python_scripts/withing_rawdata_process.py -d /var/www/Simons-sleep-DM/Data/data_share_test -o /var/www/Simons-sleep-DM/Data/export

"""
BASE_DIR = __file__.split('/python_scripts/')[0]
os.environ["BASE_DIR"] = BASE_DIR
    


def process_data(row):
    row['Starting date'] = pd.to_datetime(row['Starting date'])
    row['End Date'] = pd.to_datetime(row['End Date'], errors='coerce')
    row['dates'] = pd.date_range(start=row['Starting date'], end=row['End Date']).date.tolist()
    row['dates'] = [date.strftime('%Y-%m-%d') for date in row['dates']]
    row['User ID'] = pd.to_numeric(row['User ID'].upper().replace('U', ''))
    return row

# subject_id,tz_str,data_path,output_path, dates
def process_withings_shared_data(participant):
    DATA_DIR = os.environ['DATA_DIR']
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
    user_id = participant['SubjectId']
    spid = participant['SPID']
    tz_str = participant['tz_str']
    dates = participant['dates']

    print(f"Processing process_withings_shared_data for {spid}::{user_id} {datetime.now()}")
                                 
    #define path, folders, uzaser 
    data_path = f'{DATA_DIR}/withings' # path to the folder that contains folder for each date
    
    try:
        input_folder_wit = glob.glob(f'{data_path}/**/*{user_id.lower()}*/', recursive=True)[0]
    except Exception as e:
        print(f"Error : {spid}::{user_id} : {e}")
        return None
    data_folders = sorted([folder for folder in os.listdir(input_folder_wit) if folder.isdigit()], key=int, reverse=True)

    output_share_dir = f"{OUTPUT_DATA_DIR}/{spid}/withings/"
    if not os.path.isdir(output_share_dir):
        os.makedirs(output_share_dir, exist_ok=True)

    input_zip = None

    # Iterate over the folders from newest to oldest
    for data_folder in data_folders:
        input_folder_wit_current = os.path.join(input_folder_wit, data_folder)
        potential_zip_files = glob.glob(f'{input_folder_wit_current}/*data*.zip', recursive=True)

        for zip_path in potential_zip_files:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                if any("raw_bed_Pressure.csv" in file for file in zip_ref.namelist()):
                    input_zip = zip_path  # Take the first .zip file found
        break



    if input_zip:
        # Open the zip file
        # with zipfile.ZipFile(input_zip, 'r') as zip_ref:
        #     # Extract all the contents into the directory
        #     zip_ref.extractall(input_folder_wit)
        #     print(os.listdir(input_folder_wit),'>>>>>>>>>>>>>>.')
        #     return
            # for file in os.listdir(input_folder_wit):
        for file in ['raw_bed_Pressure.csv','raw_bed_HR RMS SD.csv',
                        'raw_bed_HR SD NN.csv','raw_bed_hr.csv','sleep.csv', 
                        'raw_bed_respiratory-rate.csv']:
                        
            with zipfile.ZipFile(input_zip, 'r') as zip_ref:
                
                file_type = file.replace('raw_bed_','').replace('.csv','')
                if file == 'sleep.csv':
                    file_name = f'withings_report_{spid}.csv'
                else:
                    file_name = f'withings_{file_type}_{spid}.csv'
                
                with zip_ref.open(file) as zf:
                    dfi = pd.read_csv(zf)
                    
                    if dfi.empty:
                        continue
                    if 'start' in dfi.columns:
                        date_columns = ['start']

                    elif 'date' in dfi.columns:
                        date_columns = ['date']

                    elif 'Date' in dfi.columns:
                        date_columns = ['Date']

                    elif 'from' in dfi.columns:
                        date_columns = ['from','to']
                        dfi['from'] = pd.to_datetime(dfi['from'],utc=True)
                        dfi['to'] = pd.to_datetime(dfi['to'],utc=True)

                        target_timezone = pytz.timezone(tz_str) 

                        dfi['from'] = dfi['from'].dt.tz_convert(target_timezone)
                        dfi['to'] = dfi['to'].dt.tz_convert(target_timezone)

                        dfi.to_csv(os.path.join(output_share_dir,file_name), index = False)
                        continue

                    else:
                        dfi.to_csv(os.path.join(output_share_dir,file_name), index = False)
                        continue

                target_timezone = pytz.timezone(tz_str) 
                for col in date_columns: 
                    dfi[col] = pd.to_datetime(dfi[col],utc=True)
                    dfi[col] = dfi[col].dt.tz_convert(target_timezone)                        
                    dfi['date_corrected'] = dfi[col].apply(lambda x: x if (pd.notna(x) and x.strftime('%Y-%m-%d') in dates) or
                                                            ((x + timedelta(days=1)).strftime('%Y-%m-%d') in dates) else pd.NaT)

                dfi = dfi.sort_values(date_columns)
                dfi = dfi.drop_duplicates()
                dfi.to_csv(os.path.join(output_share_dir,file_name), index = False)

    # for file in os.listdir(output_share_dir):
    #         if file.startswith('withings'):
    #             os.remove(os.path.join(output_share_dir,file))
    #     print(user)

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

    DATA_DIR = os.environ['DATA_DIR'] = args.data_dir
    OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR'] = args.output_data_dir
    os.environ['WORKERS'] = str(args.workers)

    source_dir = f"{DATA_DIR}/dreem/aws_data/"
    dest_base_dir = f'{OUTPUT_DATA_DIR}'


    dfSpid = pd.read_csv(f'{BASE_DIR}/sleep_subject_ids.csv').drop_duplicates()
    df = pd.read_csv(f'{BASE_DIR}/subjects_ids.csv').drop_duplicates()
    dfSpid['User ID'] = pd.to_numeric(dfSpid['SubjectId'].str.replace('U',''))
    dfSpid['tz_str'] = dfSpid['User ID'].apply(lambda x : df.loc[df.id == x]['tz_str'].to_list()[0])
    
    data_check = pd.read_csv(f'{BASE_DIR}/Participants and devices - withings data check.csv')[['User ID','Starting date','End Date']]
    data_check = data_check.dropna(subset=['Starting date', 'End Date'], how='all')
    data_check.loc[data_check['End Date'].isna(), 'End Date'] = datetime.today().strftime('%Y-%m-%d')
    data_check.dropna(subset=['Starting date', 'End Date'], inplace=True)
    data_check =data_check.apply(process_data, axis=1)

    df = dfSpid.merge(data_check, on='User ID', how = 'inner')
    if args.spid:
        df = df.loc[df.SPID == args.spid]
    
    participants = list(df.T.to_dict().values())
    print(f"Processing process_withings_shared_data for {len(participants)} participants {datetime.now()}")
    workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
    results = workers.map(process_withings_shared_data, participants)
    workers.close()
    workers.join()
    print(f"Completed Processing process_withings_shared_data for {len(participants)} participants {datetime.now()}")

        
        

