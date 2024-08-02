import datetime as dt
import os
import sys
import multiprocessing
import pandas as pd
import argparse
from empatica_helper import acc_raw_data, bvp_raw_data, temprature_raw_data, eda_raw_data
import os
from functools import partial

"""
python python_scripts/empatica_rawdata_process.py -d /mnt/home/nshah/ceph/Sleep_study-neelay/SubjectsData/data_share_test -o /mnt/home/nshah/ceph/Sleep_study-neelay/SubjectsData/export_data --workers 16  --spid SP0252854
"""
BASE_DIR = __file__.split('/python_scripts/')[0]
os.environ["BASE_DIR"] = BASE_DIR
    

def process_data(row):
    row['Starting date'] = pd.to_datetime(row['Starting date'])
    row['End Date'] = pd.to_datetime(row['End Date'], errors='coerce')
    row['dates'] = pd.date_range(start=row['Starting date'], end=row['End Date']).date.tolist()
    row['dates'] = [date.strftime('%Y-%m-%d') for date in row['dates']]
    row['User ID'] = pd.to_numeric(row['User ID'].replace('U', ''))
    return row


def call_censor_processing_function(function, participant):
    # print(function, participant,'>>>>>>>>>>')
    function(participant)
    return



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
    # df.to_csv(f"{BASE_DIR}/temp.csv", index=False)

    if args.spid:
        df = df.loc[df['SPID'] == args.spid]

    processes = []
    for _, participant in df.iterrows():
        print(f"Processing empatica data for {participant.SPID} : {dt.datetime.now()}")
        censor_functions = [acc_raw_data, bvp_raw_data, temprature_raw_data, eda_raw_data]
    
        for function in censor_functions:
            p = multiprocessing.Process(target=call_censor_processing_function, args=(function,participant,))
            processes.append((p, participant.SPID, function.__name__ ))
            p.start()

    # Wait for all processes to finish
    for p, spid, function in processes:
        p.join()
        print(f"Completed {function} for {spid} : {dt.datetime.now()}")