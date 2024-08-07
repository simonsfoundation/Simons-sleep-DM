import os
import argparse
import sys
import pandas as pd
from dreem_helper import *

"""
python python_scripts/dreem_rawdata_process.py -d /var/www/Simons-sleep-DM/Data/data_share_test -o /var/www/Simons-sleep-DM/Data/export --spid SP0489864

"""
BASE_DIR = __file__.split('/python_scripts/')[0]
os.environ["BASE_DIR"] = BASE_DIR

     
def get_dreem_allocation_mapping():
    BASE_DIR = os.environ['BASE_DIR']
    data_check = pd.read_csv(f'{BASE_DIR}/Participants and devices - Dreem data check.csv')[['User ID','starting date','ending date','Dreem user id']]
    data_check = data_check.dropna(subset=['starting date', 'ending date'], how='all')


    data_check.loc[data_check['ending date'].isna(), 'ending date'] = datetime.today().date
    data_check['starting date'] = pd.to_datetime(data_check['starting date'])
    data_check['ending date'] = pd.to_datetime(data_check['ending date'], errors='coerce')
    data_check.dropna(subset=['starting date', 'ending date'], inplace=True)

    # Initialize dictionary

    mapping = {}
    for index, row in data_check.iterrows():

        data_tuple = (row['User ID'], str(row['starting date']), str(row['ending date']))
        dreem_user = row['Dreem user id'].split('@')[0]

        # If the Dreem user id is already a key in the mapping, append the tuple
        if dreem_user in mapping:
            dreem_user = row['Dreem user id'].split('@')[0]
            mapping[dreem_user].append(data_tuple)
            # Correct record on wrong sibling
            if dreem_user == 'simons_sleep29':
                mapping[dreem_user].append(('U325', '2024-01-27 00:00:00', '2024-01-30 00:00:00'))
        else:
            # Otherwise, create a new list with the tuple
            mapping[dreem_user] = [data_tuple]
    
    return mapping







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

    parser.add_argument(
        "-s",
        "--skip-move-files",
        nargs="?",
        type=str,
        metavar="SKIP",
        help="skip move_files",
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

    # First extract error in filenames and repalce with correct filenames in the aws dir
    unique_cases = pd.read_csv(f'{BASE_DIR}/Participants and devices - Dreem corrections.csv')
    user_wrong = unique_cases['Original Dreem Username']
    user_fixed = unique_cases['Correct Dreem Username']
    unique_cases['corrected_names'] = 0

    for i in range(len(unique_cases['Recorded Dreem Filename'])):
        part1 = unique_cases['Recorded Dreem Filename'][i].split('T')[0]
        part2 = unique_cases['Recorded Dreem Filename'][i].split('T')[1].replace('-',':')
        unique_cases['Recorded Dreem Filename'][i] = part1+'T'+part2[:8]
        unique_cases['corrected_names'][i] = (unique_cases['Recorded Dreem Filename'][i].replace(user_wrong[i],user_fixed[i]))


    for dir_type in ['edf/Prod','hypnogram/Prod','endpoints/Prod']:
        # print(source_dir, dir_type,'>>>>>>>>>>>>>')
        for filename in os.listdir(os.path.join(source_dir,dir_type)):
            if filename.startswith('simons_sleep') and filename[:47]:
                matches = unique_cases['Recorded Dreem Filename'] == filename[:47]
                if matches.any():
                    i = unique_cases.index[matches].tolist()

                    old_name = os.path.join(os.path.join(source_dir,dir_type), filename)
                    new_path = os.path.join(os.path.join(source_dir,dir_type), filename.replace(user_wrong[i].iloc[0],user_fixed[i].iloc[0]))
                    os.rename(old_name,new_path)

    print("\n\n\n")
    dfSpid = pd.read_csv(f'{BASE_DIR}/sleep_subject_ids.csv').drop_duplicates()
    df = pd.read_csv(f'{BASE_DIR}/subjects_ids.csv').drop_duplicates()
    dfSpid['User ID'] = pd.to_numeric(dfSpid['SubjectId'].str.replace('U',''))
    dfSpid['tz_str'] = dfSpid['User ID'].apply(lambda x : df.loc[df.id == x]['tz_str'].to_list()[0])
    
    mappings = get_dreem_allocation_mapping()
    participants = {}
    file_users_map = {}
    spids = set()
    for k,v in mappings.items():
        # print(v)
        file_users_map[k] = set()
        for r in v:
            user_id = r[0]
            start_date = datetime.strptime(r[1],'%Y-%m-%d %H:%M:%S').date()
            end_date = datetime.strptime(r[2],'%Y-%m-%d %H:%M:%S').date()
            spid = tz_str = None
            dft = dfSpid.loc[dfSpid.SubjectId == user_id]
            if not dft.empty:
                spid = dft['SPID'].to_list()[0]
                tz_str = dft['tz_str'].to_list()[0]

            if spid:
                if args.spid and spid != args.spid:
                    continue

                file_users_map[k].add(user_id)
                # spids.add(spid)
                if spid not in participants:
                    participants[user_id] = {
                        'sleep_id' : k,
                        'user_id' : user_id,
                        'start_date' : start_date,
                        'end_date' : end_date,
                        'spid' : spid,
                        'tz_str' : tz_str
                    }
                else:
                    if start_date < participants[spid]['start_date']:
                        participants[spid]['start_date'] = start_date
                    if end_date < participants[spid]['end_date']:
                        participants[spid]['end_date'] = end_date

    missing_ids = set(dfSpid.SubjectId.unique()) - set(participants.keys())
    for k in missing_ids:
        print(f"Error : can't find data files for {k}.")

    # print(participants,'>>>>>>>>>>>>>')
    ## move raw data files to ouput directory by user 
    if args.skip_move_files is None:
        print("moving raw data files to ouput directory by user")
        move_files(file_users_map, participants, source_dir)
    
    pDf = pd.DataFrame(participants.values())
    if args.spid:
        pDf = pDf.loc[pDf.spid == args.spid]
    
    participants = list(pDf.T.to_dict().values())
    
    print(f"Processing dreem_process_hypno for {len(participants)} participants {datetime.now()}")
    workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
    results = workers.map(dreem_process_hypno, participants)
    workers.close()
    workers.join()
    print(f"Completed Processing dreem_process_hypno for {len(participants)} participants {datetime.now()}")

    print(f"Processing dreem_process_report for {len(participants)} participants {datetime.now()}")
    workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
    results = workers.map(dreem_process_report, participants)
    workers.close()
    workers.join()
    print(f"Completed Processing dreem_process_report for {len(participants)} participants {datetime.now()}")

    print(f"Processing dreem_process_edf for {len(participants)} participants {datetime.now()}")
    workers = Pool(int(os.environ['WORKERS']) if os.environ['WORKERS'] else 14)
    results = workers.map(dreem_process_edf, participants)
    workers.close()
    workers.join()
    print(f"Completed Processing dreem_process_edf for {len(participants)} participants {datetime.now()}")
    
    
        
