# Simons-sleep-DM
Data management scripts for Simons Sleep Project 

1. [Abstract](#abstract)
2. [Requirements](#requirements)
3. [Installation](#installation)
4. [Project Structure](#project-structure)
5. [Emaptica script](#empatica-sync)
6. [Dreem script](#dreem-sync)
7. [Withings script](#Withings-sync)

## Abstract

This repository holds the codebase, scripts and files for the creation and managment of the Simons Sleep Project pilot repository.

In this project we collected data from at-home sleep recordings using an EEG headband (Dreem; Beacon bio-siganls Ltd., a under-the-mattress pressures sensor (Withings ) and a multi-sensor smartwatch (EmbracePlus).

## Requirements
he following environment and libraries are required:

1. Python 3.8 or newer
2. AWS CLI 
3. An operating system with support for Python and the above libraries (tested on Linux Ubuntu 20.04 and macOS Catalina)

Other OS/Python distributions are expected to work.

## Installation
### Prepare new environment Using Conda:

This project provides a sleep.yml file which contains all the necessary Python package dependencies.
> conda env create -f sleep.yml

This command reads the sleep.yml file, sets up the sleep_study environment with all specified packages and their versions.

Activate the environment before running the script with:
> conda activate sleep

## Project Structure

The project operates on a hierarchical pipeline logic, beginning with the synchronization of database and AWS bucket data, followed by preprocessing and data harmonization. The final steps involve processing the data to infer basic sleep and activity measures.

The primary orchestrators for the Dreem and Empatica data are the empatica_sync.py and dreem_sync.py scripts, respectively. These scripts include embedded slurm commands to process user data, iterating over participant dates and performing various data management tasks, such as timezone harmonization, typo correction, and raw data analysis for deriving metrics like activity counts and sleep/wake classifications.

Bellow is a tree digram that demonstrates the orgnization of the data at the end of the data processing and harmonization process described above

![data_share](https://github.com/Micha098/Simons-sleep-DM/assets/107123518/ce2a49b8-7102-48ce-badb-22c47a539847)


## Emaptica script
`empatica_sync.py`

The empatica_sync.py script is responsible for pulling data from the AWS cloud for Empatica devices, organizing it according to participant and date, and initiating subsequent processing steps. It uses AWS CLI commands for data synchronization and schedules daily tasks to update and process new data.

This script ensures that all Empatica data is current and correctly allocated, facilitating the comprehensive analysis of participant sleep patterns.
The General structure of the code works on hirracical logic of pipline of set of actions starting from the syncronization of the data in the databese exising in the cluser with the device data the is on the AWS bucket for each of the different devices. The next step after the syncronization is preprocessing and harmonization of time zone of the data.

Key steps include:

1. Synchronizing Empatica device data from an AWS S3 bucket to a local directory.
2. Running slurm batch jobs to preform preprocessing and hramonization of the the data


- **AWS S3 Data Sync**: Sets environment variables for AWS S3 access and synchronizes data from an S3 bucket to a local directory and Uses the AWS CLI command to sync data from the specified S3 bucket to a local path.
  
```
%sync_command = f"aws s3 sync {os.environ['ACCESS_URL']} {os.environ['LOCAL_PATH']} --region us-east-1"
subprocess.run(sync_command, shell=True)
subprocess.run(f"{sync_command} > output.txt", shell=True)
```

### Data Preparation
- **Subject Data Retrieval**: This step extracts subject IDs and their respective time zones from a CSV file at a specified path, ensuring accurate data handling for each subject.

Below is a tree diagram that demonstrates the original organization of the data when it is pulled from the S3 cloud:

![raw_data](https://github.com/Micha098/Simons-sleep-DM/assets/107123518/f5e32b5b-4adb-49d3-b4e1-d3144f1d0464)

### Summrized Data Processing
- **Slurm Job Submission**: The aggregated data script submits a Slurm job to process summary measures from the Empatica directory. It combines these measures into a single table per day in the format `empatica_measures_{subject_id}_{date}.csv`.
- **Measurements Processed**: The measures included in this process are wear detection, sleep detection, activity count, step count, heart rate, and respiratory rate. Importantly, since not all summary data were released by Empatica at the same time (i.e. "respiratory rate"), some participents have only some of the measures meantioned above.
- 
- **Time Zone Handling**: Since data on the Empatica server is uploaded in UTC (00:00), the data for each participant is concatenated into one long file per subject. It is then adjusted to match each participant's local time zone before being split again into individual daily files for each subject.

### Raw Data Processing
- **Slurm Job Submission**: A second Slurm job processes raw data from the Empatica directory, producing individual daily files adjusted to each subject's time zone.

- **Raw Data Processed**: This process handles raw data including accelerometer, gyroscope, blood volume pulse (BVP), electrodermal activity (EDA), and temperature. Similar to summary data, the availability of certain types of raw data (e.g., gyroscope) may vary among participants. Data sampling frequencies range from 1 to 64 Hz.
- **Preprocessing**: Raw data are stored in Avro files segmented into 15-minute intervals, formatted as `1-1-{sub_id}_{UNIX timestamp}.avro`, where the timestamp marks the start of the 15-minute period. The script extracts and concatenates these data into one array per day, filling any gaps with NaN values.
- **Time Zone Handling**: As with summary data, raw data uploaded in UTC at 00:00 are concatenated into two-day segments. These are then adjusted to match each participant's local time zone and subsequently split into individual daily files.


A more detailed explantion regarding Emaptica measures, and some preproccing code examples could be found in the Ematica documenation:

/https://manuals.empatica.com/ehmp/careportal/data_access/v2.4e/en.pdf

## Dreem script
`dreem_sync.py`
This script handles the synchronization of sleep data from Dreem devices, processes the data, and prepares summary reports. Below are the key functionalities implemented in the script:

#### AWS Data Pull
- **AWS S3 Data Sync**: Sets environment variables for AWS S3 access and synchronizes data from an S3 bucket to a local directory and Uses the AWS CLI command to sync data from the specified S3 bucket to a local path.
  
```
%sync_command = f"aws s3 sync {os.environ['ACCESS_URL']} {os.environ['LOCAL_PATH']} --region us-east-1"
subprocess.run(sync_command, shell=True)
subprocess.run(f"{sync_command} > output.txt", shell=True)
```
#### Data Mapping
The script calls a dictionary table that translates betweeen Dreem-id and dates to the subject ids. the code then allcoates the appropiate dreem files to the folders of the correct subjects.

- **Unique Case mapping**: Calls the `dreem_allocation()` function to prepare a list of cases of "unique cases" for data saved incorrectly under the wrong Dreem user id or worng device.
- **subject ids mapping and mapping**: Since every Dreem_id is associated with few different participent ids at different dates,the script redirect the appropriate Dreem files to the folders of the correct subjects, ensuring that each set of data is associated with the right participant.


#### Hypnogram Processing
- **Slurm Job Submission**: The script submits a job to process Dreem hypnogram data using `slurm_dreem_hypno.sh`. This job focuses on handling the raw hypnogram outputs from Dreem devices. The command is followed by a 10-min timer wait, to allow the complition of the hypnogram preprocessing that is necessary for the next stage.

- **Preprocessing and Time Zone Correction**: The script iterates over each file per user, comparing the time zone indicated in the file with the one listed in the "subjects_ids" data frame. If discrepancies are found, the data is shifted to align with the correct time zone. Additionally, it reconstructs the data to eliminate unnecessary text, enabling the data to be saved in a more usable CSV format. Each file is saved as a per-night file named `dreem_{subject_id}_{date}.csv`, where `date` represents the morning after the recording. The Slurm command is followed by a 10-min timer wait to ensure completion. This allows the complition of the hypnogram preprocessing that is necessary for the next stage.

#### EDF Files Processing

- **Further Data Processing**: Submits a job to process EDF files using `slurm_dreem_job.sh`, followed by a 60-minute wait to ensure the completion of this intensive task.

- **Preprocessing and Time Zone Correction**: Similar to the hypnogram files processing, this script iterates over each EDF file per user, comparing the time zone indicated in the file with the one listed in the "subjects_ids" data frame. If any discrepancies are found, the script adjusts the time zone accordingly. This correction modifies the actual data within the EDF files, particularly updating the 'Start_rec' timestamp to reflect the correct recording time. Additionally, the script cleans the data by removing unnecessary text, making the data suitable for storage in a CSV format. Each processed file is then saved as a per-night file named `eeg_{subject_id}_{date}.csv`, where `date` is the morning following the recording.

More detailed explantion regarding Dreem devices, the electrode location and the signal, could be found in the Beacon documenation and in this article 

[/https://manuals.empatica.com/ehmp/careportal/data_access/v2.4e/en.pdf
](https://academic.oup.com/sleep/article/43/11/zsaa097/5841249)

## Withings script
`withings_sync.py`

This script pulls data from the AWS cloud using the AWS CLI.

- **Time Zone Handling**: Data on the Withings server is initially uploaded in UTC at 01:00. This script adjusts each participant's data to match the time zone specified in the `subjects_id.csv` file. Specifically for the `sleep.csv` file, which already displays local time, the script corrects any discrepancies that may have occurred due to errors in the smartphone to which the device was connected.

- **File Naming**: The data is saved under the same filename as it was received.
