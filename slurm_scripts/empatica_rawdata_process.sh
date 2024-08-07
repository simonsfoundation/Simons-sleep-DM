#!/bin/bash
#SBATCH --job-name=sleep_empatica
#SBATCH --output=/mnt/home/nshah/Simons-sleep-DM/empatica_job.log
#SBATCH --mem=400GB
#SBATCH --nodes=4
#SBATCH --ntasks=4  # Adjust based on available resources and desired concurrency
#SBATCH --cpus-per-task=22  # Adjust if each task requires multiple CPUs


srun /mnt/home/nshah/sleep-venv/bin/python /mnt/home/nshah/Simons-sleep-DM/python_scripts/withing_rawdata_process.py -d /mnt/home/nshah/ceph/Sleep_study-neelay/SubjectsData/data_share_test -o /mnt/ceph/users/info/datasets/sleep_data/aug_data_release_30_families --workers 20 -t acc
srun /mnt/home/nshah/sleep-venv/bin/python /mnt/home/nshah/Simons-sleep-DM/python_scripts/withing_rawdata_process.py -d /mnt/home/nshah/ceph/Sleep_study-neelay/SubjectsData/data_share_test -o /mnt/ceph/users/info/datasets/sleep_data/aug_data_release_30_families --workers 20 -t eda
srun /mnt/home/nshah/sleep-venv/bin/python /mnt/home/nshah/Simons-sleep-DM/python_scripts/withing_rawdata_process.py -d /mnt/home/nshah/ceph/Sleep_study-neelay/SubjectsData/data_share_test -o /mnt/ceph/users/info/datasets/sleep_data/aug_data_release_30_families --workers 20 -t bvp
srun /mnt/home/nshah/sleep-venv/bin/python /mnt/home/nshah/Simons-sleep-DM/python_scripts/withing_rawdata_process.py -d /mnt/home/nshah/ceph/Sleep_study-neelay/SubjectsData/data_share_test -o /mnt/ceph/users/info/datasets/sleep_data/aug_data_release_30_families --workers 20 -t temprature