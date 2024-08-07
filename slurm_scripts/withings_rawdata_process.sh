#!/bin/bash
#SBATCH --job-name=sleep_withings
#SBATCH --output=/mnt/home/nshah/Simons-sleep-DM/withing_job.log
#SBATCH --mem=256GB
#SBATCH --nodes=1
#SBATCH --ntasks=1  # Adjust based on available resources and desired concurrency
#SBATCH --cpus-per-task=28  # Adjust if each task requires multiple CPUs


srun /mnt/home/nshah/venv-test/bin/python /mnt/home/nshah/Simons-sleep-DM/python_scripts/withing_rawdata_process.py -d /mnt/home/nshah/ceph/Sleep_study-neelay/SubjectsData/data_share_test -o /mnt/ceph/users/info/datasets/sleep_data/aug_data_release_30_families --workers 26