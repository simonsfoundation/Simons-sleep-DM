#!/bin/bash
#SBATCH --time 1:00:00
#SBATCH --job-name=Sleep_algo
#SBATCH --output=ceph/SO_%a.out
#SBATCH --array=0-150# Number of tasks/subjects
#SBATCH --mem 10GB

# Load the necessary modules or activate the virtual environment if required
 
# Change to the directory containing your Python script
cd /mnt/ceph/users/mhacohen/data/Github/simons-sleep-ai/analysis/

# Call your Python script and pass the subject ID as an argument
python SoWu_algo.py $SLURM_ARRAY_TASK_ID 'Ilan' 'id'