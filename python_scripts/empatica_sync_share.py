#!/usr/bin/env python
import os
import subprocess
import pandas as pd
import datetime as dt
from datetime import datetime
from datetime import timedelta
from datetime import time
import shutil
import json
import glob 
import pytz
import numpy as np
import pandas as pd
import re
import scipy
import seaborn as sns
from scipy import signal
import ast
import time as timer

# code for AWS data pull

os.environ['LOCAL_PATH'] = "/mnt/ceph/users/nshah/Sleep_study-neelay/SubjectsData/empatica/aws_data/"

sync_command = f"aws s3 sync {os.environ['ACCESS_URL']} {os.environ['LOCAL_PATH']} --region us-east-1"
subprocess.run(sync_command, shell=True)
subprocess.run(f"{sync_command} > output.txt", shell=True)

# activate slurm for agg data

command = [
    'sbatch', '/mnt/ceph/users/nshah/Simons-sleep-DM/slurm_scripts/slurm_agg_share.sh',
]
subprocess.run(command)


# wait 60 minutes hour 
timer.sleep(60 * 60)  # 60 minutes * 60 seconds


command = [
    'python', '/mnt/ceph/users/nshah/Simons-sleep-DM/python_scripts/empatica_raw_sahre.py',
]
subprocess.run(command)
