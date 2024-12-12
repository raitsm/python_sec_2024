# Read /var/log/messages or any other file tha%t you prefer
# Find all the logs which pertain to USB (or choose other keywords) and print them out selectively
# Try to filter out fields within the log line

# OpenSSH logfile sourced from Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics
# loghub's GitHub site: https://github.com/logpai/loghub
import pandas as pd
from datetime import datetime, timezone
from classes.basedataset_class import BaseDataset, DatasetConfig
from classes.opensshlog_class import OpenSSHLogonData
from constants import *
from constants_openssh import *

def main():

    # define required fields for OpenSSH logs
    openssh_log_fields = [OSSH_TSTAMP, OSSH_HOST, OSSH_PROCESS, USERID_FIELD, OSSH_SRC_IP, OSSH_RAW, OSSH_DETAILS]

    # define configuration parameters for datasets
    openssh_log_config = DatasetConfig(dataset_id="OpenSSH sample log", data_input_path="./data_in/SSH.log",
                                data_output_path="./SSH_Log_data.csv",
                                mandatory_fields=openssh_log_fields, data_input_type=FORMAT_OPENSSH)

    # create the datasets; load data from the source file
    print("loading and parsing logfile")
    openssh_data = OpenSSHLogonData(dataset_config=openssh_log_config)

    openssh_data.data_cleanup()
    print("identifying risky events")
    openssh_data.calculate_features()
   
    # show all failed attempts
    print("writing output")
    openssh_data.save_as_csv()
    print("all done")
    
    # show all suspicious attempts: two or more failed attempts followed by a successful attempt, all within 15 minutes.

        
main()
