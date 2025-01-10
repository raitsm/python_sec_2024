# project_root/uzdevums_8.py

# this is the main run file for the assignment 8+

# Read /var/log/messages or any other file that you prefer
# Find all the logs which pertain to USB (or choose other keywords) and print them out selectively
# Try to filter out fields within the log line

# OpenSSH logfile sourced from Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics
# loghub's GitHub site: https://github.com/logpai/loghub
# OpenSSH log: https://zenodo.org/records/8196385/files/SSH.tar.gz?download=1
import pandas as pd
from datetime import datetime, timezone

# initialize global logger & task manager
# NB, logger comes first because task manager uses logger
from classes.logger import Logger
logger = Logger().get_logger()

from classes.task_manager import TaskManager
task_mgr = TaskManager.get_taskmgr(max_threads=2)

from classes.basedataset_class import BaseDataset, DatasetConfig
from classes.opensshlog_class import OpenSSHLogonData
from classes.userdataset_class import UserDataset
from constants import *
from constants_openssh import *

def main():

    logger.info("Project launch")
    # define required fields for OpenSSH logs and for user datasets
    openssh_log_fields = [OSSH_TSTAMP, OSSH_HOST, OSSH_PROCESS, USERID_FIELD, OSSH_SRC_IP, OSSH_RAW, OSSH_DETAILS]
    user_fields = ["user_id", "privileges", "account_enabled"]


    # define configuration parameters for datasets
    openssh_log_config = DatasetConfig(dataset_id="OpenSSH sample log", data_input_path="./data_in/SSH.log",
                                data_output_path="./SSH_Log_data.csv",
                                mandatory_fields=openssh_log_fields, data_input_format=FORMAT_OPENSSH,
                                immediately_load_data=False)

    # define configuration parameters for datasets
    user_config_a = DatasetConfig(dataset_id="system A users", data_input_path="./system_a_users.csv",
                                data_output_path="./system_a_users_OUT.csv",
                                mandatory_fields=user_fields, data_input_format="csv",
                                immediately_load_data=False)

    user_config_b = DatasetConfig(dataset_id="system B users", data_input_path="./system_b_users.csv", 
                                data_output_path="./system_b_users_OUT.csv",
                                mandatory_fields=user_fields, data_input_format="csv",
                                immediately_load_data=False)


    # create the dataset objects, but do not load data immediately
    users_a = UserDataset(dataset_config=user_config_a)
    users_b = UserDataset(dataset_config=user_config_b)
    openssh_data = OpenSSHLogonData(dataset_config=openssh_log_config)

    # now load data into the datasets as directed by the configuration
    
    # threaded data loading using task manager
    openssh_data.load_data(use_taskmgr=True)
    users_a.load_data(use_taskmgr=True)
    users_b.load_data(use_taskmgr=True)

    # regular data loading
    # openssh_data.load_data(use_taskmgr=False)
    # users_a.load_data(use_taskmgr=False)
    # users_b.load_data(use_taskmgr=False)

    task_mgr.wait_for_all()         # do not proceed further until all data is loaded


    openssh_data.data_cleanup()
    print("identifying risky events")
    openssh_data.calculate_features()


    # add load date to system A users
    users_a.add_load_date()     # using default load date column and default date format
    users_b.add_load_date()

   
    print("writing output")
    openssh_data.save_as_csv(use_taskmgr=True)
    users_a.save_as_csv(filepath=users_a.dataset_config.get_output_path(), use_taskmgr=True)
    users_b.save_as_csv(filepath=users_b.dataset_config.get_output_path(), use_taskmgr=True)

    # save data without using task manager
    # openssh_data.save_as_csv(use_taskmgr=False)
    # users_a.save_as_csv(filepath=users_a.dataset_config.get_output_path(), use_taskmgr=False)
    # users_b.save_as_csv(filepath=users_b.dataset_config.get_output_path(), use_taskmgr=False)

    # wait for everything to complete.
    task_mgr.wait_for_all()    


    logger.info("all done")

        
main()
