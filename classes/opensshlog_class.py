# classes/opensshlog_class.py

# Basedataset child classs to load and process OpenSSH logs
import re
from datetime import datetime
import pandas as pd
import threading

from classes.logger import Logger
logger = Logger().get_logger()

from classes.task_manager import TaskManager
# task_mgr = TaskManager()
task_mgr = TaskManager.get_taskmgr(max_threads=2)


from classes.basedataset_class import BaseDataset, DatasetConfig
from constants import *
from constants_openssh import *
from decorators import requires_loaded_data, log_method_call

class OpenSSHLogonData(BaseDataset):

    DEFAULT_DATE_FORMAT = "%b %d %H:%M:%S"      # date format found in ssh log file, eg, Dec 10 07:22:46
    SUPPORTED_DATA_FORMATS = ["opensshlog",]
        
    # parsing patterns acquired through ChatGPT 4o.
    PARSING_PATTERNS = {
        # ip spoofing?
        "Reverse Mapping Issue": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): reverse mapping checking .+\[(?P<{OSSH_SRC_IP}>[\d\.]+)\].+$'
        ),
        # ip spoofing?
        "Reverse Mapping Check Failure": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+)\s+'
            rf'(?P<{OSSH_HOST}>\S+)\s+(?P<{OSSH_PROCESS}>\S+):\s+Address\s+(?P<{OSSH_SRC_IP}>[\d\.]+)\s+'
            rf'maps\s+to\s+\S+,\s+but\s+this\s+does\s+not\s+map\s+back\s+to\s+the\s+address\s+-\s+'
            rf'POSSIBLE\s+BREAK-IN\s+ATTEMPT!$'
        ),        
        "Invalid User": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): Invalid user\s+(?P<{USERID_FIELD}>\S+)\s+from\s+(?P<{OSSH_SRC_IP}>[\d\.]+)$'
        ),        
        "Authentication Failure": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): Failed password for (invalid user\s+)?(?P<{USERID_FIELD}>\S+)\s+'
            rf'from\s+(?P<{OSSH_SRC_IP}>[\d\.]+)\s+port\s+(?P<{OSSH_PORT}>\d+)\s+ssh2$'
        ),
        "Successful Login": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): Accepted password for (?P<{USERID_FIELD}>\S+) '
            rf'from (?P<{OSSH_SRC_IP}>[\d\.]+) port (?P<{OSSH_PORT}>\d+).+$'
        ),
        "Disconnection": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): Received disconnect from (?P<{OSSH_SRC_IP}>[\d\.]+).+$'
        ),
        # brute-force related
        "Too Many Authentication Failures": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): Disconnecting: Too many authentication failures for (?P<{OSSH_MSG}>.+) \[preauth\]$'
        ),
        "PAM Ignoring Max Retries": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): PAM service\(sshd\) ignoring max retries; (?P<{OSSH_DETAILS}>.+)$'
        ),
        "Repeated PAM Authentication Failures": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+)\s+'
            rf'(?P<{OSSH_HOST}>\S+)\s+(?P<{OSSH_PROCESS}>\S+):\s+PAM\s+\d+\s+more\s+authentication\s+failure[s]?\s*;\s+'
            rf'logname=\s*(?P<{OSSH_LOGNAME}>\S*)\s*uid=\s*(?P<{OSSH_UID}>\d+)\s*'
            rf'euid=\s*(?P<{OSSH_EUID}>\d+)\s*tty=\s*(?P<{OSSH_TTY}>\S*)\s*'
            rf'ruser=\s*(?P<{OSSH_RUSER}>\S*)\s*rhost=\s*(?P<{OSSH_SRC_IP}>[A-Za-z0-9\.\-]+)\s*'
            rf'(user=\s*(?P<{USERID_FIELD}>\S+))?$'
        ),
        # ip address blocking
        "Blocked IP Address": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): Blocked IP address (?P<{OSSH_SRC_IP}>[\d\.]+)$'
        ),        
        "Connection Reset by Peer": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): Connection reset by (?P<{OSSH_SRC_IP}>[\d\.]+) \[preauth\]$'
        ),
        "Connection Closed by Peer": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): Connection closed by (?P<{OSSH_SRC_IP}>[\d\.]+) \[preauth\]$'
        ),
        # possible break-in attempt alerts
        # "POSSIBLE BREAK-IN ATTEMPT": (
        #     rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
        #     rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): POSSIBLE BREAK-IN ATTEMPT!$'
        # ),
        # ip spoofing?
        # "Failed Reverse Mapping Check": (
        #     rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
        #     rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): reverse mapping checking .+\[(?P<{OSSH_SRC_IP}>[\d\.]+)\] failed - POSSIBLE BREAK-IN ATTEMPT!$'
        # ),
        # invalid authentication - usually a problem if it repeats
        "Invalid User Auth Request": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): input_userauth_request: invalid user\s+(?P<{USERID_FIELD}>\S*)\s*\[preauth\]$'
        ),
        "PAM Check Pass (User Unknown)": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): pam_unix\(sshd:auth\): check pass; user unknown$'
        ),
        "PAM Authentication Single Failure": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+)\s+'
            rf'(?P<{OSSH_HOST}>\S+)\s+(?P<{OSSH_PROCESS}>\S+):\s+pam_unix\(sshd:auth\):\s+authentication\s+failure;\s+'
            rf'logname=(?P<{OSSH_LOGNAME}>\S*)\s*uid=(?P<{OSSH_UID}>\d+)\s*euid=(?P<{OSSH_EUID}>\d+)\s*'
            rf'tty=(?P<{OSSH_TTY}>\S*)\s*ruser=(?P<{OSSH_RUSER}>\S*)\s*rhost=(?P<{OSSH_SRC_IP}>[A-Za-z0-9\.\-]+)\s*'
            rf'(user=(?P<{USERID_FIELD}>\S+))?$'
        ),
        "Repeated Password Failure": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+)\s+'
            rf'(?P<{OSSH_HOST}>\S+)\s+(?P<{OSSH_PROCESS}>\S+):\s+message\s+repeated\s+\d+\s+times:\s+\[\s*'
            rf'Failed password for (?P<{USERID_FIELD}>\S+)\s+from\s+(?P<{OSSH_SRC_IP}>[\d\.]+)\s+'
            rf'port\s+(?P<{OSSH_PORT}>\d+)\s+ssh2\s*\]\s*$'
        ),
        
        "PAM Session Opened": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): pam_unix\(sshd:session\): session opened for user (?P<{USERID_FIELD}>\S+) by \(uid=(?P<{OSSH_UID}>\d+)\)$'
        ),
        "PAM Session Closed": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): pam_unix\(sshd:session\): session closed for user (?P<{USERID_FIELD}>\S+)$'
        ),
               
        "extended": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): '
            rf'(?P<{USERID_FIELD}>\S+) (?P<{OSSH_DETAILS}>.+)$'
        ),
        "default": (
            rf'^(?P<{OSSH_TSTAMP}>[A-Za-z]+\s+\d+\s+\d+:\d+:\d+) '
            rf'(?P<{OSSH_HOST}>\S+) (?P<{OSSH_PROCESS}>\S+): '
            rf'(?P<{OSSH_DETAILS}>.+)$'
        )        
    }
    
    # graded risk scores from 0 to 100 are added to the events.
    # the scores take cybersecurity perspective.
    # 0 - informational event with no risk relevance.
    # 100 - a certain or near-certain break-in.
    
    BASE_RISK_SCORE_MAP = {
        "Reverse Mapping Issue": 50,  # Average relevance unless combined with other suspicious activity.
        "Reverse Mapping Check Failure": 70,  # Indicates potential IP spoofing; higher risk when frequent or combined with login attempts.
        "Invalid User": 40,  # Medium risk; indicates probing for weak or misconfigured accounts.
        "Authentication Failure": 60,  # Medium-high risk; further elevated if repeated or targeting sensitive accounts like root.
        "Successful Login": 20,  # Informational; risk depends on the context (e.g., unexpected logins). problematic if follows a series of unsuccessful logins.
        "Disconnection": 5,  # Informational; not inherently risky but could indicate aborted attacks.
        "Too Many Authentication Failures": 60,  # High risk; often indicates brute force attacks.
        "PAM Ignoring Max Retries": 70,  # High risk; system is set up to allow unlimited login attempts; thus enabling brute force attacks. a lot of such records indicate ongoing attacks.
        "Repeated PAM Authentication Failures": 60,  # High risk; indicates repeated brute force attempts or misconfigured accounts.
        "Blocked IP Address": 10,  # Informational; indicates a proactive defense mechanism.
        "Connection Reset by Peer": 20,  # May indicate reconnaissance or aborted attack attempts.
        "Connection Closed by Peer": 20,  # Similar to reset; low to medium risk depending on frequency.
        # "POSSIBLE BREAK-IN ATTEMPT": 90,  # Extremely high risk; indicates strong evidence of an active attack.
        # "Failed Reverse Mapping Check": 50,  # Medium-high risk; may indicate suspicious activity or poorly configured DNS.
        "Invalid User Auth Request": 50,  # Medium-high risk; reconnaissance or brute force attempts.
        "PAM Check Pass (User Unknown)": 30,  # a single event not risky; repeated events may indicate malicious activity.
        "PAM Authentication Single Failure": 50,  # Medium-high risk; risk depends on frequency and target account.
        "Repeated Password Failure": 70,  # High risk; sustained brute force attempts.
        "PAM Session Opened": 20,  # Informational; consider risk if combined with "Invalid User" or "Authentication Failure."
        "PAM Session Closed": 20,  # Informational; related to normal activity unless suspicious patterns are seen.
        "extended": 10,  # Catch-all; risk depends entirely on the parsed details.
        "default": 10,  # Generic fallback; low relevance unless analyzed further.
    }    
    
    SUCCESSFUL_LOGIN_EVTS = [                           # event types that correspond to successful logins
        "Successful Login", "PAM Session Opened", 
    ]

    BRUTEFORCE_EVTS = [                                 # event types that correspond to suspected bruteforce attempts
        "Too Many Authentication Failures", "Repeated PAM Authentication Failures",
        "Repeated Password Failure",
    ]
    
    TRUSTED_NETWORKS = [
        "137.189.204", "137.189.205", "137.189.206", "137.189.207",
        "137.189.240","137.189.241", "137.189.88", "137.189.89",
        "119.137.60", "119.137.62", "119.137.63",
    ]
    
    
    # risk score modifiers
    INSIDER_BRUTEFORCE_ATTEMPT = 60
    SUCCESSFUL_BRUTEFORCE = 150
         
    # various column labels
    BASE_RISK_SCORE_COL = "base_risk_score"
    ADJUSTED_RISK_SCORE_COL = "adjusted_risk_score"
    SUCCESSFFUL_LOGIN_FLAG = "Successful_Login_Flag"            # 1 if the login was successful, 0 otherwise
    BRUTEFORCE_FLAG = "Suspect_Bruteforce_Flag"                 # 1 if the event type is one that indicates a (suspected) bruteforce attack, 0 otherwise
    TRUSTED_NETWORK_FLAG = "Trusted_Network_Flag"               # 1 if the source address comes from what is defined as a trusted network, 0 otherwise
    INSIDER_BRUTEFORCE_FLAG = "Insider_Bruteforce_Flag"         # 1 if a suspected bruteforce from a trusted network, 0 otherwise
    

    def __init__(self, dataset_config: 'DatasetConfig'):
        """
        Initialize the OpenSSHLogonData class.
        Calls the parent constructor for shared attributes and configuration.
        """
        super().__init__(dataset_config=dataset_config)


    @log_method_call
    def load_data(self, use_taskmgr=False):
        """
        Custom data loading for OpenSSH log data.
        Validates the input file and parses the log into a DataFrame.
        
        Args:
            use_task_mgr (bool): whether to execute in a separate thread using task manager, default is False
        """
        # Validate the input file path
        def load_operation():
            with self.lock:
                self.validate_input_file(self.dataset_config.get_input_path())

                # Ensure the data format is supported
                if self.dataset_config.data_input_format.lower() not in self.SUPPORTED_DATA_FORMATS:
                    raise ValueError(
                        f"Dataset '{self.dataset_config.get_id()}' attempted to load an unsupported format "
                        f"'{self.dataset_config.data_input_format}'. Supported formats: {self.SUPPORTED_DATA_FORMATS}"
                    )

                # Custom parsing logic for OpenSSH logs
                self.df = self.parse_log(self.dataset_config.get_input_path())

                # Validate mandatory fields after loading
                self.validate_mandatory_fields()
                # print(f"OpenSSH log data loaded successfully for dataset '{self.dataset_config.get_id()}'.")
                logger.info(f"Dataset '{self.dataset_config.get_id()}' loaded successfully from '{self.dataset_config.get_input_path()}' in thread {threading.current_thread().name}.")

        if use_taskmgr:
            task_mgr.submit(load_operation)  # Asynchronous execution
        else:
            load_operation()  # Synchronous execution


    def validate_mandatory_fields(self):
        """
        validate that mandatory fields are present using BaseDataset method.
        """
        super().validate_mandatory_fields()


    @classmethod
    def parse_log(cls, path_to_logfile):
        """
        parses OpenSSH log file into a Pandas df

        returns: Pandas df with parsed log entries; for each log entry the regexp pattern used is specified.

        """
        
        parsed_log_entries = []
        with open(path_to_logfile, 'r') as logfile:
            for log_entry in logfile:
                log_entry = log_entry.strip()                       # b/c otherwise the entries will include line breaks and output csv will look like a mess.
                parsed_log_entry = cls.parse_log_entry(log_entry)
                if parsed_log_entry:
                    parsed_log_entries.append(parsed_log_entry)      
        return pd.DataFrame(parsed_log_entries)
    

    @classmethod
    def detect_pattern(cls, line):
        """
        matches the patterns from PARSING_PATTERNS with logfile entry.
        Returns the pattern name from PARSING_PATTERNS and match object if found, else None.

        """
        for name, pattern in cls.PARSING_PATTERNS.items():
            match = re.match(pattern, line)
            if match:
                return name, match  # return pattern name & match object
        return UNKNOWN, None    # if there is no match, the pattern is unknown.


    @classmethod
    def parse_log_entry(cls, log_entry):
        """
        detects the parsing pattern and parses the log entries.
        for events that have a matching pattern includes the pattern name in the output, otherwise uses UNKNOWN.
        always includes also raw log entry
        based on different patterns, different fields will be returned; if a pattern does not contain a specific field,
        it will have a None/NaN value.
        returns: dictionary based on the detected pattern.
        """
        pattern_name, match = cls.detect_pattern(log_entry) # do pattern detection and data extraction into a regexp match object if pattern is detected.
        if match:
            parsed_data = match.groupdict() # this is the magic where the named groups are extracted from a match into a dictionary
            parsed_data[OSSH_EVENT_PATTERN] = pattern_name  # Add the pattern name to the output
            parsed_data[OSSH_RAW] = log_entry   # add the raw log entry
            return parsed_data
        return {OSSH_EVENT_PATTERN: UNKNOWN, OSSH_RAW: log_entry}  # Include raw line for unmatched cases


    @requires_loaded_data
    def data_cleanup(self):
        """
        clean up erroneous and unnecessary data, process blank values, bring the datetime formats to a common standard.
        """
        # if not self.is_loaded:  
        #     raise ValueError(f"Data not loaded into '{self.dataset_config.get_id()}' .")
        
        print("no data cleanup required this time!")
        pass


    @log_method_call
    @requires_loaded_data
    def calculate_features(self):
        """
        a collection of calls to methods that update self.df by adding new features (counts, encoded values, risk scores)
        """

        # if not self.is_loaded:  
        #     raise ValueError(f"Data not loaded into '{self.dataset_config.get_id()}' .")


        self.add_load_date()
        
        # set base risk score according to the event types
        self.calc_entry_base_score(risk_score_col=self.BASE_RISK_SCORE_COL,event_type_col=OSSH_EVENT_PATTERN)
        self.add_timestamps(input_col=OSSH_TSTAMP, output_col=self.UNIX_TIMESTAMP_SEC)
        
        # do some feature encoding -> from strings to integers
        # set flags for successful logins and for bruteforce events - to make filtering and counting easier & faster
        self.df[self.SUCCESSFFUL_LOGIN_FLAG] = self.df[OSSH_EVENT_PATTERN].isin(self.SUCCESSFUL_LOGIN_EVTS).astype(int)
        self.df[self.BRUTEFORCE_FLAG] = self.df[OSSH_EVENT_PATTERN].isin(self.BRUTEFORCE_EVTS).astype(int)
        
        # flag event where source ip is from a trusted network
        self.df[self.TRUSTED_NETWORK_FLAG] = self.df[OSSH_SRC_IP].str.startswith(tuple(self.TRUSTED_NETWORKS), na=False).astype(int)

        # identify events related to suspected bruteforce from a trusted network (there should not be any)
        self.df[self.INSIDER_BRUTEFORCE_FLAG] = ((self.df[self.BRUTEFORCE_FLAG] == 1) & (self.df[self.TRUSTED_NETWORK_FLAG] == 1)).astype(int)
  
       
        self.df = self.df.sort_values(by=self.UNIX_TIMESTAMP_SEC, ascending=True).reset_index(drop=True)

        # in future, this should support further transparency, showing original risk score for an entry next to "adjusted risk score"
        # which is achieved based on advanced methods like password_bruteforcing_events()
        self.initialize_adjusted_risk_score(base_risk_score_col=self.BASE_RISK_SCORE_COL, adjusted_score_col=self.ADJUSTED_RISK_SCORE_COL)


        # any updates of adjusted risk score must only come after the adjusted risk score has been initialized.

        # if a bruteforce attack is coming from a trusted network, increase adjusted risk score.
        # self.df.loc[(self.df[self.BRUTEFORCE_FLAG] == 1) & (self.df[self.TRUSTED_NETWORK_FLAG] == 1), self.ADJUSTED_RISK_SCORE_COL] += self.INSIDER_BRUTEFORCE_ATTEMPT
        self.df.loc[self.df[self.INSIDER_BRUTEFORCE_FLAG] == 1, self.ADJUSTED_RISK_SCORE_COL] += self.INSIDER_BRUTEFORCE_ATTEMPT

        # check for likely successful bruteforce attempts.
        self.password_bruteforcing_events(event_timestamp_col=self.UNIX_TIMESTAMP_SEC, event_type_col=OSSH_EVENT_PATTERN,
                                          adjusted_score_col=self.ADJUSTED_RISK_SCORE_COL, lookup_key_col=USERID_FIELD,
                                          time_period_secs=600, success_score=self.SUCCESSFUL_BRUTEFORCE)




    @requires_loaded_data
    def calc_entry_base_score(self, risk_score_col, event_type_col):
        """
        adds base score for each log entry based on event type to risk score mapping in BASE_RISK_SCORES.
        the base score is placed into risk_score_col.
        NB, base score does not consider additional factors that might increase the risk. these are prt of adjusted entry score.
        """
        
        self.df[risk_score_col] = self.df[event_type_col].map(self.BASE_RISK_SCORE_MAP)       
        return

    @requires_loaded_data
    def initialize_adjusted_risk_score(self, base_risk_score_col, adjusted_score_col):
        """
        initializes adjusted score column with values from base score column.
        assumes base score column exists.
        nothing else at the moment.
        
        Args:
            base_risk_score_col (str): base score column.
            adjusted_score_col (str): adjusted score column.
        """
        self.df[adjusted_score_col] = self.df[base_risk_score_col]

    @requires_loaded_data
    def password_bruteforcing_events(self, event_type_col, event_timestamp_col, adjusted_score_col, lookup_key_col, time_period_secs=600, success_score=90):
        """
        validates if successful logins might be related to bruteforce attacks preceeding the login.
        increases the risk scores of successful login events if there is reason to suspect they are a result of a successful bruteforce attack.

        Args:
            event_type_col (str): column name for event types
            event_timestamp_col (str): column name for event timestamps (Unix timestamp in seconds)
            adjusted_score_col (str): column name for adjusted risk scores
            lookup_key_col (str): column name for user identifiers (needed to tie together bruteforce attempts and logins)
            time_period_secs (int): time period in seconds relative to a successful login events to look back for brute force attacks
            success_score (int): penalty to add to successful logins related to bruteforce attacks
            
        NB, this has a potential for generalization, and could be used in parameter-driven processing.
        
        algorithm:
        create a temporary dataframe with all successful login events and a temporary dataframe with likely bruteforce events
        merge both together with an inner join using user id as a key: the merged df will contain success+bruteforce pairs (row number num_success * num_bruteforce)
        analyse each pair if success and bruteforce occurs within the defined time
        keep original index values for rows where success and bruteforce occur close enough
        update these rows with extra risk score   
        """
        # SUCCESSFUL_LOGIN_EVTS = ["Successful Login", "PAM Session Opened"]
        # BRUTEFORCE_EVTS = ["Too Many Authentication Failures", "Repeated PAM Authentication Failures", "Repeated Password Failure"]

        # filter out successful logins as well as likely brute force events
        successful_logins = self.df[self.df[event_type_col].isin(self.SUCCESSFUL_LOGIN_EVTS)]
        brute_force_events = self.df[self.df[event_type_col].isin(self.BRUTEFORCE_EVTS)]

        # Perform a self-join on username
        merged = pd.merge(
            successful_logins.reset_index(),              # reset_index preserves the copies of original indices, and this can the be retrieved later on.
            brute_force_events.reset_index(),
            how="inner",
            left_on=lookup_key_col,                     # lookup is on the username
            right_on=lookup_key_col,
            suffixes=("_success", "_bruteforce")
        )           # rows will be produced only for users who have at least one successful login and at least one bruteforce event recorded on their userid
        # number of rows per user: number of successful logins * number of bruteforce attempts

        # Filter for brute force events within the time period preceding successful logins
        valid_bruteforce = merged[
            (merged[f"{event_timestamp_col}_success"] <= merged[f"{event_timestamp_col}_bruteforce"] + time_period_secs) &
            (merged[f"{event_timestamp_col}_success"] >= merged[f"{event_timestamp_col}_bruteforce"])
        ]

        # Get the original indexes of successful login events to update
        valid_successful_indexes = valid_bruteforce["index_success"].unique()
        valid_bruteforce_indexes = valid_bruteforce["index_bruteforce"].unique()

        # add success_score value to adjusted scores for successful logins in the original dataframe (use the original indexes):
        self.df.loc[valid_successful_indexes, adjusted_score_col] += success_score
        self.df.loc[valid_bruteforce_indexes, adjusted_score_col] += success_score

