# Basedataset child classs to load OpenSSH logs
import re
from datetime import datetime
import pandas as pd

from classes.basedataset_class import BaseDataset, DatasetConfig
from constants import *
from constants_openssh import *

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
         
    # various column labels
    BASE_RISK_SCORE_COL = "base_risk_score"
    ADJUSTED_RISK_SCORE_COL = "adjusted_risk_score"
    
    def __init__(self, dataset_config: 'DatasetConfig'):
        """
        overrides BaseDataset __init__ method and uses own loader to parse SSH logfile into a pandas dataframe

        """
        self.dataset_config = dataset_config
        if self.dataset_config.data_input_format not in self.SUPPORTED_DATA_FORMATS:
            raise ValueError(f"Dataset '{self.dataset_config.get_id()}' attempted to use a data input format {self.dataset_config.data_input_format} unsuitable for OpenSSH log parsing.")
        self.df = self.load()  # Load the dataset using the "other" type
        self.validate_mandatory_fields()


    def load(self):
        """
        Just a wrapper to call the logfile parser class method.
        returns a dataframe with parsed logfile data.
        """
        return self.parse_log(self.dataset_config.data_input_path)

    @classmethod
    def parse_log(cls, path_to_logfile):
        """
        A custom function for loading.
        Parses OpenSSH log file into a Pandas dataframe

        Returns:
            Pandas dataframe with parsed log entries; for each log entry the regexp pattern used is specified.

        """
        
        parsed_log_entries = []
        with open(path_to_logfile, 'r') as logfile:
            for log_entry in logfile:
                log_entry = log_entry.strip()                       # b/c otherwise the entries will include line breaks and output csv will look like a mess.
                parsed_log_entry = cls.parse_log_entry(log_entry)
                if parsed_log_entry:
                    parsed_log_entries.append(parsed_log_entry)

        # Create a DataFrame from the parsed log entries
      
        return pd.DataFrame(parsed_log_entries)
    

    @classmethod
    def detect_pattern(cls, line):
        """
        Detect the pattern used for a log line.
        Returns the pattern name and match object if found, else None.

        """
        for name, pattern in cls.PARSING_PATTERNS.items():
            match = re.match(pattern, line)
            if match:
                return name, match
        return UNKNOWN, None    # if there is no match, the pattern is unknown.


    @classmethod
    def parse_log_entry(cls, log_entry):
        """
        detects the parsing pattern and parses the log entries.
        for events that have a matching pattern includes the pattern name in the output, otherwise uses UNKNOWN.
        always includes also raw log entry
        based on different patterns, different fields will be returned; if a pattern does not contain a specific field,
        it will have a None/NaN value.
        """
        pattern_name, match = cls.detect_pattern(log_entry)
        if match:
            parsed_data = match.groupdict()
            parsed_data[OSSH_EVENT_PATTERN] = pattern_name  # Add the pattern name to the output
            parsed_data[OSSH_RAW] = log_entry
            return parsed_data
        return {OSSH_EVENT_PATTERN: UNKNOWN, OSSH_RAW: log_entry}  # Include raw line for unmatched cases


    def data_cleanup(self):
        """
        clean up erroneous and unnecessary data, process blank values, bring the datetime formats to a common standard.
        """
        print("no data cleanup required this time!")
        pass


    def calculate_features(self):
        """
        a collection of calls to methods that update self.df by adding new features (counts, encoded values, risk scores)
        """

        self.add_load_date()
        
        # set base risk score according to the event types
        self.calc_entry_base_score(risk_score_col=self.BASE_RISK_SCORE_COL,event_type_col=OSSH_EVENT_PATTERN)
        self.add_timestamps(input_col=OSSH_TSTAMP, output_col=self.UNIX_TIMESTAMP_SEC)
        self.df = self.df.sort_values(by=self.UNIX_TIMESTAMP_SEC, ascending=True).reset_index(drop=True)

        self.initialize_adjusted_risk_score(base_risk_score_col=self.BASE_RISK_SCORE_COL, adjusted_score_col=self.ADJUSTED_RISK_SCORE_COL)

        # check for likely successful bruteforce attempts.
        self.password_bruteforcing_events(event_timestamp_col=self.UNIX_TIMESTAMP_SEC, event_type_col=OSSH_EVENT_PATTERN,
                                          adjusted_score_col=self.ADJUSTED_RISK_SCORE_COL, lookup_key_col=USERID_FIELD,
                                          time_period_secs=600)


    def calc_entry_base_score(self, risk_score_col, event_type_col):
        """
        adds base score for each log entry based on event type to risk score mapping in BASE_RISK_SCORES.
        the base score is placed into risk_score_col.
        NB, base score does not consider additional factors that might increase the risk. these are prt of adjusted entry score.
        """
        
        self.df[risk_score_col] = self.df[event_type_col].map(self.BASE_RISK_SCORE_MAP)       
        return

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
        """
        SUCCESSFUL_LOGIN_EVTS = ["Successful Login", "PAM Session Opened"]
        BRUTEFORCE_EVTS = ["Too Many Authentication Failures", "Repeated PAM Authentication Failures", "Repeated Password Failure"]

        # filter out successful logins as well as likely brute force events
        successful_logins = self.df[self.df[event_type_col].isin(SUCCESSFUL_LOGIN_EVTS)]
        brute_force_events = self.df[self.df[event_type_col].isin(BRUTEFORCE_EVTS)]

        # Perform a self-join on username
        merged = pd.merge(
            successful_logins.reset_index(),              # reset_index preserves the copies of original indices, and this can the be retrieved later on.
            brute_force_events.reset_index(),
            how="inner",
            left_on=lookup_key_col,                     # lookup is on the username
            right_on=lookup_key_col,
            suffixes=("_success", "_bruteforce")
        )

        # Filter for brute force events within the time period preceding successful logins
        valid_bruteforce = merged[
            (merged[f"{event_timestamp_col}_success"] <= merged[f"{event_timestamp_col}_bruteforce"] + time_period_secs) &
            (merged[f"{event_timestamp_col}_success"] >= merged[f"{event_timestamp_col}_bruteforce"])
        ]

        # Get the original indexes of successful login events to update
        valid_successful_indexes = valid_bruteforce["index_success"].unique()

        # add success_score value to adjusted scores for successful logins in the original dataframe:
        self.df.loc[valid_successful_indexes, adjusted_score_col] += success_score




    def get_success_after_fail(self, num_failures=3, interval_seconds=15*60 ):
        """
        Analyzes OpenSSH log data to identify successful logon events that have been preceeded 
        with K failed logon attempts within N seconds.
        
        num_failures (int): number of consecutive logon failures
        interval_seconds (int): time interval between first failed attempt and the successful logon attempt
        
        """
        pass
    
    def password_fail():
        pass
        # failregex = sshd\[.*\]: Failed password for .* from <HOST>
        
        
