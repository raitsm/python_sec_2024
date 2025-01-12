# project_root/constants_openssh.py

# constants specific for openssh log class

OSSH_TSTAMP = 'ossh_timestamp'
OSSH_HOST = 'ossh_host'
OSSH_PROCESS = 'ossh_process'
OSSH_EVT = 'ossh_event'
OSSH_SRC_IP = 'ossh_source_ip_addr'
OSSH_RESULT = 'ossh_result'            # whether the login was successful or not
OSSH_PORT = "ossh_port"
OSSH_DETAILS = "ossh_details"           # stuff at the end of the line that cannot be parrsed
OSSH_MSG = "ossh_message"               # more freetext stuff
OSSH_RAW = "ossh_raw"                   # raw log entry
OSSH_EVENT_PATTERN = "ossh_event_pattern"
OSSH_UID = "ossh_uid"                   # user id for the authentication process
OSSH_EUID = "ossh_euid"                 # effective user id for the authentication process
OSSH_TTY = "ossh_tty"                   # terminal device id
OSSH_RUSER = "ossh_ruser"               # remote user
OSSH_LOGNAME = "ossh_logname"           # yet another login name
