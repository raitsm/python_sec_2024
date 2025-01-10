# classes/basedataset_class.py

# base dataset classs that provides basic i/o and data cleanup functions
import pandas as pd
import numpy as np
import csv
import os
from datetime import datetime
# for multi-thread operation demo
import threading

from classes.logger import Logger
logger = Logger().get_logger()

from classes.task_manager import TaskManager
# task_mgr = TaskManager()
task_mgr = TaskManager.get_taskmgr(max_threads=2)


from constants import UNKNOWN
from decorators import requires_loaded_data, log_method_call
from classes.custom_exceptions import DatasetMandatoryFieldsMissing, SearchColumnsMissing


class BaseDataset:
    # base dataset classs that provides basic i/o and data cleanup functions
    # at the core of a dataset there is a Pandas dataframe.
    # the added methods provide for i/o, data validation, cleanup and transformation.

    DEFAULT_ENCODING = "utf-8"
    DEFAULT_CSV_DELIMITER = ","
    DEFAULT_CSV_QUOTECHAR = '"'
    DEFAULT_DATA_TYPE = "str"
    DEFAULT_QUOTING = csv.QUOTE_ALL
    
    # constants for common column labels
    LOAD_DATE_COL = "Load_Date"
    UNIX_TIMESTAMP_SEC = "Unix_Timestamp_Secs"
    
    DEFAULT_DATE_FORMAT = "%Y-%m-%d"
    
    # default columns with default values that shall be added once data is loaded.
    DEFAULT_COLUMNS = {
        LOAD_DATE_COL: UNKNOWN,
    }
    
    # max number of threads to use
    # MAX_WORKERS = 2
    # _executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    


    def __init__(self, dataset_config: 'DatasetConfig'):
        """
        Initialize a dataset with configuration.
        If load_data configuration parameter is set to True, immediately load data
        """
        self.dataset_config = dataset_config
        self.lock = threading.Lock()  # instance level lock to be used by thread-safe operations
        self.df = None  # placeholder and a flag to see if data has been loaded.

        if self.dataset_config.load_data:   # immediate data loading requested.
            self.load_data()  


    def load_data(self, use_taskmgr=False):
        """
        Default method to load data based on the input format.
        Can be overridden by child classes for custom loading logic.
        
        Args:
            use_taskmgr (bool): whether to run in a separate thread using task manager, default is False
        """

        def load_operation():
            with self.lock:  # Ensure thread-safe access to self.df
                self.validate_input_file(self.dataset_config.get_input_path())

                data_format = self.dataset_config.data_input_format.lower()

                if data_format == "csv":
                    self.df = self.from_csv(self.dataset_config.get_input_path())
                elif data_format == "json":
                    self.df = self.from_json(self.dataset_config.get_input_path())
                else:
                    errmsg = f"Error: dataset '{self.dataset_config.get_id()}' expects unsupported format '{data_format}'."
                    logger.error(errmsg)
                    raise NotImplementedError(errmsg)
                self.validate_mandatory_fields()
                logger.info(f"Dataset '{self.dataset_config.get_id()}' loaded successfully from '{self.dataset_config.get_input_path()}' in thread {threading.current_thread().name}.")

        if use_taskmgr:
            task_mgr.submit(load_operation)  # send the task to task manager
        else:
            load_operation()  # run the task in the main thread


    def validate_input_file(self, filepath):
        """
        Validate the input file path to ensure it exists and is a file.
        To be extended.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Error while loading dataset '{self.dataset_config.get_id()}'. The source file '{filepath}' does not exist.")
        if not os.path.isfile(filepath):
            raise ValueError(f"Error while loading dataset '{self.dataset_config.get_id()}'. The source file specified '{filepath}' is not a file.")


    def from_csv(self, filepath, **kwargs):
        """
        Default method to load data from a CSV file.
        """
        kwargs.setdefault("encoding", self.DEFAULT_ENCODING)
        kwargs.setdefault("delimiter", self.DEFAULT_CSV_DELIMITER)
        kwargs.setdefault("quotechar", self.DEFAULT_CSV_QUOTECHAR)
        kwargs.setdefault("dtype", self.DEFAULT_DATA_TYPE)

        # Attempt to load the CSV file
        try:
            dataframe = pd.read_csv(filepath, **kwargs)
        except pd.errors.EmptyDataError as e:
            raise ValueError(f"Error while loading dataset '{self.dataset_config.get_id()}'. The file '{filepath}' is empty or invalid: {e}")
        except pd.errors.ParserError as e:
            raise ValueError(f"Error while loading dataset '{self.dataset_config.get_id()}'. Parsing error while reading the file '{filepath}': {e}")

        return dataframe


    def from_json(self, filepath, **kwargs):
        """
        Default method to load data from a JSON file.
        """
        kwargs.setdefault("encoding", self.DEFAULT_ENCODING)

        # Attempt to load the JSON file
        try:
            dataframe = pd.read_json(filepath, **kwargs)
        except ValueError as e:
            raise ValueError(f"Error while loading dataset '{self.dataset_config.get_id()}'. The file '{filepath}' is not a valid JSON file: {e}")

        return dataframe




    @property
    def is_loaded(self):
        """
        Check if data has been loaded into the dataset.
        """
        return self.df is not None

    
    def parse_log(self):
        """
        The method shall be used only for basedataset child clases to import data from sources using a specific format, e.g., from a logfile.
        """
        raise 



    @log_method_call
    @requires_loaded_data
    def save_as_csv(self, filepath=None, use_taskmgr=False, **kwargs):
        """
        Save the dataframe to a CSV file.
        Default parameters are provided through constants but can be overridden.
        Supports both synchronous and asynchronous execution.

        Args:
            filepath (str): path to the output file
            use_taskmgr (bool): whether to execute in a separaste thread using task manager, default: False
            **kwargs: encoding, separator, quote character, quoting mode -- all the stuff for CSV files
        """
        if filepath is None:
            filepath = self.dataset_config.data_output_path

        kwargs.setdefault('encoding', self.DEFAULT_ENCODING)
        kwargs.setdefault('sep', self.DEFAULT_CSV_DELIMITER)
        kwargs.setdefault('quotechar', self.DEFAULT_CSV_QUOTECHAR)
        kwargs.setdefault('quoting', self.DEFAULT_QUOTING)

        def save_operation():
            with self.lock:  # Ensure thread-safe access to the dataframe
                if os.path.exists(filepath) and not os.path.isfile(filepath):
                    raise ValueError(f"Error while saving dataset '{self.dataset_config.get_id()}'. The path '{filepath}' is not a valid file.")

                try:
                    self.df.to_csv(filepath, index=False, **kwargs)
                    # print(f"Successfully saved dataset '{self.dataset_config.get_id()}' to {filepath}\n")
                    logger.info(f"Successfully saved dataset '{self.dataset_config.get_id()}' to {filepath} in thread {threading.current_thread().name}.")
                except FileNotFoundError:
                    raise FileNotFoundError(f"Error while saving dataset '{self.dataset_config.get_id()}'. The directory for the file '{filepath}' does not exist.")
                except PermissionError:
                    raise PermissionError(f"Error while saving dataset '{self.dataset_config.get_id()}'. Permission denied: Unable to write to '{filepath}'.")

            return filepath

        if use_taskmgr:
            task_mgr.submit(save_operation)  # use task manager
        else:
            save_operation()  # run in the main thread


    @requires_loaded_data                   # checks if dataframe self.df has data loaded (empty is also ok)    
    def validate_mandatory_fields(self):
        """
        Validate if mandatory column labels (as defined in dataset config) are present in self.df.
        Returns True if they are, throws a custom exception if not.
        """
        
        missing_labels = set(self.dataset_config.mandatory_fields) - set(self.df.columns)

        if missing_labels:
            raise DatasetMandatoryFieldsMissing(missing_fields=missing_labels, dataset_id=self.dataset_config.get_id())
            return False
                    
        return True


    @requires_loaded_data    
    def add_default_columns(self, column_dict=DEFAULT_COLUMNS):
        # adds default columns to the dataset, using a dictionary: column name as a key, init value as value
        # default is DEFAULT_COLUMNS
        # if not self.is_loaded:  
        #     raise ValueError(f"Data not loaded into '{self.dataset_config.get_id()}' .")

        pass


    @requires_loaded_data
    def rename_columns(self, column_dict):
        # renames columns in dataset using a dictionary: key is the old (current) column name, value is the new column name
        # example: rename_columns(column_dict={'technical.and.ugly.name': 'Cool_User_Friendly_name})
        # if not self.is_loaded:  
        #     raise ValueError(f"Data not loaded into '{self.dataset_config.get_id()}' .")

        pass

    @requires_loaded_data
    def add_load_date(self, date_col=LOAD_DATE_COL, date_format="%Y-%m-%d"):
        # updates desired column with current (load) date in specified format
        # default format is YYYY-MM-DD, default column is stored in a constant LOAD_DATE_COL
        # the dataframe that is updated, is instance attribute self.df
        # if not self.is_loaded:  
        #     raise ValueError(f"Data not loaded into '{self.dataset_config.get_id()}' .")
        
        try:
            # Test the format with the current date
            current_date = datetime.now().strftime(date_format)
        except ValueError as e:
            raise ValueError(f"Invalid date format '{date_format}': {e}")        

        self.df[date_col] = current_date


    @requires_loaded_data
    def add_timestamps(self, input_col, output_col, datetime_format=None, default_year=1970):
        """
        adds a column (output_col) containing Unix timestamps derived from the text values in input column (input_col).
        uses datetime format set for the class, which can be overriden using datetime_format variable.
        
        Args:
            input_column (str): Name of the column containing datetime strings.
            output_column (str): Name of the new column to store timestamps (epoch time).
            datetime_format (str, optional): The format to use for parsing datetime strings.
                                             Overrides class-level time_format if provided.
            default_year (int, optional): Default year to assign to parsed dates if year is missing in the data.
                                        Use only if you need to put the data within a specific year.
                                        and note that your data may not cross over the change of the year.
            
        Caveat! If the data source lacks year, and the data crosses the turn of the year (eg, data starts in December, and ends in January next year),
        records from January will be treated as if they were created before December.
        And it may indeed be the case, e.g., if the data source provides the more recent records first.
        Also, the sort order of records in the input data may not even be sorted in any chronological order. 
        Hence, go proactive and request your sources to provide complete dates for their records. :)
         
        """
        # if not self.is_loaded:  
        #     raise ValueError(f"Data not loaded into '{self.dataset_config.get_id()}' .")

        
        UNIX_EPOCH_START = "1970-01-01"         # start of Unix epoch, a reference date to calculate unix timestamps.
        
        if input_col not in self.df:
            raise ValueError(f"Input column '{input_col}' not found in dataset '{self.get_id()}'.")

        # if date time format is specified, override the default.
        datetime_format_to_use = datetime_format if datetime_format else self.DEFAULT_DATE_FORMAT

        # set a flag to see if the date time format contains a year
        # sometimes the input data may lack it - then we'll use some sort of default year (default_year parameter)
        contains_year = '%Y' in datetime_format_to_use or '%y' in datetime_format_to_use

        # convert from string into pandas datetime format.
        datetimes = pd.to_datetime(self.df[input_col], format=datetime_format_to_use, errors='coerce')

        if not contains_year:
            # Shift the year vectorized for all rows where the year is 1900
            datetimes = datetimes + pd.offsets.DateOffset(years=default_year - 1900)    # 1900 is the default year pandas uses.

        # print(datetimes.head(10))
        timestamps = (datetimes - pd.Timestamp(UNIX_EPOCH_START)) // pd.Timedelta(seconds=1)


        # convert to unix timestamp.
        self.df[output_col] = timestamps



    @requires_loaded_data
    def search(self, search_pattern, return_col):
        """
        Args:
            search_pattern (dict): search pattern, defined as a dictionary. 
                                    Dictionary keys are labels for columns where to search, 
                                    values are lists of values to search for. 
            return_col (str): label for the column from which to return the search results
            
        Returns:
            list: list of values from return_col for rows that match the search pattern.
                  Empty list if there are no matches.

        """

        if return_col not in self.df.columns:
            raise KeyError(f"The column for return data '{return_col}' does not exist in the DataFrame.")

        missing_cols = set(search_pattern.keys()) - set(self.df.columns)
        if missing_cols:
            raise SearchColumnsMissing(missing_fields=missing_cols, dataset_id=self.get_id())

        # Start with all rows
        matching_rows = self.df

        for col, values in search_pattern.items():
            
            # Handle NaN values in both the column and search values
            search_values = [np.nan if pd.isna(v) else v for v in values]
            
            # Match rows that satisfy the search pattern
            matching_rows = matching_rows[
                matching_rows[col].isin(search_values)
            ]

        # Return values from return_col, handling NaNs gracefully
        return matching_rows[return_col].dropna().tolist()

    
    def get_id(self):
        # returns dataset id as set in dataset config.
        return self.dataset_config.get_id()


    def data_cleanup(self):
        """
        entry point for data cleanup, must be implemented through child classes.
        
        """
        raise NotImplementedError("Cannot be used with BaseDataset class - pls implement the method through a child class.")



    def calculate_features(self):
        """
        entry point for feature engineering, must be implemented through child classes.
        
        """
        raise NotImplementedError("Cannot be used with BaseDataset class - pls implement the method through a child class.")
    
    
class DatasetConfig:
    # class to store dataset parameters
    def __init__(self, dataset_id, data_input_path, data_output_path, mandatory_fields, 
                 data_input_format="csv", data_output_format="csv", immediately_load_data = True):
        
        self.dataset_id = dataset_id
        self.data_input_path = data_input_path
        self.data_input_format = data_input_format
        self.data_output_path = data_output_path
        self.data_output_format = data_output_format
        self.mandatory_fields = mandatory_fields
        self.load_data = immediately_load_data
        pass
    
    def get_id(self):
        return self.dataset_id
    
    def get_input_path(self):
        return self.data_input_path
    
    def get_output_path(self):
        return self.data_output_path
    
    def load_data(self):
        return self.load_data
    