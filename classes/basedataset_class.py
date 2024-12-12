# base dataset classs that provides basic i/o and data cleanup functions
import pandas as pd
import numpy as np
import csv
import os
from datetime import datetime

from constants import UNKNOWN
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
    
    def __init__(self, dataset_config: 'DatasetConfig', type="csv"):
        """
        Initialize a dataset using dataset configuration.
        Assumes the source is a csv file.
        Validates if mandatory fields are present in the loaded file.
        
        Args:
            dataset_config (DatasetConfig): dataset configuration object
            
        """
        
        self.dataset_config = dataset_config
        
        if self.dataset_config.data_input_format.lower() == "csv":
            self.df = self.from_csv(self.dataset_config.get_input_path())
        elif self.dataset_config.data_input_format.lower() == "opensshlog":
            raise ValueError(f"something off in dataframe '{self.dataset_config.get_id()}' - parent class init shall not be used with openssh.")
        else:
            raise ValueError(f"unknown input file type specified")
        
        self.validate_mandatory_fields()
    
    
    def parse_log(self):
        """
        The method shall be used only for basedataset child clases to import data from sources using a specific format, e.g., from a logfile.
        """
        raise 
    
    def from_csv(self, filepath, **kwargs):
        # creates a BaseDataset from a csv file; uses class values
        
        # validates if the file specified is a file and if it exists
 
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Error while loading dataset '{self.dataset_config.get_id()}'. The source file '{filepath}' does not exist.")
        if not os.path.isfile(filepath):
            raise ValueError(f"Error while loading dataset '{self.dataset_config.get_id()}'. The source file specified '{filepath}' is not a file.")
      
        
        kwargs.setdefault('encoding', self.DEFAULT_ENCODING)
        kwargs.setdefault('delimiter', self.DEFAULT_CSV_DELIMITER)
        kwargs.setdefault('quotechar', self.DEFAULT_CSV_QUOTECHAR)
        kwargs.setdefault('dtype', self.DEFAULT_DATA_TYPE)
 
        # check for most common issues related to Pandas dataframes
        try:
            dataframe = pd.read_csv(filepath, **kwargs)
        except pd.errors.EmptyDataError as e:
            raise ValueError(f"Error while loading dataset '{self.dataset_config.get_id()}'. The file '{filepath}' is empty or invalid: {e}")
        except pd.errors.ParserError as e:
            raise ValueError(f"Error while loading dataset '{self.dataset_config.get_id()}'. Parsing error while reading the file '{filepath}': {e}")
 
        dataframe = pd.read_csv(filepath, **kwargs)
        return dataframe


    def save_as_csv(self, filepath=None, **kwargs):
       
        """
        Save the dataframe to a CSV file.
        Default parameters are provided through constants but can be overriden.

        Args:
            file_path (_type_): _description_
        """
        if filepath is None:
            filepath = self.dataset_config.data_output_path

        # If the file already exists, ensure it's a file
        if os.path.exists(filepath) and not os.path.isfile(filepath):
            raise ValueError(f"Error while saving dataset '{self.dataset_config.get_id()}'. The path '{filepath}' is not a file.")
 
        kwargs.setdefault('encoding', self.DEFAULT_ENCODING)
        kwargs.setdefault('sep', self.DEFAULT_CSV_DELIMITER)
        kwargs.setdefault('quotechar', self.DEFAULT_CSV_QUOTECHAR)
        kwargs.setdefault('quoting', self.DEFAULT_QUOTING)
        
        try:
            self.df.to_csv(filepath, index=False, **kwargs)
        except FileNotFoundError:
            raise FileNotFoundError(f"Error while saving dataset '{self.dataset_config.get_id()}'. The directory for the file '{filepath}' does not exist.")
        
        except PermissionError:
            raise PermissionError(f"Error while saving dataset '{self.dataset_config.get_id()}'. Permission denied: Unable to write to '{filepath}'.")

        pass
    
    
    def validate_mandatory_fields(self):
        """
        Validate if mandatory column labels (as defined in dataset config) are present in self.df.
        Returns True if they are, throws a custom exception if not.
        """
        # check if there is a dataframe in the dataset
        if not hasattr(self, 'df'):
            raise ValueError(f"Dataset not present in '{self.dataset_config.get_id()}'")
        
        missing_labels = set(self.dataset_config.mandatory_fields) - set(self.df.columns)

        if missing_labels:
            raise DatasetMandatoryFieldsMissing(missing_fields=missing_labels, dataset_id=self.dataset_config.get_id())
            return False
                    
        return True
    
    def add_default_columns(self, column_dict=DEFAULT_COLUMNS):
        # adds default columns to the dataset, using a dictionary: column name as a key, init value as value
        # default is DEFAULT_COLUMNS
        pass
    
    def rename_columns(self, column_dict):
        # renames columns in dataset using a dictionary: key is the old (current) column name, value is the new column name
        # example: rename_columns(column_dict={'technical.and.ugly.name': 'Cool_User_Friendly_name})
        pass
    
    def add_load_date(self, date_col=LOAD_DATE_COL, date_format="%Y-%m-%d"):
        # updates desired column with current (load) date in specified format
        # default format is YYYY-MM-DD, default column is stored in a constant LOAD_DATE_COL
        # the dataframe that is updated, is instance attribute self.df
        
        try:
            # Test the format with the current date
            current_date = datetime.now().strftime(date_format)
        except ValueError as e:
            raise ValueError(f"Invalid date format '{date_format}': {e}")        

        self.df[date_col] = current_date


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

        # if input data contains no year, use default year. NB, use with caution!
        # if not contains_year:
        #     default_year_start = (pd.Timestamp(f"{default_year}-01-01") - pd.Timestamp(UNIX_EPOCH_START)) // pd.Timedelta(seconds=1)
        #     timestamps += default_year_start

        if not contains_year:
            # Shift the year vectorized for all rows where the year is 1900
            datetimes = datetimes + pd.offsets.DateOffset(years=default_year - 1900)    # 1900 is the default year pandas uses.

        # print(datetimes.head(10))
        timestamps = (datetimes - pd.Timestamp(UNIX_EPOCH_START)) // pd.Timedelta(seconds=1)


        # convert to unix timestamp.
        self.df[output_col] = timestamps




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
                 data_input_type="csv", data_output_type="csv"):
        
        self.dataset_id = dataset_id
        self.data_input_path = data_input_path
        self.data_input_format = data_input_type
        self.data_output_path = data_output_path
        self.data_output_format = data_output_type
        self.mandatory_fields = mandatory_fields
        pass
    
    def get_id(self):
        return self.dataset_id
    
    def get_input_path(self):
        return self.data_input_path
    
    def get_output_path(self):
        return self.data_output_path
    
    