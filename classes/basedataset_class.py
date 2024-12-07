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
    
    LOAD_DATE_COL = "Load_Date"
    
    # default columns with default values that shall be added once data is loaded.
    DEFAULT_COLUMNS = {
        LOAD_DATE_COL: UNKNOWN,
    }
    
    def __init__(self, dataset_config: 'DatasetConfig'):
        """
        Initialize a dataset using dataset configuration.
        Assumes the source is a csv file.
        Validates if mandatory fields are present in the loaded file.
        
        Args:
            dataset_config (DatasetConfig): dataset configuration object
            
        """
        
        self.dataset_config = dataset_config
        self.df = self.from_csv(self.dataset_config.get_input_path())
        self.validate_mandatory_fields()

    
    
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


    def save_as_csv(self, filepath, **kwargs):
       
        """
        Save the dataframe to a CSV file.
        Default parameters are provided through constants but can be overriden.

        Args:
            file_path (_type_): _description_
        """

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
    
    
class DatasetConfig:
    # class to store dataset parameters
    def __init__(self, dataset_id, data_input_path, data_output_path, mandatory_fields, 
                 data_input_type="csv", data_output_type="csv"):
        
        self.dataset_id = dataset_id
        self.data_input_path = data_input_path
        self.data_input_type = data_input_type
        self.data_output_path = data_output_path
        self.data_output_type = data_output_type
        self.mandatory_fields = mandatory_fields
        pass
    
    def get_id(self):
        return self.dataset_id
    
    def get_input_path(self):
        return self.data_input_path
    
    def get_output_path(self):
        return self.data_output_path
    
    