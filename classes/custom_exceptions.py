# classes/custom_exceptions.py
# module to store custom exceptions defined for the project.

class DatasetMandatoryFieldsMissing(Exception):
 
    def __init__(self, missing_fields, dataset_id=""):
        # self.missing_fields = missing_fields
        message = f"Dataset '{dataset_id}' The following mandatory fields are missing in dataset: {', '.join(missing_fields)}"
        super().__init__(message)

class SearchColumnsMissing(Exception):
 
    def __init__(self, missing_fields, dataset_id=""):
        # self.missing_fields = missing_fields
        message = f"Dataset '{dataset_id}' The following search columns could not be found in dataset: {', '.join(missing_fields)}"
        super().__init__(message)

