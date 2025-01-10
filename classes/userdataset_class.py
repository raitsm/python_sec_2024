# classes/userdataset_class.py

# user dataset class that includes methods specific for user data
# inherits from BaseDataset class.


from classes.logger import Logger
logger = Logger().get_logger()

from classes.task_manager import TaskManager
# task_mgr = TaskManager()
task_mgr = TaskManager.get_taskmgr(max_threads=2)


from classes.basedataset_class import BaseDataset, DatasetConfig
from decorators import requires_loaded_data

class UserDataset(BaseDataset):
    

    USER_ID_COL = 'user_id'
    
    def __init__(self, dataset_config: DatasetConfig):
        super().__init__(dataset_config=dataset_config)


    @requires_loaded_data
    def data_cleanup(self):
        """
        clean up erroneous and unnecessary data, process blank values, bring the datetime formats to a common standard.
        """
        # if not self.is_loaded:  
        #     raise ValueError(f"Data not loaded into '{self.dataset_config.get_id()}' .")
        
        print("no cleanup required this time!")
        pass

    @requires_loaded_data
    def calculate_features(self):
        """
        a collection of code that updates self.df by adding new features (counts, encoded values, scores)
        """
        # if not self.is_loaded:  
        #     raise ValueError(f"Data not loaded into '{self.dataset_config.get_id()}' .")

        self.add_load_date()


    @requires_loaded_data
    def get_all_users(self, user_id_col=USER_ID_COL):
        # returns a pandas series containing all user id's in the dataset
        # if not self.is_loaded:  
        #     raise ValueError(f"Data not loaded into '{self.dataset_config.get_id()}' .")

        return self.df[user_id_col]
        
        
        