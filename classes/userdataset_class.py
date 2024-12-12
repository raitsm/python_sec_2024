# user dataset class that includes methods specific for user data
# inherits from BaseDataset class.

from classes.basedataset_class import BaseDataset, DatasetConfig

class UserDataset(BaseDataset):
    

    USER_ID_COL = 'user_id'
    
    def __init__(self, dataset_config: DatasetConfig):
        super().__init__(dataset_config=dataset_config)
    
    
    def data_cleanup(self):
        """
        clean up erroneous and unnecessary data, process blank values, bring the datetime formats to a common standard.
        """
        print("no cleanup required this time!")
        pass

    def calculate_features(self):
        """
        a collection of code that updates self.df by adding new features (counts, encoded values, scores)
        """
        self.add_load_date()


    def get_all_users(self, user_id_col=USER_ID_COL):
        # returns a pandas series containing all user id's in the dataset
        return self.df[user_id_col]
        
        
        