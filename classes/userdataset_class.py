# user dataset class that includes methods specific for user data
# inherits from BaseDataset class.

from classes.basedataset_class import BaseDataset, DatasetConfig

class UserDataset(BaseDataset):
    

    USER_ID_COL = 'user_id'
    
    def __init__(self, dataset_config: DatasetConfig):
        super().__init__(dataset_config=dataset_config)
    

    def get_all_users(self, user_id_col=USER_ID_COL):
        # returns a pandas series containing all user id's in the dataset
        return self.df[user_id_col]
        
        
        