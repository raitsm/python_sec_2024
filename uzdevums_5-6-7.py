# Assignment 5
# Demonstrate a use of Class in Python with a few methods
# For advanced example use of inheritance can be demonstrated.

# Assignment 6
# Demonstrate use of Exception
# Can you write your own defined exception?..

# Assignment 7
# Explore the use of modules and demonstrate with example
# Demonstrate the use of packages by organizing your written modules in a package


from constants import *
from classes.basedataset_class import BaseDataset, DatasetConfig
from classes.userdataset_class import UserDataset


# def get_early_password_warnings(system_userlist, days_before):

#     # demonstrates a regular for loop    
#     result = []
#     for user in system_userlist:
#         if user.get(DAYS_TILL_PWD_CHANGE_FIELD) < days_before:
#             result.append(user.get(USERID_FIELD))

#     return result


def main():
    user_fields = ["user_id", "privileges", "account_enabled"]

    # define configuration parameters for datasets
    user_config_a = DatasetConfig(dataset_id="system A users", data_input_path="./system_a_users.csv",
                                data_output_path="./system_a_users_OUT.csv",
                                mandatory_fields=user_fields)

    user_config_b = DatasetConfig(dataset_id="system B users", data_input_path="./system_b_users.csv", 
                                data_output_path="./system_b_users_OUT.csv",
                                mandatory_fields=user_fields)

    # create the datasets; load data from the source file
    users_a = UserDataset(dataset_config=user_config_a)
    users_b = UserDataset(dataset_config=user_config_b)

    # add load date to system A users
    users_a.add_load_date()     # using default load date column and default date format

    # show users present in both system A and B 
    common_users = set(users_a.get_all_users()).intersection(set(users_b.get_all_users()))
    print(f"Users present in both {users_a.get_id()} and {users_b.get_id()}: {common_users}")

    # find disabled users in system A
    disabled_users_a = users_a.search(search_pattern={ACCOUNT_STAT_FIELD: ["False"]},return_col=USERID_FIELD)
    print(f"Disabled users in {users_a.get_id()}: {disabled_users_a}")
    # enhance the datasets by adding current date as the load date.
    
    
    # save any output
    users_a.save_as_csv(users_a.dataset_config.get_output_path())
    users_b.save_as_csv(users_b.dataset_config.get_output_path())

        
main()
