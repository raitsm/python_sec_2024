# Assignment 3 
# Demonstrate use of the following Python data types
# List
# Dictionary
# Set
# Tuple
HIGH_PRIVILEGES = ['admin', 'manager'] 
USERID_FIELD = 'user_id'
NAME_FIELD = 'name'
PRIVS_FIELD = 'privileges'
ACCOUNT_STAT_FIELD = 'account_enabled'

# userlists for system A and B were created using chatgpt (I do not like monkey jobs), the idea and the rest of the code are all mine.

# user records for system A
system_a_users = [
    {USERID_FIELD: 'alicjohn', NAME_FIELD: 'Alice Johnson', PRIVS_FIELD: ['user', 'analyst','operator'], 'last-login': '2024-11-10', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 20},
    {USERID_FIELD: 'bobsmit', NAME_FIELD: 'Bob Smith', PRIVS_FIELD: ['manager', ], 'last-login': '2024-11-18', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 30},
    {USERID_FIELD: 'charbrow', NAME_FIELD: 'Charlie Brown', PRIVS_FIELD: ['operator'], 'last-login': '2024-10-15', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 10},
    {USERID_FIELD: 'dianprin', NAME_FIELD: 'Diana Prince', PRIVS_FIELD: ['admin'], 'last-login': '2024-11-01', ACCOUNT_STAT_FIELD: False, 'days_till_password_change': 15},
    {USERID_FIELD: 'eveadam', NAME_FIELD: 'Eve Adams', PRIVS_FIELD: ['auditor', 'analyst'], 'last-login': '2024-11-12', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 25},
    {USERID_FIELD: 'francast', NAME_FIELD: 'Frank Castle', PRIVS_FIELD: ['user'], 'last-login': '2024-11-11', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 14},
    {USERID_FIELD: 'grachopp', NAME_FIELD: 'Grace Hopper', PRIVS_FIELD: ['operator'], 'last-login': '2024-11-16', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 7},
    {USERID_FIELD: 'hankpym', NAME_FIELD: 'Hank Pym', PRIVS_FIELD: ['user'], 'last-login': '2024-11-14', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 21},
    {USERID_FIELD: 'ivygree', NAME_FIELD: 'Ivy Green', PRIVS_FIELD: ['manager'], 'last-login': '2024-11-20', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 19},
    {USERID_FIELD: 'jackryan', NAME_FIELD: 'Jack Ryan', PRIVS_FIELD: ['auditor'], 'last-login': '2024-11-05', ACCOUNT_STAT_FIELD: False, 'days_till_password_change': 8},
    {USERID_FIELD: 'karadanv', NAME_FIELD: 'Kara Danvers', PRIVS_FIELD: ['user','analyst'], 'last-login': '2024-11-09', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 12},
    {USERID_FIELD: 'leofitz', NAME_FIELD: 'Leo Fitz', PRIVS_FIELD: ['admin'], 'last-login': '2024-11-03', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 18},
    {USERID_FIELD: 'miawong', NAME_FIELD: 'Mia Wong', PRIVS_FIELD: ['operator'], 'last-login': '2024-11-08', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 27},
    {USERID_FIELD: 'ninasimo', NAME_FIELD: 'Nina Simone', PRIVS_FIELD: ['auditor'], 'last-login': '2024-11-07', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 5},
    {USERID_FIELD: 'oscarisa', NAME_FIELD: 'Oscar Isaac', PRIVS_FIELD: ['user'], 'last-login': '2024-11-06', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 11},
    {USERID_FIELD: 'paulatre', NAME_FIELD: 'Paul Atreides', PRIVS_FIELD: ['manager','analyst'], 'last-login': '2024-11-17', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 13},
    {USERID_FIELD: 'quinmall', NAME_FIELD: 'Quinn Mallory', PRIVS_FIELD: ['operator'], 'last-login': '2024-11-19', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 22},
    {USERID_FIELD: 'rachadam', NAME_FIELD: 'Rachel Adams', PRIVS_FIELD: ['admin'], 'last-login': '2024-11-04', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 16},
    {USERID_FIELD: 'samwils', NAME_FIELD: 'Sam Wilson', PRIVS_FIELD: ['auditor'], 'last-login': '2024-11-13', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 9},
]

# user records for system B
system_b_users = [
    {USERID_FIELD: 'tinagold', NAME_FIELD: 'Tina Goldstein', PRIVS_FIELD: ['user'], 'last-login': '2024-11-10', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 14},
    {USERID_FIELD: 'umathur', NAME_FIELD: 'Uma Thurman', PRIVS_FIELD: ['manager', 'operator'], 'last-login': '2024-11-11', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 17},
    {USERID_FIELD: 'victston', NAME_FIELD: 'Victor Stone', PRIVS_FIELD: ['operator'], 'last-login': '2024-11-15', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 10},
    {USERID_FIELD: 'wandmaxi', NAME_FIELD: 'Wanda Maximoff', PRIVS_FIELD: ['admin'], 'last-login': '2024-11-18', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 12},
    {USERID_FIELD: 'xandcage', NAME_FIELD: 'Xander Cage', PRIVS_FIELD: ['auditor', 'manager'], 'last-login': '2024-11-12', ACCOUNT_STAT_FIELD: False, 'days_till_password_change': 8},
    {USERID_FIELD: 'yaragrey', NAME_FIELD: 'Yara Greyjoy', PRIVS_FIELD: ['user'], 'last-login': '2024-11-13', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 20},
    {USERID_FIELD: 'zarakhan', NAME_FIELD: 'Zara Khan', PRIVS_FIELD: ['operator'], 'last-login': '2024-11-14', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 11},
    {USERID_FIELD: 'adamstra', NAME_FIELD: 'Adam Strange', PRIVS_FIELD: ['manager'], 'last-login': '2024-11-09', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 15},
    {USERID_FIELD: 'paulatre', NAME_FIELD: 'Paul Atreides', PRIVS_FIELD: ['user'], 'last-login': '2024-11-21', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 18},
    {USERID_FIELD: 'quinmall', NAME_FIELD: 'Quinn Mallory', PRIVS_FIELD: ['auditor'], 'last-login': '2024-11-19', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 24},
    {USERID_FIELD: 'rachadam', NAME_FIELD: 'Rachel Adams', PRIVS_FIELD: ['manager'], 'last-login': '2024-11-05', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 16},
    {USERID_FIELD: 'samwils', NAME_FIELD: 'Sam Wilson', PRIVS_FIELD: ['operator'], 'last-login': '2024-11-13', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 9},
    {USERID_FIELD: 'buckbarn', NAME_FIELD: 'Bucky Barnes', PRIVS_FIELD: ['admin','analyst'], 'last-login': '2024-11-16', ACCOUNT_STAT_FIELD: True, 'days_till_password_change': 5},
]


all_systems_users = [system_a_users, system_b_users]


def find_common_users(system_a, system_b):

    # find common users in system A and B, based on combination of USERID_FIELD and name.
    # shows use of sets, tuples, lists and dictionaries

    system_a_set = {(user[USERID_FIELD], user[NAME_FIELD]) for user in system_a_users}
    system_b_set = {(user[USERID_FIELD], user[NAME_FIELD]) for user in system_b_users}

    common_users = system_a_set.intersection(system_b_set)

    return common_users

def find_disabled_users(system_userlist):
    # shows list comprehension and use of dictionary and Boolean data types
    disabled_users = [ user[USERID_FIELD] for user in system_userlist if not user[ACCOUNT_STAT_FIELD]]
    return disabled_users


def main():
    users_ab = find_common_users(system_a=system_a_users, system_b=system_b_users)
    print('Users present in both system A and B')
    for user in users_ab:
        print(user[0])
    
    print('disabled users in System A')
    disabled_A = find_disabled_users(system_userlist=system_a_users)
    print(disabled_A)

main()
