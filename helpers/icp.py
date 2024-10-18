import pandas as pd 

def identify_icp(row):
    """
    For given enriched user data, returns True/False for whether person is an ICP
    If True, also returns conditions they met as a str, "" if False
    """

    icp_flag = False
    icp_conditions = "Met the following criteria:\n\n"

    slack_user_id = row[0]
    estimated_num_employees = row[1]

    if not pd.isna(estimated_num_employees) :
        if int(estimated_num_employees) >= 10:
            icp_flag = True
            icp_conditions += "Estimated company size: {}".format(estimated_num_employees)
    #add any number of conditions here

    return icp_flag, icp_conditions if icp_flag else ""