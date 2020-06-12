# -*- coding: utf-8 -*-
"""
Created on 2020-04-11 15:28

@author: christopherwong

Requires:
`credentials.json` from the GSuite API
`envvars.json` containing MySQL and AWS credentials and variables
"""
import datafeeddashboard.GenericDashboardService as qs
import json
import os

# Load other necessary S3 and DB credentials/variables into os module
with open("envvars.json") as envVarFile:
    envVars = json.load(envVarFile)updat
    os.environ.update(envVars)

def main():
    gsheet = qs.gSheetServiceConnection()
    qs.updateLastFileReceipt(gsheet)
    qs.updateLastAccountUpdate(gsheet)
    qs.updateReconciliationBreaks(gsheet)

if __name__ == "__main__":
    main()