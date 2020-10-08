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
import sys

# Load other necessary S3 and DB credentials/variables into os module
with open("envvars.json") as envVarFile:
    envVars = json.load(envVarFile)
    os.environ.update(envVars)

def main(cli_args):
    gsheet = qs.gSheetServiceConnection()
    dispatch_map = {'LastFileReceipt': qs.updateLastFileReceipt,
                    'LastAccountUpdate': qs.updateLastAccountUpdate,
                    'ReconciliationBreaks': qs.updateReconciliationBreaks,
                    }
    if len(cli_args) == 0:
        for k,v in dispatch_map.items():
            eval("v(gsheet)")
    else:
        if len(set(cli_args) - set(dispatch_map.keys())) > 0:
            raise KeyError('{} invalid. Only accept arguments: {}'.format(\
                list(set(cli_args) - set(dispatch_map.keys())), list(dispatch_map.keys())))
        else:
            for arg in cli_args:
                eval("dispatch_map[arg](gsheet)")


if __name__ == "__main__":
    main(sys.argv[1:])