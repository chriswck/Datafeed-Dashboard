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
import typer

# Load other necessary S3 and DB credentials/variables into os module
with open("envvars.json") as envVarFile:
    envVars = json.load(envVarFile)
    os.environ.update(envVars)

# Explicitly create typer.Typer app
app = typer.Typer()

@app.command()
def updateDashboard(sheet: str = typer.Argument(None,
                                                help='Acceptable values: \
                                                LastFileReceipt, \
                                                LastAccountUpdate, \
                                                ReconcilliationBreaks.')):
    gsheet = qs.gSheetServiceConnection()
    dispatch_map = {'LastFileReceipt': qs.updateLastFileReceipt,
                    'LastAccountUpdate': qs.updateLastAccountUpdate,
                    'ReconciliationBreaks': qs.updateReconciliationBreaks,
                    }
    if sheet is None:
        for k in dispatch_map.keys():
            dispatch_map[k](gsheet)
    else:
        dispatch_map[sheet](gsheet)

@app.command()
def triggerAlerts():
    return 'This is work in progress'

if __name__ == "__main__":
    app()
