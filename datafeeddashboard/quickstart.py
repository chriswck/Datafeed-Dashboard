# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START sheets_quickstart]
# Gsheet auth libraries
from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
# Wrangling required libraries
import os
import json
import pandas as pd
# Custom libraries
import datafeeddashboard.monitors.LastFileReceipt as lfr
import datafeeddashboard.monitors.LastAccountUpdate as lau
import datafeeddashboard.utils.SheetSettings as ss

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def gSheetServiceConnection():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    return sheet

def updateLastFileReceipt(gsheet):
    '''
    :param gsheet: googleapiclient.discovery.build Google API Service
    :return: Response from put request to Google API Service
    '''
    outputList = lfr.execute()

    # Clear Sheet of all values
    clearResponse = gsheet.values().clear(
        spreadsheetId=ss.SheetSettingsDict['LastAccountUpdate']['SPREADSHEET_ID'],
        range='{0}!B:Z'.format(ss.SheetSettingsDict['LastFileReceipt']['SPREADSHEET_TAB_NAME']),
        body={}).execute()
    print(clearResponse)

    # Put values to Sheet
    putResponse = gsheet.values().update(
        spreadsheetId=ss.SheetSettingsDict['LastFileReceipt']['SPREADSHEET_ID'],
        #The following range assumes identical number of elements per row. This will break eventually
        range='{0}!B1:{1}{2}'.format(ss.SheetSettingsDict['LastFileReceipt']['SPREADSHEET_TAB_NAME'],
                                     chr(max(2,64+len(outputList[0])+2)), # Get length of column names
                                     str(len(outputList))),
        valueInputOption='RAW',
        body={'values': outputList}).execute()
    print('{0} cells updated.'.format(putResponse.get('updatedCells')))
    return putResponse

def updateLastAccountUpdate(gsheet):
    '''
    :param gsheet: googleapiclient.discovery.build Google API Service
    :return: Response from put request to Google API Service
    '''
    outputList = lau.execute()
    print(pd.DataFrame(data=outputList[1:], columns=outputList[0]))

    # Clear Sheet of all values
    clearResponse = gsheet.values().clear(
        spreadsheetId=ss.SheetSettingsDict['LastAccountUpdate']['SPREADSHEET_ID'],
        range='{0}!B:Z'.format(ss.SheetSettingsDict['LastAccountUpdate']['SPREADSHEET_TAB_NAME']),
        body={}).execute()
    print(clearResponse)

    # Put values to Sheet
    putResponse = gsheet.values().update(
        spreadsheetId=ss.SheetSettingsDict['LastAccountUpdate']['SPREADSHEET_ID'],
        #The following range assumes identical number of elements per row. This will break eventually
        range='{0}!B1:{1}{2}'.format(ss.SheetSettingsDict['LastAccountUpdate']['SPREADSHEET_TAB_NAME'],
                                     chr(max(2, 64 + len(outputList[0]) + 2)),  # Get length of column names
                                     str(len(outputList))),
        valueInputOption='RAW',
        body={'values': outputList}).execute()
    print('{0} cells updated.'.format(putResponse.get('updatedCells')))
    return putResponse

if __name__ == '__main__':
    gsheet = gSheetServiceConnection()
    updateLastFileReceipt(gsheet)

# [END sheets quickstart]