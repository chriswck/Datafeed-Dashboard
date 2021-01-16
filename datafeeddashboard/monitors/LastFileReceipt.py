# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 14:11:20 2019

@author: christopherwong
"""

import pandas as pd
import datetime
import boto3
import os
import datafeeddashboard.utils.PriveSQL as pql
import datafeeddashboard.utils.FeedSettings as fs
import re

def lastFileReceiptImpl():
    '''
    :return: List of elements showing last File Receipt Details
    '''
    print('=======Staring LastFileReceipt.py=======')
    s3c = boto3.client('s3', aws_access_key_id = os.environ['AWS_KEY_ACCESS'],
                       aws_secret_access_key = os.environ['AWS_KEY_SECRET'])
    sqlConn = pql.mySqlDatabase()
    activeCompanies = sqlConn.executeQuery(pql.generateStaticDataQueries.getActiveAdvisorCompanies())
    fsobj = fs.FeedSettings()
    feeds = fsobj.getAllFeedsNames()

    def treeLookup(maxDepth, args):
        if maxDepth == 0:
            return args
        else:
            try:
                response = s3c.list_objects_v2(Bucket=os.environ['AWS_BUCKET_PRIVE_ETL'],
                                               Prefix='/'.join(args) + '/', Delimiter='/')

                while response.get('NextContinuationToken') is not None:
                    response = s3c.list_objects_v2(Bucket=os.environ['AWS_BUCKET_PRIVE_ETL'],
                                                   Prefix='/'.join(args) + '/', Delimiter='/',
                                                   ContinuationToken=response['NextContinuationToken'])

                maxPathKeys = response['CommonPrefixes']
                maxKeys = [maxPathKeys[i]['Prefix'].split('/')[-2] for i in range(len(maxPathKeys))]
                maxKey = max(list(filter(lambda x: re.search('^[0-9]*$', str(x)), maxKeys)))
                args.append(maxKey)
                # print('About to pass argument to next recursive tree call: ' + str(args))
                return treeLookup(maxDepth - 1, args)
            except KeyError:
                return 'No Files Found'

    for feed in feeds:
        print('====Handling Feed {}===='.format(feed))

        # EAM Custodian Feeds
        if feed in fsobj.getEAMFeedNames():
            maxDepth = fsobj.getAllFeedsDict()[feed]['maxDepth']
            pathKeys = s3c.list_objects_v2(Bucket=os.environ['AWS_BUCKET_PRIVE_ETL'],
                                           Prefix='{}/{}/'.format(feed, 'archive'), Delimiter='/')['CommonPrefixes']
            companyKeys = [pathKeys[i]['Prefix'].split('/')[-2] for i in range(len(pathKeys))]
            for comKey in companyKeys:
                if comKey not in list(activeCompanies['KEY']):
                    continue
                print('Handling Feed {} Company {}'.format(feed, comKey))
                lookupResult = treeLookup(maxDepth, [feed, 'archive', comKey])
                yield [feed, comKey, activeCompanies[activeCompanies['KEY']==comKey]['NAME'].iloc[0],
                       ''.join(lookupResult[-maxDepth:]), str(datetime.datetime.now())]

        # IFA Provider Feeds
        elif feed in fsobj.getIFAFeedNames():
            maxDepth = fsobj.getAllFeedsDict()[feed]['maxDepth']
            archiveKeyStr = 'archive' if fsobj.getAllFeedsDict()[feed]['archiveKeyPath'] else ''

            if fsobj.getAllFeedsDict()[feed]['comKeyKeyPath']:
                # Grab the company keys
                pathKeys = s3c.list_objects_v2(Bucket=os.environ['AWS_BUCKET_PRIVE_ETL'],
                                               Prefix='{}/{}/'.format(feed, archiveKeyStr), Delimiter='/')['CommonPrefixes']
                companyKeys = [pathKeys[i]['Prefix'].split('/')[-2] for i in range(len(pathKeys))]
                for comKey in companyKeys:
                    if comKey not in list(activeCompanies['KEY']):
                        continue
                    print('Handling Feed {} Company {}'.format(feed, comKey))
                    lookupResult = treeLookup(maxDepth, [feed, archiveKeyStr, comKey])
                    yield [feed, comKey, activeCompanies[activeCompanies['KEY']==comKey]['NAME'].iloc[0],
                           ''.join(lookupResult[-maxDepth:]), str(datetime.datetime.now())]
            else:
                print('Handling Feed {} No Company Key Path'.format(feed))
                lookupResult = treeLookup(maxDepth, [feed, archiveKeyStr, comKey])
                yield [feed, comKey, activeCompanies[activeCompanies['KEY'] == comKey]['NAME'].iloc[0],
                       ''.join(lookupResult[-maxDepth:]), str(datetime.datetime.now())]

        # Enterprise Feeds
        elif feed in fsobj.getEnterpriseFeedNames():
            maxDepth = fsobj.getAllFeedsDict()[feed]['maxDepth']
            lookupResult = treeLookup(maxDepth, [feed, 'archive'])
            yield [feed, fsobj.feeds[feed]['comKey'],
                   activeCompanies[activeCompanies['KEY']==fsobj.feeds[feed]['comKey']]['NAME'].iloc[0],
                   ''.join(lookupResult[-maxDepth:]), str(datetime.datetime.now())]
        else:
            yield ['Something Wrong']

    print('=======Completed LastFileReceipt.py run=======')

def execute():
    '''
    :return: Executes implementation and returns list of values to write to a sheet in one API call
    '''
    responseList = list()
    colnames = ['S3 Feed Bucket', 'Company Key', 'Company Name', 'LastestFolder', 'UpdateTimeStamp']

    for each in lastFileReceiptImpl():
        print(each)
        each = each + ['null'] * (len(colnames) - len(each))
        responseList.append(each)

    outputdf = pd.DataFrame(data=responseList, columns=colnames, dtype = str)
    outputList = outputdf.values.tolist()
    outputList.insert(0, colnames)
    print(outputList)
    return outputList

if __name__ == "__main__":
    execute()

