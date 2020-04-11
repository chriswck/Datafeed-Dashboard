# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 18:57 2020

@author: christopherwong
"""

import pandas as pd
import os
import datafeeddashboard.utils.PriveSQL as pql
from datafeeddashboard.utils import FeedSettings as fs

def lastAccounUpdateImpl():
    '''
    :return: pandas dataframe from MySQL query
    '''
    print('=======Staring LastAccountUpdate.py=======')
    sqlConn = pql.mySqlDatabase()
    fsobj = fs.FeedSettings()

    for feedKey, feedValue in fsobj.getAllFeedsDict().items():
        # Messed up part is that some accounts are manually updated using Datafeed Type `Prive`
        print('====Handling Feed {0}:{1}===='.format(feedKey, feedValue['Constant']))
        lastInvestorAccountInfo = sqlConn.executeQuery(pql.generatePositionQueries \
                                                       .getLastInvestorAccountInformation(feedSource=feedValue['Constant']))
        print('For feed `{0}`, {1} rows retrieved'.format(feedKey, str(lastInvestorAccountInfo.shape[0])))
        lastInvestorAccountInfo = lastInvestorAccountInfo[lastInvestorAccountInfo['DATEDIFF'] <= -5]
        lastInvestorAccountInfo['SOURCENAME'] = feedKey
        yield lastInvestorAccountInfo
    print('=======Completed LastAccountUpdate.py run=======')

def execute():
    '''
    :return: Executes implementation and returns list of values for the body for a single API call
    '''
    responseList = list()
    for eachdf in lastAccounUpdateImpl():
        if len(responseList) == 0:
            colnames = eachdf.columns.to_list()
            responseList.append(colnames)
        responseList.extend(eachdf.values.tolist())
    outputList = responseList
    return outputList

if __name__ == "__main__":
    execute()

