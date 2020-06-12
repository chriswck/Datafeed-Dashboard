# -*- coding: utf-8 -*-
"""
Created on 2020-06-12

@author: christopherwong
"""

import pandas as pd
import os
import datafeeddashboard.utils.PriveSQL as pql
from datafeeddashboard.utils import FeedSettings as fs

def portfolioMetricsImpl():
    '''
    :return: pandas dataframe from MySQL query
    '''
    print('=======Starting ReconciliationBreaks.py=======')
    sqlConn = pql.mySqlDatabase()
    fsobj = fs.FeedSettings()

    for feedKey, feedValue in fsobj.getAllFeedsDict().items():
        # Messed up part is that some accounts are manually updated using Datafeed Type `Prive`
        print('====Handling Feed {0}:{1}===='.format(feedKey, feedValue['Constant']))
        portfolioMetrics = sqlConn.executeQuery(pql.generatePositionQueries \
                                                .getPortfolioMetrics(feedSource=feedValue['Constant']))
        print('For feed `{0}`, {1} rows retrieved'.format(feedKey, str(portfolioMetrics.shape[0])))
        portfolioMetrics['SOURCENAME'] = feedKey
        yield portfolioMetrics
    print('=======Completed ReconciliationBreaks.py run=======')

def execute():
    '''
    :return: Executes implementation and returns list of values for the body for a single API call
    '''
    responseList = list()
    for eachdf in portfolioMetricsImpl():
        if len(responseList) == 0:
            colnames = eachdf.columns.to_list()
            responseList.append(colnames)
        responseList.extend(eachdf.values.tolist())
    outputList = responseList
    return outputList

if __name__ == "__main__":
    execute()

