# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 18:57 2020

@author: christopherwong
"""

import pandas as pd
import os
import datetime
import numpy as np
import datafeeddashboard.utils.PriveSQL as pql
from datafeeddashboard.utils import FeedSettings as fs

def outstandingParsingErrorsImpl():
    print('=======Staring OutstandingParsingErrors.py=======')
    sqlConn = pql.mySqlDatabase()
    fsobj = fs.FeedSettings()

    for feedKey, feedValue in fsobj.getAllFeedsDict().items():
        # Messed up part is that some accounts are manually updated using Datafeed Type `Prive`
        print('====Handling Feed {0}:{1}===='.format(feedKey, feedValue['Constant']))
        lastInvestorAccountInfo = sqlConn.executeQuery(pql.generatePositionQueries \
                                                       .getLastInvestorAccountInformation(feedSource=feedValue['Constant']))
        print('For feed `{0}`, {1} rows retrieved'.format(feedKey, str(lastInvestorAccountInfo.shape[0])))
        lastInvestorAccountInfo['BUSDAYDIFF'] = np.busday_count(datetime.datetime.now() -
                                                datetime.datetime.strptime(lastInvestorAccountInfo['VALUEDATE'], '%Y-%m-%d %H:%M:%S.%f'))
        lastInvestorAccountInfo = lastInvestorAccountInfo[lastInvestorAccountInfo['BUSDAYDIFF'] <= -4]
        lastInvestorAccountInfo['SOURCENAME'] = feedKey
        yield lastInvestorAccountInfo
    print('=======Completed OutstandingParsingErrors.py run=======')