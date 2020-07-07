# -*- coding: utf-8 -*-
"""
Created on Tue Sep  3 10:49:14 2019

@author: christopherwong
"""
import mysql.connector
import pandas as pd
import datetime
import os

class mySqlDatabase:

    def __init__(self):
        self.config = {'user': os.environ['MYSQL_USERNAME'], 'password': os.environ['MYSQL_PASSWORD'],
                           'host': os.environ['MYSQL_HOST'], 'database': os.environ['MYSQL_DB_NAME']}

    def executeQuery(self, queryInput):
        prodConx = mysql.connector.connect(**self.config)
        prodCursor = prodConx.cursor()
        prodCursor.execute(queryInput)
        try:
            outdf = pd.DataFrame(prodCursor.fetchall(), columns=prodCursor.column_names)
        except Exception as e:
            if str(e) == 'No result set to fetch from.':
                # This is hacky - need to find more appropriate generalization to handle not results from sql query
                return pd.DataFrame(columns=['BROKERTXNREF', 'BROKERACCNTID'])
        prodCursor.close()
        prodConx.close()

        return outdf

class generatePositionQueries:

    def getLastInvestorAccountInformation(feedSource = None, advComKey=None, execPlatObjKey=None):
        query = "SELECT IA.KEY AS `ACCKEY`, IA.BROKERACCNTID, IA.NAME, IA.EXECUTIONPLATFORMOBJECT_KEY_OID, IA.STATUS, \
                IAF.VALUEDATE, DATEDIFF(IAF.VALUEDATE, NOW()) AS `DATEDIFF`, IAF.PORTFOLIOCURRENCY, IAF.PORTFOLIOVALUE, IAF.SOURCE, \
                U.COMPANY_ADVISORCOMPANY_KEY_EID AS `COMPANYKEY`, AC.NAME AS `COMPANYNAME` FROM PORTFOLIOMETRICS PM \
                JOIN INVESTORACCOUNTINFORMATION IAF ON IAF.KEY = PM.LASTINVESTORACCOUNTINFORMATION_KEY_OID \
                JOIN INVESTORACCOUNT IA ON IA.KEY = IAF.INVESTORACCOUNT_KEY_OID \
                JOIN USER U ON U.KEY = IA.OWNEDBYUSER_KEY_OID \
                JOIN EXECUTIONPLATFORMOBJECT EPO ON EPO.KEY = IA.EXECUTIONPLATFORMOBJECT_KEY_OID \
                JOIN ADVISORCOMPANY AC ON AC.KEY = U.COMPANY_ADVISORCOMPANY_KEY_EID \
                WHERE \
                PM.INPUTTIMESERIESTYPE = '1' AND PM.TYPE = '1' AND IA.DELETED <> '1' AND U.DELETED <> '1' \
                AND IA.STATUS NOT IN ('4','9') AND AC.ACTIVE = '1' AND AC.DEMO <> '1' AND IA.BROKERACCNTID NOT LIKE 'TBA%'"
        if execPlatObjKey:
            query += " AND IA.EXECUTIONPLATFORMOBJECT_KEY_OID = '" + str(execPlatObjKey) + "'"
        if advComKey:
            query += " AND U.COMPANY_ADVISORCOMPANY_KEY_EID = '" + str(advComKey) + "'"
        if feedSource:
            query += " AND IAF.SOURCE = '" + str(feedSource) + "'"

        query += "ORDER BY IAF.SOURCE, EPO.KEY, AC.KEY, IAF.VALUEDATE DESC"

        return query

    def getPortfolioMetrics(feedSource = None, advComKey=None, execPlatObjKey=None):
        query = "SELECT IA.KEY AS `ACCKEY`, IA.BROKERACCNTID, IA.NAME, EPO.KEY AS `EPOKEY`, IA.STATUS, \
                U.COMPANY_ADVISORCOMPANY_KEY_EID AS `COMPANYKEY`, AC.NAME AS `COMPANYNAME`, IAF.SOURCE, \
                MAX(PM.RECONERRORDATAFEED) AS `RECONERRORDATAFEED`, IAF.VALUEDATE FROM PORTFOLIOMETRICS PM \
                JOIN INVESTORACCOUNTINFORMATION IAF ON IAF.KEY = PM.LASTINVESTORACCOUNTINFORMATION_KEY_OID \
                JOIN INVESTORACCOUNT IA ON IA.KEY = PM.INVESTORACCOUNT_KEY_OID \
                JOIN USER U ON U.KEY = IA.OWNEDBYUSER_KEY_OID \
                JOIN ADVISORCOMPANY AC ON AC.KEY = U.COMPANY_ADVISORCOMPANY_KEY_EID \
                JOIN EXECUTIONPLATFORMOBJECT EPO ON EPO.KEY = IA.EXECUTIONPLATFORMOBJECT_KEY_OID \
                WHERE \
                IA.DELETED <> '1' AND U.DELETED <> '1' AND `RECONERRORDATAFEED` = 1 \
                AND IA.STATUS NOT IN ('4','9') AND AC.ACTIVE = '1' AND AC.DEMO <> '1' AND IA.BROKERACCNTID NOT LIKE 'TBA%'"
        if execPlatObjKey:
            query += " AND IA.EXECUTIONPLATFORMOBJECT_KEY_OID = '" + str(execPlatObjKey) + "'"
        if advComKey:
            query += " AND U.COMPANY_ADVISORCOMPANY_KEY_EID = '" + str(advComKey) + "'"
        if feedSource:
            query += " AND IAF.SOURCE = '" + str(feedSource) + "'"

        query += "GROUP BY IA.KEY ORDER BY IAF.SOURCE, EPO.KEY, AC.KEY, IAF.VALUEDATE DESC"

        return query

class generateTransactionQueries:

    def __init__(self, execStartDateTime, execEndDateTime):
        if isinstance(execStartDateTime, datetime.datetime) and isinstance(execEndDateTime, datetime.datetime):
            self.queryStartDateTime = '{:04d}-{:02d}-{:02d} 00:00:00.000'.format(execStartDateTime.year,
                                                                                 execStartDateTime.month,
                                                                                 execStartDateTime.day)
            self.queryEndDateTime = '{:04d}-{:02d}-{:02d} 00:00:00.000'.format(execEndDateTime.year,
                                                                               execEndDateTime.month,
                                                                               execEndDateTime.day)
        else:
            print('Input not datetime.datetime objects')
            raise

    def getOrderBroker(self, advComKey=None, execPlatObjKey=None):
        query = "SELECT OB.BROKERPERMID AS `BROKERTXNREF`, OB.EXECUTIONTIMESTAMP AS `TRADEDATE`, OB.SETTLEMENTDATE, IA.BROKERACCNTID,\
            EPO.NAME AS `BROKER`, CASE WHEN OB.SIDE = '1' THEN 'BUY' ELSE 'SELL' END AS `TRANSACTIONTYPE`,\
            TK.ISIN AS `PRODUCTID`, TK.NAME AS `PRODUCTNAME`, OB.QUANTITY, TK.CURRENCY,\
            OB.AVGFILLPRICE AS `PRICE` FROM ORDERBROKER OB\
            JOIN POSTTRADEALLOCATIONITEM PTI ON PTI.ORDERBROKER_KEY_OID = OB.KEY\
            JOIN TICKER TK ON TK.KEY = OB.ASSET_TICKER_KEY_EID\
            JOIN INVESTORACCOUNT IA ON IA.KEY = PTI.INVESTORACCOUNT_KEY_OID\
            JOIN USER U ON U.KEY = IA.OWNEDBYUSER_KEY_OID\
            JOIN EXECUTIONPLATFORMOBJECT EPO ON EPO.KEY = IA.EXECUTIONPLATFORMOBJECT_KEY_OID\
            WHERE\
            OB.EXECUTIONTIMESTAMP >= '" + self.queryStartDateTime + "' AND OB.EXECUTIONTIMESTAMP < '" + self.queryEndDateTime + "'"
        if execPlatObjKey:
            query += " AND IA.EXECUTIONPLATFORMOBJECT_KEY_OID = '" + str(execPlatObjKey) + "'"
        if advComKey:
            query += " AND U.COMPANY_ADVISORCOMPANY_KEY_EID = '" + str(advComKey) + "'"

        return query

    def getAssetFlow(self, advComKey=None, execPlatObjKey=None):
        query = "SELECT TAF.BROKERTXNREF, IA.BROKERACCNTID, TAF.EXECUTIONTIMESTAMP,\
            TAF.SETTLEMENTDATE, TAF.QUANTITY, TAF.VALUE, TAF.VALUEINBASECURRENCY FROM TRANSACTIONASSETFLOW TAF\
            JOIN INVESTORACCOUNT IA ON IA.KEY = TAF.INVESTORACCOUNT_KEY_OID\
            JOIN USER U ON U.KEY = IA.OWNEDBYUSER_KEY_OID\
            WHERE\
            TAF.EXECUTIONTIMESTAMP >= '" + self.queryStartDateTime + "' AND TAF.EXECUTIONTIMESTAMP < '" + self.queryEndDateTime + "'"
        if execPlatObjKey:
            query += " AND IA.EXECUTIONPLATFORMOBJECT_KEY_OID = '" + str(execPlatObjKey) + "'"
        if advComKey:
            query += " AND U.COMPANY_ADVISORCOMPANY_KEY_EID = '" + str(advComKey) + "'"

        return query

    def getInstrumentDistribution(self, advComKey=None, execPlatObjKey=None):
        query = "SELECT TID.BROKERTXNREF, TID.EXECUTIONTIMESTAMP, TID.SETTLEMENTDATE, IA.BROKERACCNTID FROM TRANSACTIONINSTRUMENTDISTRIBUTION TID\
            JOIN INVESTORACCOUNT IA ON IA.KEY = TID.INVESTORACCOUNT_KEY_OID\
            JOIN USER U ON U.KEY = IA.OWNEDBYUSER_KEY_OID\
            WHERE\
            TID.EXECUTIONTIMESTAMP >= '" + self.queryStartDateTime + "' AND TID.EXECUTIONTIMESTAMP < '" + self.queryEndDateTime + "'"
        if execPlatObjKey:
            query += " AND IA.EXECUTIONPLATFORMOBJECT_KEY_OID = '" + str(execPlatObjKey) + "'"
        if advComKey:
            query += " AND U.COMPANY_ADVISORCOMPANY_KEY_EID = '" + str(advComKey) + "'"

        return query

    def getTransactionOther(self, advComKey=None, execPlatObjKey=None):
        query = "SELECT OT.KEY, OT.BROKERTXNREF, IA.BROKERACCNTID, OT.EXECUTIONTIMESTAMP, OT.SETTLEMENTDATE FROM TRANSACTIONOTHER OT\
            JOIN INVESTORACCOUNT IA ON IA.KEY = OT.INVESTORACCOUNT_KEY_OID\
            JOIN USER U ON U.KEY = IA.OWNEDBYUSER_KEY_OID\
            WHERE\
            OT.EXECUTIONTIMESTAMP >= '" + self.queryStartDateTime + "' AND OT.EXECUTIONTIMESTAMP < '" + self.queryEndDateTime + "'"
        if execPlatObjKey:
            query += " AND IA.EXECUTIONPLATFORMOBJECT_KEY_OID = '" + str(execPlatObjKey) + "'"
        if advComKey:
            query += " AND U.COMPANY_ADVISORCOMPANY_KEY_EID = '" + str(advComKey) + "'"

        return query

    def getTransactionMoneyMarket(self, advComKey=None, execPlatObjKey=None):
        query = "SELECT TMM.KEY, TMM.BROKERTXNREF, IA.BROKERACCNTID, TMM.EVENTTYPE, TMM.PRINCIPAL, TMM.EXECUTIONTIMESTAMP\
            FROM TRANSACTIONMONEYMARKET TMM\
            JOIN INVESTORACCOUNT IA ON IA.KEY = TMM.INVESTORACCOUNT_KEY_OID\
            JOIN USER U ON U.KEY = IA.OWNEDBYUSER_KEY_OID\
            WHERE\
            TMM.EXECUTIONTIMESTAMP >= '" + self.queryStartDateTime + "' AND TMM.EXECUTIONTIMESTAMP < '" + self.queryEndDateTime + "'"
        if execPlatObjKey:
            query += " AND IA.EXECUTIONPLATFORMOBJECT_KEY_OID = '" + str(execPlatObjKey) + "'"
        if advComKey:
            query += " AND U.COMPANY_ADVISORCOMPANY_KEY_EID = '" + str(advComKey) + "'"

        return query

class generateStaticDataQueries:

    def getInvestorAccounts(advComKey, execPlatObjKey=None):
        query = "SELECT IA.BROKERACCNTID FROM INVESTORACCOUNT IA\
            JOIN USER U ON U.KEY = IA.OWNEDBYUSER_KEY_OID\
            WHERE\
            U.COMPANY_ADVISORCOMPANY_KEY_EID = '" + str(advComKey) + "'"

        if execPlatObjKey:
            query += " AND IA.EXECUTIONPLATFORMOBJECT_KEY_OID = '" + str(execPlatObjKey) + "'"

        return query

    def getActiveAdvisorCompanies():
        query = "SELECT AC.KEY, AC.NAME FROM ADVISORCOMPANY AC \
                WHERE AC.ACTIVE = '1' AND AC.DEMO <> '1';"

        return query

class generateErrorQueries:

    def getReportParsingErrors(greaterthandate):
        query = ""