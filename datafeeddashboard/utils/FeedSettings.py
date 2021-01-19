# -*- coding: utf-8 -*-
"""
Created on 2020-04-11 15:28

@author: christopherwong
"""

class FeedSettings:

    def __init__(self):
        #{ FeedName on S3 : {Java Enum Constant, Feed Type, maxDepth Lookup on S3} }
        self.feeds = {'creditsuisse': {'Constant': 700, 'Type': 'EAM', 'maxDepth': 3},
                      'juliusbair': {'Constant': 801, 'Type': 'EAM', 'maxDepth': 3},
                      'efg': {'Constant': 802, 'Type': 'EAM', 'maxDepth': 3},
                      'lgt': {'Constant': 1500, 'Type': 'EAM', 'maxDepth': 3},
                      'ubs': {'Constant': 1300, 'Type': 'EAM', 'maxDepth': 3},
                      'dbs': {'Constant': 1100, 'Type': 'EAM', 'maxDepth': 3},
                      'phillipsecurities': {'Constant': 1600, 'Type': 'EAM', 'maxDepth': 3},
                      'baaderbank': {'Constant': 800, 'Type': 'EAM', 'maxDepth': 3},
                      'generalithailand': {'Constant': 1800, 'Type': 'ENT', 'comKey': '6471', 'maxDepth': 3},
                      #'ifast': {'Constant': 400, 'Type': 'IFA', 'comKey': '7021', 'maxDepth': 3},
                      'ageas': {'Constant': 14, 'Type': 'IFA', 'archiveKeyPath': False,
                                'comKeyKeyPath': True, 'maxDepth': 3},
                      'axa_pulsar': {'Constant': 500, 'Type': 'IFA', 'archiveKeyPath': False,
                                  'comKeyKeyPath': True, 'maxDepth': 3},
                      'friendsprovident': {'Constant': 4, 'Type': 'IFA', 'archiveKeyPath': False,
                                           'comKeyKeyPath': False, 'maxDepth': 3}, # Also constants 17?
                      'generali': {'Constant': 5, 'Type': 'IFA', 'archiveKeyPath': False,
                                   'comKeyKeyPath': False, 'maxDepth': 3}, # Also constants 19 and 54?
                      'hansard': {'Constant': 6, 'Type': 'IFA', 'archiveKeyPath': True,
                                  'comKeyKeyPath': False, 'maxDepth': 3},
                      'rl360v2': {'Constant': 5, 'Type': 'IFA', 'archiveKeyPath': False,
                                  'comKeyKeyPath': True, 'maxDepth': 3}, # Also constants 23?
                      'standardlife': {'Constant': 10, 'Type': 'IFA', 'archiveKeyPath': False,
                                       'comKeyKeyPath': True, 'maxDepth': 3},
                      'skandiaRegular': {'Constant': 22, 'Type': 'IFA', 'archiveKeyPath': False,
                                         'comKeyKeyPath': True, 'maxDepth': 3},
                      'skandiaBond': {'Constant': 8, 'Type':'IFA', 'archiveKeyPath': False,
                                      'comKeyKeyPath': True, 'maxDepth': 3},
                      'zurich': {'Constant': 12, 'Type': 'IFA', 'archiveKeyPath': False,
                                 'comKeyKeyPath': True, 'maxDepth': 3},
                      }

    def getSpecificFeedValues(self, feed):
        return self.feeds[feed]

    def getAllFeedsDict(self):
        return self.feeds

    def getAllFeedsNames(self):
        return [k for k,v in self.feeds.items()]

    def getEAMFeedNames(self):
        return [k for k,v in self.feeds.items() if v['Type'] == 'EAM']

    def getEnterpriseFeedNames(self):
        return [k for k, v in self.feeds.items() if v['Type'] == 'ENT']

    def getIFAFeedNames(self):
        return [k for k, v in self.feeds.items() if v['Type'] == 'IFA']
