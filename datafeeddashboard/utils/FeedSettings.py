# -*- coding: utf-8 -*-
"""
Created on 2020-04-11 15:28

@author: christopherwong
"""

class FeedSettings:

    def __init__(self):
        #{ FeedName : {Java Constant, Feed Type, maxDepth Lookup on S3} }
        self.feeds = {'creditsuisse': {'Constant': 700, 'Type': 'EAM', 'maxDepth': 3},
                      'juliusbair': {'Constant': 801, 'Type': 'EAM', 'maxDepth': 3},
                      'efg': {'Constant': 802, 'Type': 'EAM', 'maxDepth': 3},
                      'lgt': {'Constant': 1500, 'Type': 'EAM', 'maxDepth': 3},
                      'ubs': {'Constant': 1300, 'Type': 'EAM', 'maxDepth': 3},
                      'dbs': {'Constant': 1100, 'Type': 'EAM', 'maxDepth': 3},
                      'phillipsecurities': {'Constant': 1600, 'Type': 'EAM', 'maxDepth': 3},
                      'generalithailand': {'Constant': 1800, 'Type': 'ENT', 'comKey': '6471', 'maxDepth': 3},
                      'ifast': {'Constant': 400, 'Type': 'EAM', 'comKey': '7021', 'maxDepth': 3},
                      'baaderbank': {'Constant': 800, 'Type': 'EAM', 'maxDepth': 3}
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
