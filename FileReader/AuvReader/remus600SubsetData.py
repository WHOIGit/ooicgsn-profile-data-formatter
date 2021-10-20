"""
class: remusXYSubsetData

description: Encapsulates REMUS 600 Subset Data.
Remus subset data consists of time series data grouped by
message type (corresponding to instrument types, subsystems
and system status)

history:
09/21/2021 ppw created
"""
import os
import glob
import logging
from FileReader.AuvReader.remus600SubsetMsgData import remus600SubsetMsgData

class remus600SubsetData(  ) :

    def __init__( self ) :

        # Use temp output path to split AUV msg data
        self._tempPath = '/tmp'

        # maintain map from message id to remus600SubsetMsg object
        self._msgData = {}

    @property
    def tempPath( self ) :
        return self._tempPath

    @tempPath.setter
    def tempPath(self, newpath):
        self._tempPath = newpath

    @property
    def msgData( self ) :
        return self._msgData

    def getDataForMessageId(self, msgId):
        """
        Retrieve remus600 Subset data for msgId in the form of
        a pandas DataFrame
        :param msgId:
        :return: pandas DataFrame if found, else None
        """
        msgData = None
        if msgId in self.msgData:
            msgData = self.msgData[msgId].getData()
        return msgData
