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

        # maintain map from message id to remus600SubsetMsgData object
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

    def timesInMillisecs(self, timesSecs, dayOffsetsMillisecs):

        if dayOffsetsMillisecs is not None:
            return dayOffsetsMillisecs % 1000 / 1000. + timesSecs
        else:
            return timesSecs

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

    def getDataSliceForMessageId(self, msgId, startTime, endTime):
        """
        Retrieve time range bound slice of data for passed msgId
        :param msgId:
        :param startTime:
        :param endTime:
        :return: dataframe values within the time range passed
        """

        msgData = self.getDataForMessageId(msgId)

        # use timestamp, missiontime to get millisec timestamps
        # for slicing dataset between start and end times

        timesMs = self.timesInMillisecs(
            msgData.get('timestamp'), msgData.get('missionTime') )

        sliceRange = timesMs.between( startTime, endTime )

        # Note: if reading slices of data repeatedly proves too slow,
        # and memory proves not a concern, lose the copy and enable
        # cacheing in remus600SubsetMsgData.py
        # Note: enabling caching speedup 2x, memory 2x

        #dataSlice = msgData[ sliceRange ].copy()

        #return dataSlice
        return msgData[ sliceRange ]
