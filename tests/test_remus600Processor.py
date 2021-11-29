"""
Unit test for remus600SubsetDataReader.py
"""
import os
import sys
import logging
import numpy as np

sys.path.append("..")
import unittest
import tempfile
import inspect
import common.constants as consts
import DataProcessor.AuvProcessor.remus600Processor as r600proc
import FileReader.AuvReader.remus600SubsetDataReader as r600reader
import FileReader.AuvReader.remus600SubsetData as r600data
import FileReader.AuvReader.remus600SubsetMsgData as r600msgdata

LOG_HEADER_FORMAT = ('%(levelname)s:%(module)s: [line %(lineno)d]'
                     '\n%(message)s')

class TestRemus600Processor(unittest.TestCase):

    def getDataFilePath(self, dataFileName):

        # Find the path of the current test module
        # not cwd, as test can be run from anywhere
        testsPath = os.path.abspath(os.path.dirname(inspect.stack()[0][1]))
        dataPath = os.path.join( testsPath, "auvdata", dataFileName)
        return dataPath

    def test_profile_calcs(self):

        # Read in a test subset data file

        reader = r600reader.remus600SubsetDataReader()
        tempPathObject = tempfile.TemporaryDirectory()
        tempPath = tempPathObject.name
        infilePath = self.getDataFilePath('20190930_121445_AUVsubset.txt')
        data = reader.read( infilePath, tempPath )
        self.assertIsNotNone( data, 'Failed to read data file')

        # use data from Neil Brown CTD to compute profiles

        ctdData = data.getDataForMessageId( 1107 )
        self.assertIsNotNone(ctdData, "Failed to get message data for Neil Brown CTD")

        # convert to ndarrays for processing
        rawTimes = np.asarray( ctdData['timestamp'] )
        rawMissionTimes = np.asarray( ctdData['missionTime'] )
        rawDepths = np.asarray( ctdData['depth'] )

        # keep track of start of trajectory
        startTime = rawTimes[0]

        # test time offsets

        proc = r600proc.remus600Processor()
        offsets = proc.computeTimeOffsets(
            rawTimes[0], rawTimes, rawMissionTimes )
        self.assertEqual( len(offsets), len(ctdData['timestamp']), "Bad count of offsets")
        self.assertTrue( offsets[0] < 1000, "First offset should be < 1000 msecs")
        for i in range(1, len(offsets)-1):
            self.assertTrue( offsets[i] > offsets[i-1], "Offsets should continually increase")

        # test clean depths

        cleanTimes, cleanDepths = proc.cleanDepthData(
            offsets, rawDepths )
        self.assertTrue( (cleanDepths >= 0.0).all(), "invalid negative depths")
        self.assertTrue( (cleanDepths <= consts.OCEAN_DEPTH_M).all(), "invalid high depths")

        # clear raw to save memory
        #rawTimes = []
        rawMissionTimes = []
        rawDepths = []

        # test smoothing depth data

        smoothedTimes, smoothedDepths = proc.smoothDepths(
            cleanTimes, cleanDepths, proc.SMOOTHING_WINDOW_SIZE )

        # no good way to test smoothing except to plot
        with open('/tmp/smoothedDepth.csv', 'w') as f:
            for t,d in zip( smoothedTimes, smoothedDepths):
                f.write( str(t) + ',' + str(d) + '\n')

        # save memory
        cleanTimes = []
        cleanDepths = []


        # test computation of inflection points

        #inflectionTimes = proc.computeInflectionTimes( filteredTimes, filteredDepths,
        #                                               offsets[0], offsets[-1])
        inflectionTimes = proc.computeInflectionTimes( smoothedTimes, smoothedDepths,
                                                       startTime,
                                                       proc.MIN_PROFILE_DEPTH_METERS,
                                                       proc.MIN_PROFILE_TIME_SECONDS,
                                                       proc.MAX_TIME_GAP_SECONDS )
        self.assertTrue( len(inflectionTimes >= 1), "Too few inflection points")
        for profile in inflectionTimes:
            self.assertTrue( profile[0] >= rawTimes[0], "Bad start inflection time")
            self.assertTrue( profile[0] <= rawTimes[-1], "Bad start inflection time - beyond range")
            self.assertTrue( profile[1] >= rawTimes[0], "Bad end inflection time")
            self.assertTrue( profile[1] <= rawTimes[-1], "Bad end inflection time - beyond range")

        # log profiles for plotting
        with open('/tmp/profileBounds.csv', 'w') as f:
            for profile in inflectionTimes:
                f.write( str(profile[0]) + ', ' + str(profile[1]) +'\n')

        # Select a profile and use its data to test var computations

        # Yes, I know we already have the full ctd dataset here. Testing
        # the slice function based on profile time bounds
        ctdProfileData = data.getDataSliceForMessageId( 1107,
                                                        inflectionTimes[0][0],
                                                        inflectionTimes[0][1] )
        self.assertIsNotNone(ctdProfileData, "Failed to get message data slice for Neil Brown CTD")

        densities = proc.calculateDensity( ctdProfileData['salinity'],
                                           ctdProfileData['temperature'],
                                           ctdProfileData['pressure'],
                                           ctdProfileData['latitude'],
                                           ctdProfileData['longitude'] )
        # log densities for plotting
        with open('/tmp/densities.csv', 'w') as f:
            for density in densities:
                f.write( str(density) + '\n')

        # calculate depth averaged current values from adcp data

        adcpProfileData = data.getDataSliceForMessageId( 1141,
                                                         inflectionTimes[0][0],
                                                         inflectionTimes[0][1] )

        uv_time, uv_lat, uv_lon, u, v = proc.calculateUVVars(
            adcpProfileData['timestamp'],
            adcpProfileData['latitude'],
            adcpProfileData['longitude'],
            adcpProfileData['averageCurrent'],
            adcpProfileData['averageDirection'] )
        self.assertTrue( uv_time >= inflectionTimes[0][0], 'Mean UV time below profile range')
        self.assertTrue( uv_time <= inflectionTimes[0][1], 'Mean UV time above profile range')
        with open('/tmp/uvData.csv', 'w') as f:
            f.write( str(uv_time) + ', ' + str(uv_lat) + ', ' + str(uv_lon) + ', ' + str(u) + ', ' + str(v) + '\n')

        tempPathObject.cleanup()

if __name__ == '__main__':

    # Set up logging
    logging.basicConfig(filename='./test_remus600Processor.log', level= 'INFO' )
    formatter = logging.Formatter( LOG_HEADER_FORMAT )
    logging.getLogger().handlers[0].setFormatter( formatter )
    logging.info('----- starting profileDataFormatter -----')

    unittest.main()
