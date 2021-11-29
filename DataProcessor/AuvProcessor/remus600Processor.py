"""
class: remusXYProcessor

*** TODO  Need to interpolate GPS data to 1 second cadence
          It is 1 sec while surfaced w/ big gaps between

description:

history:
09/21/2021 ppw created
"""
import logging
import numpy as np
from pandas import DataFrame, Series
from scipy import signal
from DataProcessor.AuvProcessor.auvProcessor import auvProcessor
from common.constants import OCEAN_DEPTH_M
from gsw import SP_from_C, SA_from_SP, CT_from_t, rho, z_from_p


class remus600Processor( auvProcessor ) :

    def __init__( self ) :
        super().__init__()

        # constants (move to config?)
        self.MIN_PROFILE_DEPTH_METERS = 10
        self.MIN_PROFILE_TIME_SECONDS = 120
        self.MAX_TIME_GAP_SECONDS = 30
        self.SMOOTHING_WINDOW_SIZE = 150

    def computeProfiles(self, trajectoryData, timeIndex, missionTimeIndex, depthIndex):
        """
        Given trajectory data for any instrument containing time, mission time
        and depth columns, computes a series of profile start and end times.

        :param trajectoryData: pandas dataframe containing time, mission time and depth
        :param timeIndex: index identifier of the dataframe time column
        :param missionTimeIndex: index identifier of the dataframe mission time column
        :param depthIndex: index identifier of the dataframe depth column
        :return: pandas dataframe containing 'startTime' and 'endTime' columns
        """

        # convert pandas dataframe series into ndarrays for processing

        times = np.asarray( trajectoryData[timeIndex] )
        missionTimes = np.asarray( trajectoryData[missionTimeIndex] )
        depths = np.asarray( trajectoryData[depthIndex] )

        # Combine time fields into millisecs since trajectory start
        # (Time in seconds, missionTime in millisecs into day)

        startTime = times[0]
        timeOffsets = self.computeTimeOffsets(
            startTime, times, missionTimes )

        # Create clean depths ( 0 <= depth <= ocean depth )

        times, depths = self.cleanDepthData(
            timeOffsets, depths )

        # Smooth depths

        smoothingSize = 10
        times, depths = self.smoothDepths(
            times, depths, smoothingSize)

        # compute inflection points (profile bounds)

        profileBounds = self.computeInflectionTimes( times,
                                                     depths,
                                                     startTime,
                                                     self.MIN_PROFILE_DEPTH_METERS,
                                                     self.MIN_PROFILE_TIME_SECONDS,
                                                     self.MAX_TIME_GAP_SECONDS)

        return profileBounds

    def computeTimeOffsets(self, startTime, timestamps, dayOffsets):
        """
        Combine timestamps, dayOffsets into a single offset from start of mission
        :param startTime: unixtime defining start of mission
        :param timestamps: unixtime (secs) for each sample
        :param dayOffsets: offset (msecs) into day
        :return: offsets (ms) from start of mission
        """

        timeOffsets = (1000 * (timestamps - startTime)) + \
            np.fmod( dayOffsets, 1000 )

        return timeOffsets


    def cleanDepthData(self, timeOffsets, rawDepths ):
        """
        Excise samples where depth < 0 or unreasonably large
        :param timeOffsets:
        :param rawDepths:
        :return: updated times, depths
        """

        # Create clean depths ( 0 <= depth <= ocean depth )
        cleanDepths = rawDepths
        cleanDepths = np.where( cleanDepths > OCEAN_DEPTH_M, np.nan, cleanDepths)
        cleanDepths = np.where( cleanDepths < 0.0, np.nan, cleanDepths)

        # Filter bad depths out of time and depth
        depthIndices = np.isfinite( cleanDepths )
        timeOffsets = timeOffsets[ depthIndices ]
        cleanDepths = cleanDepths[ depthIndices ]

        return timeOffsets, cleanDepths

    def smoothDepths(self, times, depths, windowSize):
        """
        Perform boxcar smoothing on depth data
        :param times:
        :param depths:
        :param windowSize:
        :return: times and depths with smoothing applied
        """

        window = signal.windows.boxcar(windowSize)
        smoothedDepths = signal.convolve(depths, window, 'same') / windowSize

        # remove the extra points with filter edge effects
        smoothedDepths = smoothedDepths[windowSize:-(windowSize+1)]
        timesOut = times[windowSize:-(windowSize+1)]

        return timesOut, smoothedDepths

    def computeInflectionTimes(self, times, depths, missionStartTime, minDeltaD,
                               minDeltaTSecs, maxTimeGapSecs):
        """
        Find profile bounds, eliminating periods of level flight, short profiles and time gaps
        :param times: offset in milliseconds from trajectory start
        :param depths: meters
        :param missionStartTime: unixtime UTC
        :param minDeltaD: min depth change for a valid profile (meters)
        :param minDeltaTSecs: min time duration for valid profile (seconds)
        :param maxTimeGapSecs: max time gap valid within a profile
        :return: array of valid profile [start, end] times in UnixTime UTC seconds
        """

        logging.info("computeInflectionTimes")
        logging.info( "Mission start time: " + str(missionStartTime))

        minDeltaTMillisecs = 1000 * minDeltaTSecs

        # compute rate of depth change and direction
        dZdT = np.diff( depths ) / np.diff( times )
        updownlevel = np.sign( dZdT )

        # init return array of [start, end] items
        profileBounds = []

        # traverse time/depth looking for direction changes and time gaps
        start = 0
        end = 1
        while end < len(updownlevel):

            #if (times[end] - times[end - 1] > 1000 * self.MAX_TIME_GAP_SECONDS):
            #    logging.info(" gap between " + str(end-1) + ' and ' + str(end))

            # if time gap too large or direction changes, end current profile here

            if (times[end] - times[end-1] > 1000 * self.MAX_TIME_GAP_SECONDS) or \
                    (updownlevel[end] != 0 and updownlevel[end] != updownlevel[end - 1]):
                if np.fabs(depths[end-1] - depths[start]) >= minDeltaD and \
                    times[end-1] - times[start] >= minDeltaTMillisecs:
                    profileBounds.append( [ times[start], times[end-1] ] )
                    #logging.info("profile between " + str(start) + ' and ' + str(end-1))

                start = end
            end = end + 1

        # if incomplete profile at end of data, complete it

        if start < len(depths)-1:
            if np.fabs(depths[end - 1] - depths[start]) >= minDeltaD and \
                    times[end - 1] - times[start] >= minDeltaTMillisecs:
                profileBounds.append( [ times[start], times[end-1] ] )
                #logging.info("profile between " + str(start) + ' and ' + str(end - 1))

        # Convert profile bounds back to unixtime UTC

        profileBoundsRA = np.asarray(profileBounds,dtype=int)
        return self.offsetTimesToEpochSecs( missionStartTime, profileBoundsRA )


    def offsetTimesToEpochSecs(self, missionStartSecs, profileBoundsRA):
        """
        convert millisec offset from mission start to unix time utc
        :param missionStartSecs:
        :param profileBoundsRA:
        :return:
        """
        return profileBoundsRA / 1000 + missionStartSecs

    def interpolateGpsData(self, gpsData ):
        """
        gps data at 1 sec resolution while surfaced, blank when not;
        fill to 1 sec resolution through interpolation
        :param gpsData:
        :return: filledGpsData
        """

        #  create new 1 second res timestamp array (no gaps)

        allTimestamps = np.arange( int(gpsData['timestamp'][0]),
                                   int(gpsData['timestamp'].iloc[-1] + 1))

        # Interpolate lat, lon to 1 second resolution in gaps

        allLatitudes = np.interp( allTimestamps, gpsData['timestamp'], gpsData['latitude'] )
        allLongitudes = np.interp( allTimestamps, gpsData['timestamp'], gpsData['longitude'] )

        filledGpsData = { 'timestamp': allTimestamps,
                          'latitude': allLatitudes,
                          'longitude': allLongitudes }

        return DataFrame( filledGpsData )


    def processOxygenData(self, rawO2Concentration, salinity):
        """
        TBD: OPTA oxygen data may need processing
        :param o2data:
        :return:
        """

        # TBD determine processing needed
        return rawO2Concentration

    def processPARData(self, temperature, depth, supplyVoltage, sensorVoltage):
        """
        TBD PAR data may need calibrations and calculations
        :param parData:
        :return:
        """

        #TBD determine processing needed
        return np.zeros(len(sensorVoltage))

    def calculateDensity(self, salinity, temperature, pressure, latitude, longitude):
        """Calculates density given practical salinity, temperature, pressure, latitude,
        and longitude using Gibbs gsw SA_from_SP and rho functions.

        Parameters:
            temperature (C), pressure (dbar), salinity (psu PSS-78),
            latitude (decimal degrees), longitude (decimal degrees)

        Returns:
            density (kg/m**3),
        """

        # dBar_pressure = pressure * 10

        absolute_salinity = SA_from_SP(
            salinity,
            pressure,
            longitude,
            latitude
        )

        conservative_temperature = CT_from_t(
            absolute_salinity,
            temperature,
            pressure
        )

        density = rho(
            absolute_salinity,
            conservative_temperature,
            pressure
        )

        return density

    def calculateCurrentComponents(self, currentSpeeds, currentDirections):

        currentEast = currentSpeeds * np.sin( np.deg2rad( currentDirections ) )
        currentNorth = currentSpeeds * np.cos( np.deg2rad( currentDirections ) )

        return currentEast, currentNorth

    def calculateUVVars(self, times, latitudes, longitudes, currents, directions):
        """
        Computes depth averaged u, v components w/ mean time, lat and lon
        from ADCP data
        :param times:
        :param latitudes:
        :param longitudes:
        :param currents:
        :param directions:
        :return: t, lat, lon, u, v
        """
        # compute mean east and north components of current
        U, V = self.calculateCurrentComponents( currents, directions )
        u = U.mean()
        v = V.mean()

        # compute mean profile time, lat, lon at that time
        uvTime, uvLat, uvLon = self.findMidpointTimeLatLon(
            times, latitudes, longitudes )

        return uvTime, uvLat, uvLon, u, v

    def findMidpointTimeLatLon(self, times, latitudes, longitudes):
        """
        Finds mean time, closest latitude and longitude
        :param times:
        :param latitudes:
        :param longitudes:
        :return: time, lat, lon
        """

        midpointTime = times.mean()
        timeIndex = times.sub( midpointTime ).abs().idxmin()
        midpointLat = latitudes[ timeIndex ]
        midpointLon = longitudes[ timeIndex ]

        return midpointTime, midpointLat, midpointLon
