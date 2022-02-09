"""
class: remusXYSubsetMsgData

description: Encapsulates REMUS 600 Subset Data for a message id
Remus subset data consists of time series data grouped by
message type (corresponding to instrument types, subsystems
and system status)

Message and field definitions from:
2017-04-01 CDRLA004 Data Subsetting Manual 20171027.pdf

Note: For message type 1055 (GPS NMEA-183), headers in this document
did not match test data files (missing 12x5 satellite related columns)
Column definitions from test data files used.

history:
09/21/2021 ppw created
"""
import logging

import pandas

class remus600SubsetMsgData(  ) :

    def __init__( self ) :

        # Remus message id uniquely defines msg type, content
        self._msgId = None

        # Use temp output path to split AUV msg data
        self._dataFile = None

        # Read the data file once, then cache in memory

        self._cachedData = None

        # Map from msgId to column names, and
        # override the column names returned by read_csv()
        # [returned column names too cumbersome for dataframe subscripts]

        self.msgTypeFields = {
            1000 : # AUV state data
                   'messageId,timestamp,softwareVersion,temperature,'
                   'headingRate,pressure,depth,depthGoal,obs,voltage,'
                   'current,gfi,pitch,pitchGoal,roll,thruster,thrusterGoal,'
                   'compassTrueHeading,headingGoal,missionTime,daysSince1970,'
                   'latitude,longitude,drLatitude,drLongitude,latitudeGoal,'
                   'longitudeGoal,estimatedVelocity,headingOffset,flags,'
                   'thrusterCommand,pitchCommand,rudderCommand,pitchFinPosition,'
                   'rudderFinPosition,totalObjectives,currentObjective,cpuUsage,'
                   'objectiveIndex,legNumber,spareSlider,rollRate,pitchRate,'
                   'faults,navigationMode,secondaryFaults',
            1001 : # Fault
                   'messageId,fileName,line,timestamp,message',
            1050 : # Digital Tx Board data
                   'messageId,timestamp,transponderTableIndex1,transponderTableIndex2,'
                   'inbandChannel1Snr,inbandChannel2Snr,interrogateChannel1Snr,'
                   'interrogateChannel2Snr,receiverChannel1,receiverChannel2,'
                   'range1,range2,replyAge1,replyAge2,latitude,longitude,'
                   'compassTrueHeading,soundSpeed,failFlag,receivedBits,'
                   'outbandChannel1Snr,outbandChannel2Snr',
            1055 : # GPS NMEA-183
                   'messageId,timestamp,latitude,longitude,heading,fixAge,flags,gpsFixStatus,'
                   'satellitesInView,satellitesUsed,speedOverGround,courseOverGround,'
                   'positionError,horizontalError,verticalError,year,month,day,hour,'
                   'minute,second,fixStatus,satelliteData1SpaceVehicleId,satelliteData1Elevation,'
                   'satelliteData1Azimuth,satelliteData1SignalNoiseRatio,satelliteData1Flags,'
                   'satelliteData2SpaceVehicleId,satelliteData2Elevation,satelliteData2Azimuth,'
                   'satelliteData2SignalNoiseRatio,satelliteData2Flags,'
                   'satelliteData3SpaceVehicleId,satelliteData3Elevation,satelliteData3Azimuth,'
                   'satelliteData3SignalNoiseRatio,satelliteData3Flags,'
                   'satelliteData4SpaceVehicleId,satelliteData4Elevation,satelliteData4Azimuth,'
                   'satelliteData4SignalNoiseRatio,satelliteData4Flags,'
                   'satelliteData5SpaceVehicleId,satelliteData5Elevation,satelliteData5Azimuth,'
                   'satelliteData5SignalNoiseRatio,satelliteData5Flags,'
                   'satelliteData6SpaceVehicleId,satelliteData6Elevation,satelliteData6Azimuth,'
                   'satelliteData6SignalNoiseRatio,satelliteData6Flags,'
                   'satelliteData7SpaceVehicleId,satelliteData7Elevation,satelliteData7Azimuth,'
                   'satelliteData7SignalNoiseRatio,satelliteData7Flags,'
                   'satelliteData8SpaceVehicleId,satelliteData8Elevation,satelliteData8Azimuth,'
                   'satelliteData8SignalNoiseRatio,satelliteData8Flags,'
                   'satelliteData9SpaceVehicleId,satelliteData9Elevation,satelliteData9Azimuth,'
                   'satelliteData9SignalNoiseRatio,satelliteData9Flags,'
                   'satelliteData10SpaceVehicleId,satelliteData10Elevation,satelliteData10Azimuth,'
                   'satelliteData10SignalNoiseRatio,satelliteData10Flags,'
                   'satelliteData11SpaceVehicleId,satelliteData11Elevation,satelliteData11Azimuth,'
                   'satelliteData11SignalNoiseRatio,satelliteData11Flags,'
                   'satelliteData12SpaceVehicleId,satelliteData12Elevation,satelliteData12Azimuth,'
                   'satelliteData12SignalNoiseRatio,satelliteData12Flags'
            ,
            1071 : # Smart battery data
                   'messageId,flags,batteryIndex,batteryCount,address,voltage,'
                   'endOfDischarge,averageCurrent,temperatureTenth,fullChargeCapacity,'
                   'remainingCapacity,desiredChargeRate,serial,batteryStatus,'
                   'batteryFlags,cycleCount,timestamp,availablePower,temperature,'
                   'pressure,picChargeValue,picBalanceEnabled,picFetState,picFaults,'
                   'picCellVoltage1,picCellVoltage2,picCellVoltage3,picCellVoltage4,'
                   'picCellVoltage5,picCellVoltage6,picCellVoltage7,batteryTemperature',
            1075 : # Oil compensator data
                   'messageId,timestamp,latitude,longitude,depth,rawValue,oilLevel,'
                   'missionTime,oilLocation',
            1076 : # Emergency board data
                   'messageId,timestamp,voltage,status,descentStatus,ascentStatus,'
                   'pickupStatus,descentContinuity,ascentContinuity,pickupContinuity,'
                   'secondaryStatus',
            1087 : # "undocumented, but found in multiple test data files"
                   'messageId,timestamp,source,wrongStartCharacter,messageTooLong,'
                   'messageTooLong2,payloadTooLong,payloadTooShort,checksumError,'
                   'goodMessages,attempts,connectionState,timeUntilHangup,'
                   'betweenCalls',
            1089 : # Tri-fin motor data
                   'messageId,timestamp,finCount,pitchPosition,rudderPosition,'
                   'rollPosition,pitchCommand,rudderCommand,rollCommand,commandData1,'
                   'positionData1,commandData2,positionData2,commandData3,positionData3,'
                   'commandData4,positionData4,commandData5,positionData5,commandData6,'
                   'positionData6,yawTranslationCommand,depthTranslationCommand,'
                   'yawTranslationPosition,depthTranslationPosition',
            1098 : # Digital USBL data
                   'messageId,timestamp,latitude,longitude,compassTrueHeading,'
                   'soundSpeed,reserved,rangeMinimum,rangeMaximum,latency,'
                   'xAngle,yAngle,range,gain1,gain2,arraySoundSpeed,reason,'
                   'xCenter,yCenter,inbandSnr,outbandSnr,transponderTableIndex,'
                   'missionTime',
            1102 : # Imaginex 852 data
                   'messageId,timestamp,latitude,longitude,missionTime,depth,'
                   'rangeSetting,pitch,lookDownAngle,altitude,sonarRange,'
                   'roll,sonarRangeMinimum,estimatedRange,estimatedRate,'
                   'obstacleRangeRateMinimum,obstacleRangeMaximum,'
                   'obstacleRangeCritical,obstacleRangeRateCritical,'
                   'pings,status,medianFilterRange',
            1107 : # Neil Brown CTD data
                   'messageId,timestamp,latitude,longitude,missionTime,'
                   'depth,conductivity,temperature,salinity,soundSpeed,'
                   'pressure',
            1109 : # Optode oxygen data
                   'messageId,timestamp,latitude,longitude,missionTime,'
                   'depth,salinity,model,serial,concentration,saturation,'
                   'temperature,calibratedPhase,bPhase,rPhase,bAmp,bPot,'
                   'rAmp,rawTemperature,calculatedConcentration,'
                   'calculatedSaturation,externalTemperature',
            1117 : # WetLabs ECO Config Multi
                   'messageId,timestamp,deviceId,deviceCount,parameterName,'
                   'parameterUnit,parameterId,ecoDataOffset,htmlPlot,'
                   'type,mx,b,sensorName',
            1118 : # WetLabs ECO Config Multi
                   'messageId,timestamp,latitude,longitude,missionTime,'
                   'deviceId,deviceCount,depth,version,parameter0,parameter1,'
                   'parameter2,parameter3,parameter4,parameter5,parameter6,'
                   'parameter7,parameter8,parameter9',
            1141 : # ADCP data
                   'messageId,timestamp,latitude,longitude,missionTime,'
                   'depth,heading,pitch,roll,altitude,altitudeTrackRangeBeam1,'
                   'altitudeTrackRangeBeam2,altitudeTrackRangeBeam3,'
                   'altitudeTrackRangeBeam4,forwardVelocity,starboardVelocity,'
                   'verticalVelocity,'
                   'errorVelocity,temperature,ensemble,binaryVelocityData1,'
                   'binaryVelocityData2,binaryVelocityData3,binaryVelocityData4,'
                   'coordinatesTransformation,averageCurrent,averageDirection',
            1173 : # Biospherical 2150 data
                   'messageId,timestamp,latitude,longitude,missionTime,depth,'
                   'sensorVoltage,temperature,supplyVoltage',
            1174 : # SAtlantic SUNA data
                   'messageId,timestamp,latitude,longitude,missionTime,'
                   'depth,sampleTime,nitrateConcentration,nitrogenInNitrate,'
                   'spectralAverageOfLastDarkFrame,spectrometerTemperature,'
                   'lampState,lampTemperature,cumulativeLampOnTime,'
                   'relativeHumidity,inputVoltage,lampVoltage,internalVoltage,'
                   'mainCurrent',
            1181 : # Seabird GPCTD data
                   'messageId,timestamp,latitude,longitude,missionTime,'
                   'depth,conductivity,temperature,salinity,soundSpeed,'
                   'dissolvedOxygen,powered'
              }

    @property
    def msgId( self ) :
        return self._msgId

    @msgId.setter
    def msgId(self, newid):
        if newid in self.msgTypeFields.keys():
            self._msgId = newid
        else:
            logging.error("Invalid Remus600 message id passed: " + str(newid))

    @property
    def dataFile( self ) :
        return self._dataFile

    @dataFile.setter
    def dataFile(self, datafile):
        self._dataFile = datafile

    @property
    def cachedData( self ) :
        return self._cachedData

    @cachedData.setter
    def cachedData(self, dataframe):
        self._cachedData = dataframe

    def getData(self):
        """
        Retrieve message specific data as a pandas dataframe.
        Override column names from file with those defined here,
        as
        :return: DataFrame
        """
        df = None

        if self.cachedData is None:
            if self.dataFile is not None:
                df = pandas.read_csv(
                    self.dataFile, header=0,
                    names= self.msgTypeFields[self.msgId].split(','),
                    low_memory = False)
                #memory issues - do not cache for now
                # Note: enabling caching speedup 2x, memory 2x
                self.cachedData = df
        else:
            df = self.cachedData

        return df