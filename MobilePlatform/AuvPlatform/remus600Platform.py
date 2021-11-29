"""
class: remus600Platform

*** TODO  Need to interpolate GPS data to 1 second cadence
          It is 1 sec while surfaced w/ big gaps between

description:

history:
09/21/2021 ppw created
"""
from MobilePlatform.AuvPlatform.auvPlatform import auvPlatform
from FileReader.jsonCfgReader import jsonCfgReader
from FileReader.AuvReader.remus600SubsetDataReader import remus600SubsetDataReader
from DataProcessor.AuvProcessor.remus600Processor import remus600Processor
from FileWriter.NetCDFWriter.dacNetCDFWriter import dacNetCDFWriter
import tempfile
import FileReader.AuvReader.remus600SubsetData as r600data
import FileReader.AuvReader.remus600SubsetMsgData as r600msgdata
import os
import logging
import json

class remus600Platform( auvPlatform ) :

    def __init__( self ) :
        super().__init__()

        self.cfgReader = jsonCfgReader()
        self.dataFileReader = remus600SubsetDataReader()
        self.dataProcessor = remus600Processor()
        self.outputFileWriter = dacNetCDFWriter()

        # TBD { extract platform specific args here }

        # Any overrides of default file readers/writers/processors goes here
        # (example below, not yet implemented)
        # if self.targetHost == 'OOI-EXPLORER':
        #    self.outputFileWriter = FileWriter.NetCDFWriter.dataExplorerNetCDFWriter


    def isValidFile(thePath, theFile ):

        ret = True
        if not os.path.isfile( os.path.join( thePath, theFile) ):
            logging.error(
                'Configuration file {:s}not found at {:s}'.format(
                    theFile, thePath ) )
            ret = False
        return ret


    def validateSettings( self ):

        # validate existence of config files
        # REMUS 600 requires deployment, global_attributes,
        # instruments and sensor_defs as json format config files

        ret = remus600Platform.isValidFile( self.cfgPath, 'deployment.json' )
        ret = ret and remus600Platform.isValidFile( self.cfgPath, 'global_attributes.json' )
        ret = ret and remus600Platform.isValidFile(self.cfgPath, 'instruments.json')
        ret = ret and remus600Platform.isValidFile(self.cfgPath, 'sensor_defs.json')

        # caller expects 0 for valid, -1 invalid
        if ret:
            return 0
        else:
            return -1


    def getInstrumentFromCfg(instrumentsCfg, instrumentName):

        for instr in instrumentsCfg:
            if instr['nc_var_name'] == instrumentName:
                return instr

        return None

    def useCtdDataToComputeProfiles(self, data):
        """
        Wrap profile computation to force hi-res ctd data
        to go out of scope when done
        :param data: remus subset file data
        :return: list of profile start, end times
        """
        ctdCfg = remus600Platform.getInstrumentFromCfg(
            self.instrumentsCfg, 'instrument_ctd')
        ctdData = data.getDataForMessageId(
            int(ctdCfg['attrs']['subset_msg_id']))

        allProfileBounds = self.dataProcessor.computeProfiles(
            ctdData, "timestamp", "missionTime", "depth")

        return allProfileBounds

    def sensorAttrMatches(sensorDef, attrName, match):

        if 'attrs' in sensorDef:
            if attrName in sensorDef['attrs']:
                if sensorDef['attrs'][attrName] == match:
                    return True
        return False


    def isInstrument(instrCfg, name ):

        return ('nc_var_name' in instrCfg and instrCfg['nc_var_name'] == name)


    def isInstrumentSensor( instrCfg, sensorDef ):

        if 'nc_var_name' in instrCfg:
            return remus600Platform.sensorAttrMatches(
                sensorDef, 'instrument', instrCfg['nc_var_name'] )
        return False

    def getDataSlice( dataset, startTime, endTime ):

        # return data within passed time window

        sliceRange = dataset['timestamp'].between( startTime, endTime )

        return dataset[ sliceRange ]

    def setupFormatting(self):

        # read configuration settings

        self.globalsCfg = self.readCfgFile( self.cfgPath, 'global_attributes.json')
        self.deploymentCfg = self.readCfgFile( self.cfgPath, 'deployment.json')
        self.instrumentsCfg = self.readCfgFile( self.cfgPath, 'instruments.json')
        self.sensorsCfg = self.readCfgFile( self.cfgPath, 'sensor_defs.json')


    def FormatData(self ):

        # If debug mode, go no further
        if self.suppressOutput:
            return 0

        # for each data file

        for dataFile in self.dataFiles:

            # Establish temp directory for splitting Remus
            # subset files into individual messages (instruments)

            with tempfile.TemporaryDirectory() as tempPath:

                # read in the subset data file

                data = self.dataFileReader.read( dataFile, tempPath )

                # interpolate gps data to 1 second cadence
                # Is 1 sec at surface, gaps during dives

                gpsCfg = remus600Platform.getInstrumentFromCfg(
                    self.instrumentsCfg, 'instrument_gps' )
                gpsData = data.getDataForMessageId( int(gpsCfg['attrs']['subset_msg_id'] ))
                allGpsData = self.dataProcessor.interpolateGpsData( gpsData )

                # compute profile bounds using data from CTD

                allProfileBounds = self.useCtdDataToComputeProfiles( data )

                # for each profile (id unique w/i trajectory 1..n)

                profileId = 1
                for profileBounds in allProfileBounds:

                    gpsProfileData = None
                    currentProfileData = None

                    for instrCfg in self.instrumentsCfg:

                        # get instrument data within the profile time bounds

                        if not remus600Platform.isInstrument( instrCfg, 'instrument_gps'):

                            profileData = data.getDataSliceForMessageId(
                                int( instrCfg['attrs']['subset_msg_id'] ),
                                profileBounds[0], profileBounds[1] )

                        else:
                            profileData = remus600Platform.getDataSlice(
                                allGpsData, profileBounds[0], profileBounds[1] )

                        # find sensor defs (output variables) for this instrument
                        # for each temporal sensor def, create output variable

                        for sensorDef in self.sensorsCfg:

                            # Only process time variant data here
                            if self.sensorsCfg[sensorDef]['dimension'] != 'time':
                                print("Non-temporal sensor " +
                                      self.sensorsCfg[sensorDef]['nc_var_name'] +
                                      " ignore for now" )
                                continue

                            if remus600Platform.isInstrumentSensor( instrCfg, self.sensorsCfg[sensorDef] ):

                                # measurement sensor data is copied directly

                                if remus600Platform.sensorAttrMatches(
                                        self.sensorsCfg[sensorDef], 'observation_type', 'measured'):

                                    # TODO create output variable
                                    print( "Output measurement sensor " +
                                           self.sensorsCfg[sensorDef]['nc_var_name'] +
                                           " for instrument " +
                                           instrCfg['nc_var_name'] )

                                # calculated sensor variables require processing

                                else:

                                    print( "Output calculated sensor " +
                                           self.sensorsCfg[sensorDef]['nc_var_name'] +
                                           " for instrument " +
                                           instrCfg['nc_var_name'] )

                                    if self.sensorsCfg[sensorDef]['nc_var_name'] == 'density':
                                        density = self.dataProcessor.calculateDensity(
                                            profileData['salinity'],
                                            profileData['temperature'],
                                            profileData['pressure'],
                                            profileData['latitude'],
                                            profileData['longitude'] )

                                    elif self.sensorsCfg[sensorDef]['nc_var_name'] == 'PAR':
                                        par = self.dataProcessor.processPARData(
                                            profileData['temperature'],
                                            profileData['depth'],
                                            profileData['supplyVoltage'],
                                            profileData['sensorVoltage'] )

                                    elif self.sensorsCfg[sensorDef]['nc_var_name'] == 'dissolved_oxygen':
                                        o2 = self.dataProcessor.processOxygenData(
                                            profileData['calculatedConcentration'],
                                            profileData['salinity'] )

                                    # handles both east, north components
                                    elif self.sensorsCfg[sensorDef]['nc_var_name'] == 'current_eastward':
                                        u, v = self.dataProcessor.calculateCurrentComponents(
                                            profileData['averageCurrent'],
                                            profileData['averageDirection'] )

                                    # TODO create output variable
                                    pass

                        # if gps, cache data for profile avg'd vars

                        if remus600Platform.isInstrument( instrCfg, 'instrument_gps'):
                            gpsProfileData = profileData

                        # if current meter, stash data for depth-avgd current vars

                        if remus600Platform.isInstrument( instrCfg, 'instrument_adcp'):
                            currentProfileData = profileData

                    # process non-temporal data (profile and current avgs)

                    profileTime, profileLat, profileLon = \
                        self.dataProcessor.findMidpointTimeLatLon(
                            data.timesInMillisecs(
                                gpsProfileData.get('timestamp'),
                                gpsProfileData.get('missionTime')),
                            gpsProfileData.get('latitude'),
                            gpsProfileData.get('longitude'))

                    uv_time, uv_lat, uv_lon, u, v = \
                        self.dataProcessor.calculateUVVars(
                            data.timesInMillisecs(
                                currentProfileData.get('timestamp'),
                                currentProfileData.get('missionTime')),
                            currentProfileData.get('latitude'),
                            currentProfileData.get('longitude'),
                            currentProfileData.get('averageCurrent'),
                            currentProfileData.get('averageDirection') )

                    # process metadata

                    # add profile data to output variables

                    # add config data to output attributes

                    # write the profile output file

        return 0

    # abstract, implement in subclass
    def cleanupFormatting(self):

        #TODO
        return 0
