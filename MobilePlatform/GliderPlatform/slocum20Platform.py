"""
class: slocum20Platform

description: Implementation of the Slocum 2.0 Glider specific driver
for formatting IOOS-DAC compatible NetCDF files from Slocum 2.0
Glider data files

history:
09/21/2021 ppw created
"""
import os
import logging
import json
from copy import deepcopy

from MobilePlatform.GliderPlatform.gliderPlatform import gliderPlatform
from FileReader.jsonCfgReader import jsonCfgReader
from FileReader.GliderReader.slocum20DataReader import slocum20DataReader
from DataProcessor.GliderProcessor.slocum20Processor import slocum20Processor
from FileWriter.NetCDFWriter.dacNetCDFWriter import dacNetCDFWriter
from FileWriter.NetCDFWriter.dataExplorerNetCDFWriter import dataExplorerNetCDFWriter
from legacy.gliderdac.ooidac.constants import LLAT_SENSORS
from legacy.gliderdac.dba_file_sorter import sort_function
from legacy.gliderdac.ooidac.validate import validate_sensors, validate_ngdac_var_names
from legacy.gliderdac.ooidac.data_classes import DbaData
from legacy.gliderdac.ooidac.data_checks import check_file_goodness
from common.constants import LOG_HEADER_FORMAT, LOG_PROCESSING_FORMAT

class slocum20Platform( gliderPlatform ) :
    """
    Slocum 2.0 Glider specific driver for converting a profiling platform's
    output data into IOOS-DAC compatible NetCDF files
    """

    def __init__( self ) :
        super().__init__()

        # define local instance vars
        self._deploymentCfgPath = None
        self._globalAttributesPath = None
        self._instrumentsCfgPath = None
        self._sensorDefsCfgPath = None
        self._ctdSensors = None
        self._cfgSensorDefs = None
        self._deploymentDefs = None
        self._globalAttribs = None
        self._instrumentCfgs = None
        self._status = None
        self._ctdSensorPrefix = 'sci'
        self._startProfileId = 0

        # create Slocum 2.0 specific worker objects

        self.cfgReader = jsonCfgReader()
        self.dataFileReader = slocum20DataReader()
        self.dataProcessor = slocum20Processor()
        self.outputFileWriter = dacNetCDFWriter()

        # Any overrides of default file readers/writers/processors goes here
        # (example below, not yet implemented)
        # if self.targetHost == 'OOI-EXPLORER':
        #    self.outputFileWriter = FileWriter.NetCDFWriter.dataExplorerNetCDFWriter

    @property
    def deploymentCfgPath(self):
        return self._deploymentCfgPath
    
    @deploymentCfgPath.setter
    def deploymentCfgPath(self, newpath):
        self._deploymentCfgPath = newpath

    @property
    def globalAttributesPath(self):
        return self._globalAttributesPath

    @globalAttributesPath.setter
    def globalAttributesPath(self, newpath):
        self._globalAttributesPath = newpath

    @property
    def instrumentsCfgPath(self):
        return self._instrumentsCfgPath

    @instrumentsCfgPath.setter
    def instrumentsCfgPath(self, newpath):
        self._instrumentsCfgPath = newpath

    @property
    def sensorDefsCfgPath(self):
        return self._sensorDefsCfgPath

    @sensorDefsCfgPath.setter
    def sensorDefsCfgPath(self, newpath):
        self._sensorDefsCfgPath = newpath

    @property
    def ctdSensors(self):
        return self._ctdSensors

    @ctdSensors.setter
    def ctdSensors(self, sensors):
        self._ctdSensors = sensors

    @property
    def cfgSensorDefs(self):
        return self._cfgSensorDefs

    @cfgSensorDefs.setter
    def cfgSensorDefs(self, sensorDefs):
        self._cfgSensorDefs = sensorDefs

    @property
    def deploymentDefs(self):
        return self._deploymentDefs

    @deploymentDefs.setter
    def deploymentDefs(self, defs):
        self._deploymentDefs = defs

    @property
    def globalAttribs(self):
        return self._globalAttribs

    @globalAttribs.setter
    def globalAttribs(self, attribs):
        self._globalAttribs = attribs

    @property
    def instrumentCfgs(self):
        return self._instrumentCfgs

    @instrumentCfgs.setter
    def instrumentCfgs(self, cfgs):
        self._instrumentCfgs = cfgs

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, newstatus):
        self._status = newstatus

    @property
    def cdtSensorPrefix(self):
        return self._cdtSensorPrefix

    @cdtSensorPrefix.setter
    def cdtSensorPrefix(self, prefix):
        self._cdtSensorPrefix = prefix

    @property
    def startProfileId(self):
        return self._startProfileId

    @startProfileId.setter
    def startProfileId(self, id):
        self._startProfileId = id

    # virtual method, implemented here
    def validateSettings(self):
        """
        Sanity check platform specific settings
        :return: 0: success, -1: failure, see logfile
        """

        ret = 0

        # validate platform specific arguments
        # For Slocum 2.0, these are ctd_sensor_prefix and start_profile_id

        # Extract platform specific args into object vars
        # Platform specific args are passed in a dictionary
        # Slocum 2.0 supports ctd_sensor_prefix and start_profile_id

        if 'ctd_sensor_prefix' in self.platformArgs :
            self.ctdSensorPrefix = self.platformArgs['ctd_sensor_prefix']

        if 'start_profile_id' in self.platformArgs :
            self.startProfileId = self.platformArgs['start_profile_id']

        # ctdSensorPrefix must be 'sci' or 'm'

        if self.ctdSensorPrefix not in ['sci', 'm']:
            logging.error( 'Slocum 2.0 glider platform requires the ' 
                           'ctd_sensor_prefix values of "sci" or "m"')
            ret = -1

        # validate existence of config files
        # Slocum 2.0 requires deployment, global_attributes,
        # instruments and sensor_defs as json format config files

        self.deploymentCfgPath = os.path.join(
            self.cfgPath, 'deployment.json')
        if not os.path.isfile(self.deploymentCfgPath):
            logging.error(
                'Deployment configuration file not found: '
                '{:s}'.format(self.deploymentCfgPath)
            )
            ret = -1

        self.globalAttributesPath = os.path.join(
            self.cfgPath, 'global_attributes.json')
        if not os.path.isfile(self.globalAttributesPath):
            logging.error(
                'Deployment global attributes file not found: '
                '{:s}'.format(self.globalAttributesPath)
            )
            ret = -1

        self.instrumentsCfgPath = os.path.join(
            self.cfgPath, 'instruments.json')
        if not os.path.isfile(self.instrumentsCfgPath):
            logging.error(
                'Deployment instruments configuration file not found: '
                '{:s}'.format(self.instrumentsCfgPath)
            )

        self.sensorDefsCfgPath = os.path.join(
            self.cfgPath, 'sensor_defs.json')
        if not os.path.isfile(self.sensorDefsCfgPath):
            logging.error(
                'Sensor definitions file not found: '
                '{:s}'.format(self.sensorDefsCfgPath)
            )

        return ret

    # virtual method, implemented here
    def setupFormatting(self):
        """
        Any pre-processing tasks to be done before formatting data
        :return: none
        """

        # define slocum default ctd sensors
        ctd_sensor_names = [
            '{:s}_{:s}'.format(self.ctdSensorPrefix, ctd_sensor) for ctd_sensor in
            ['water_cond', 'water_temp']
        ]
        self.ctdSensors = LLAT_SENSORS + ctd_sensor_names

        # load, validate config settings
        # (punt with exception on errors)
        self._loadConfigData()
        self._validateConfigData()

        # translate output to absolute path (resolving links)
        self.outputPath = os.path.realpath(self.outputPath)

        # Create a status.json file in the config directory to hold
        # information from the latest run
        status_path = os.path.join(self.cfgPath, 'status.json')
        if not os.path.exists(status_path):
            self.status = {
                "history": "", "date_created": "", "date_modified": "",
                "date_issued": "", "version": "", "uuid": "",
                "raw_directory": os.path.dirname(os.path.realpath(self.dataFiles[0])),
                "nc_directory": self.outputPath,
                "next_profile_id": None, "files_processed": [],
                "profiles_created": [], "profiles_uploaded": [],
                "profile_to_data_map": []
            }
        else:
            with open(status_path, 'r') as fid:
                self.status = json.load(fid)

        # get the next profile id if this dataset has been run before.
        # ToDo: for now this works for realtime, but it should be changed to
        #  exclude cases where you might re-run a recovered dataset and clobber.
        if self.status['next_profile_id'] and self.startProfileId > 0:
            self.startProfileId = self.status['next_profile_id']

        # ToDo get a comprehensive set of sensors from default and configured

        # Setup the file writer for output
        self.outputFileWriter.outputPath = self.outputPath
        self.outputFileWriter.overwriteExistingFiles = self.replaceOutputFiles
        self.outputFileWriter.compressionLevel = self.outputCompression
        self.outputFileWriter.outputFormat = self.outputFormat
        self.outputFileWriter.startProfileId = self.startProfileId
        if self.startProfileId >= 1:
            self.outputFileWriter.profileId = self.startProfileId
        self.outputFileWriter.globalAttributes = self.globalAttribs
        self.outputFileWriter.deploymentAttributes = self.deploymentDefs
        self.outputFileWriter.instrumentAttributes = self.instrumentCfgs
        self.outputFileWriter.setup()

    # virtual method, implemented here
    def FormatData( self ):
        """
        Formats all data in passed data files into one or more NetCDF files
        :return: 0 success, <0 failure (see log)
        """

        # If debug mode, go no further
        if self.suppressOutput:
            return 0

        # Write one NetCDF file for each input file
        output_nc_files = []
        source_dba_files = []
        processed_dbas = []
        profile_to_data_map = []

        # For calculated variables, if sensor config contains
        # processing inputs, add to var_processing (used later
        # as input to data processing)
        var_processing = {}
        for var_defs in self.cfgSensorDefs:
            if "processing" in self.cfgSensorDefs[var_defs]:
                var_processing[var_defs] = self.cfgSensorDefs[var_defs].pop(
                    "processing")

        # need slocum input files sorted by mission and segment
        self.dataFiles.sort(key=sort_function)

        # setup multiple log formatters (for header, data logging)
        hdrFormat = logging.Formatter( LOG_HEADER_FORMAT )
        dataFormat = logging.Formatter( LOG_PROCESSING_FORMAT )

        for dataFile in self.dataFiles:

            # change to non-indented log format (see above)
            logging.getLogger().handlers[0].setFormatter( hdrFormat )

            if not os.path.isfile( dataFile):
                logging.error('Invalid dba file specified: {:s}'.format(dataFile))
                continue

            logging.info('Processing data file: {:s}'.format(dataFile))

            # change to indented log format (see above)
            logging.getLogger().handlers[0].setFormatter( dataFormat )

            # Parse the dba file
            dba = DbaData(dataFile)
            if dba is None or dba.N == 0:
                logging.warning('Skipping empty data file: {:s}'.format(dataFile))
                continue

            mission = dba.file_metadata['mission_name'].upper()
            if ( mission == 'STATUS.MI'
                 or mission == 'LASTGASP.MI'
                 or mission == 'INITIAL.MI'):
                logging.info('Skipping {:s} data file'.format(mission))
                continue

            # init empty list of invariant data for processor to populate
            scalars = []

            # slocum processing needs sensor defs, data file list and
            # data file being processed
            self.dataProcessor.cfgSensorDefs = self.cfgSensorDefs
            self.dataProcessor.dataFiles = self.dataFiles
            self.dataProcessor.dataFile = dataFile

            # perform all sensor data calculations and updates
            profiles = self.dataProcessor.processData( dba, scalars, var_processing )
            if profiles is None:
                continue

            # write output netcdf files
            for profile in profiles:

                # Filters out excess data
                profile = self.dataProcessor.reduceProfileToScienceData(profile)

                # Merge datafile sensor definitions with configured sensor defs
                allSensorDefs = self._mergeSensorDefs(self.cfgSensorDefs, profile.sensors)
                self.outputFileWriter.sensors = allSensorDefs

                # Size data sets using sensor flagged in config
                dimensionOnSensor = self._findDimensionSensor( allSensorDefs )
                self.outputFileWriter.dimensionSensor = dimensionOnSensor

                # ToDo: fix the history writer in NetCDFWriter - comment from legacy slocum 2.0 code
                out_nc_file = self.outputFileWriter.write_profile(profile, scalars)
                if out_nc_file:  # can be None if skipping
                    output_nc_files.append(os.path.basename(out_nc_file))
                    source_dba_files.append(os.path.basename(dataFile))
                    profile_to_data_map.append((out_nc_file, dataFile))

            processed_dbas.append(os.path.basename(dataFile))

        # change back to non-indented log format (see above)
        logging.getLogger().handlers[0].setFormatter( hdrFormat )

        return 0

    # post processing cleanup
    def cleanupFormatting(self):
        """
        Post-data formatting cleanup code goes here
        :return:
        """
        self.outputFileWriter.cleanup()

    # *** internal helper methods ***

    def _loadConfigData(self):
        """
        Reads Json configuration files
        :return:
        """

        # Load configured sensor definitions

        try:
            self.cfgSensorDefs = self.cfgReader.readDictionary(
                self.sensorDefsCfgPath)
        except ValueError as e:
            self._logger.error(
                'Error parsing deployment-specific sensor definitions: '
                '{:s} ({:})'.format(self.sensorDefsCfgPath, e)
            )
            raise e

        # Load configured deployment definitions

        try:
            self.deploymentDefs = self.cfgReader.readDictionary(
                self.deploymentCfgPath)
        except ValueError as e:
            self._logger.error(
                'Error loading {:s}: {:}'.format(
                    self.deploymentCfgPath, e)
            )
            raise e

        try:
            self.globalAttribs = self.cfgReader.readDictionary(self.globalAttributesPath)

        except ValueError as e:
            self._logger.error(
                'Error loading {:s}: {:}'.format(
                    self.globalAttributesPath, e)
            )
            raise e

        try:
            self.instrumentCfgs = self.cfgReader.readDictionary(self.instrumentsCfgPath)
        except ValueError as e:
            self._logger.error(
                'Error loading {:s}: {:}'.format(
                    self.instrumentsCfgPath, e)
            )
            raise e

    def _validateConfigData(self):
        """
        Sanity check data read from configuration files.
        Throw exceptions if incompatible with formatting requirements
        :return:
        """

        # Make sure we have llat_* sensors defined in ncw.nc_sensor_defs
        ctd_valid = validate_sensors(self.cfgSensorDefs, self.ctdSensors)
        if not ctd_valid:
            logging.error(
                'Bad sensor definitions: {:s}'.format(self.sensorDefsCfgPath))
            raise RuntimeError("Bad sensor definitions")

        # Make sure we have configured sensor definitions for all IOOS NGDAC
        # required variables
        ngdac_valid = validate_ngdac_var_names(self.cfgSensorDefs)
        if not ngdac_valid:
            logging.error(
                'Bad ngdac sensor definitions: {:s}'.format(self.sensorDefsCfgPath))
            raise RuntimeError("Bad ngdac sensor definitions")

    def _mergeSensorDefs(self, cfgSensors, dataFileSensors):
        """
        Add attributes found in data file sensors that is missing in
        configured sensors

        :param cfgSensors: sensor defs from config file
        :param dataFileSensors: sensor defs from data file or calculations
        :return: merged sensor defs
        """

        allSensorDefs = deepcopy( cfgSensors )

        for sensor in dataFileSensors:
            if sensor not in allSensorDefs:
                continue

            if 'attrs' not in allSensorDefs[sensor]:
                allSensorDefs[sensor]['attrs'] = {}

            for attrName, attrVal in dataFileSensors[sensor]['attrs'].items():

                # Note: The following line from legacy netCDFwriter.py is
                # unclear, but I don't want to change behavior w/o understanding it.
                # It appears to not copy an attribute if matches any sensor name?
                if attrName in allSensorDefs:
                    continue

                if attrName in allSensorDefs[sensor]['attrs']:
                    continue

                allSensorDefs[sensor]['attrs'][attrName] = attrVal

        return allSensorDefs

    def _findDimensionSensor( self, sensorData ):
        """
        Check sensor data for one flagged as "is_dimension"
        :param sensorData: sensor collection to be searched
        for one containing an "is_dimension" attribute
        """

        # Check for the unlimited record dimension after all sensor defs have
        # been updated
        dimSensors = [sensorData[s] for s in sensorData if
                'is_dimension' in sensorData[s]
                and sensorData[s]['is_dimension']]
        if not dimSensors:
            self._logger.warning(
                'No record dimension specified in sensor definitions')
            self._logger.warning(
                'Cannot write NetCDF data until a record dimension is defined')
            return

        if len(dimSensors) != 1:
            self._logger.warning(
                'Multiple record dimensions specified in sensor definitions')
            for dimSensor in dimSensors:
                self._logger.warning(
                    'Record dimension: {:s}'.format(dimSensor['nc_var_name'])
                )
            self._logger.warning('Only one record dimension is allowed')

        return dimSensors[0]

