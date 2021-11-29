"""
class: mobilePlatform

description: Base class for platform classes that  instantiate and call
configured file reader, processor and writer object for that platform

history:
09/21/2021 ppw created
"""
import common.constants as constants
import os
import logging


class mobilePlatform( ) :

    def __init__( self ) :

        # Subclasses instantiate appropriate worker objects
        self._cfgReader = None
        self._dataFileReader = None
        self._dataProcessor = None
        self._outputFileWriter = None

        # Common platform setting defaults
        self._cfgPath = "."
        self._dataFiles = []
        self._targetHost = 'IOOS-DAC'
        self._platformArgs = {}
        self._outputPath = "."
        self._replaceOutputFiles = True
        self._outputFormat = 'NETCDF4_CLASSIC'
        self._outputCompression = 1
        self._suppressOutput = False

        # Initialize config dictionaries to empty
        self._globalsCfg = {}
        self._deploymentCfg = {}
        self._instrumentsCfg = {}
        self._sensorsCfg = {}


    @property
    def cfgReader(self):
        return self._cfgReader
        
    @cfgReader.setter
    def cfgReader(self, reader):
        self._cfgReader = reader

    @property
    def dataFileReader(self):
        return self._dataFileReader

    @dataFileReader.setter
    def dataFileReader(self, reader):
        self._dataFileReader = reader

    @property
    def dataProcessor(self):
        return self._dataProcessor

    @dataProcessor.setter
    def dataProcessor(self, processor):
        self._dataProcessor = processor

    @property
    def outputFileWriter(self):
        return self._outputFileWriter

    @outputFileWriter.setter
    def outputFileWriter(self, writer):
        self._outputFileWriter = writer

    @property
    def cfgPath(self):
        return self._cfgPath
    
    @cfgPath.setter
    def cfgPath(self, newpath):
        self._cfgPath = newpath
        
    @property
    def dataFiles(self):
        return self._dataFiles
    
    @dataFiles.setter
    def dataFiles(self, filelist):
        self._dataFiles = filelist

    @property
    def targetHost(self):
        return self._targetHost

    @targetHost.setter
    def targetHost(self, newhost):
        self._targetHost = newhost

    @property
    def platformArgs(self):
        return self._platformArgs

    @platformArgs.setter
    def platformArgs(self, argdict):
        self._platformArgs = argdict

    @property
    def outputPath(self):
        return self._outputPath

    @outputPath.setter
    def outputPath(self, newpath):
        self._outputPath = newpath

    @property
    def replaceOutputFiles(self):
        return self._replaceOutputFiles

    @replaceOutputFiles.setter
    def replaceOutputFiles(self, replace):
        self._replaceOutputFiles = replace

    @property
    def outputFormat(self):
        return self._outputFormat

    @outputFormat.setter
    def outputFormat(self, newformat):
        self._outputFormat = newformat

    @property
    def outputCompression(self):
        return self._outputCompression

    @outputCompression.setter
    def outputCompression(self, level):
        self._outputCompression = level

    @property
    def suppressOutput(self):
        return self._suppressOutput

    @suppressOutput.setter
    def suppressOutput(self, suppress):
        self._suppressOutput = suppress

    @property
    def globalsCfg(self):
        return self._globalsCfg

    @globalsCfg.setter
    def globalsCfg(self, globals):
        self._globalsCfg = globals

    @property
    def deploymentCfg(self):
        return self._deploymentCfg

    @deploymentCfg.setter
    def deploymentCfg(self, deployment):
        self._deploymentCfg = deployment

    @property
    def instrumentsCfg(self):
        return self._instrumentsCfg

    @instrumentsCfg.setter
    def instrumentsCfg(self, instruments):
        self._instrumentsCfg = instruments

    @property
    def sensorsCfg(self):
        return self._sensorsCfg

    @sensorsCfg.setter
    def sensorsCfg(self, sensors):
        self._sensorsCfg = sensors

    # abstract virtual methods

    # abstract, implement in subclass
    def validateSettings(self ):
        raise NotImplementedError()

    # abstract, implement in subclass
    def setupFormatting(self):
        raise NotImplementedError()

    # abstract, implement in subclass
    def FormatData(self ):
        raise NotImplementedError()

    # abstract, implement in subclass
    def cleanupFormatting(self):
        raise NotImplementedError()

    def readCfgFile(self, cfgPath, cfgFile):
        """
        Reads a configuration file into a dictionary structure
        :param cfgPath:
        :param cfgFile:
        :return: cfgDict
        """

        # Load configuration definitions

        cfgDict = {}
        try:
            cfgFilePath = os.path.join( cfgPath, cfgFile)
            cfgDict = self.cfgReader.readDictionary( cfgFilePath )
            return cfgDict

        except ValueError as e:
            self._logger.error(
                'Error parsing config file {:s}'.format( cfgFile ))
            raise e


