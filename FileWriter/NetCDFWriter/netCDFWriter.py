"""
class: netCDFWriter

description: Base class for all NetCDF output file writers/formatters

history:
09/21/2021 ppw created
"""
import logging

from common.constants import OUTPUT_FORMATS
from FileWriter.fileWriter import fileWriter


class netCDFWriter( fileWriter ) :

    def __init__( self ) :
        super().__init__()

        # initialize object data
        self._compressionLevel = 0
        self._writeFormat = 'NETCDF4_CLASSIC'
        self._startProfileId = 0
        self._profileId = 0
        self._nc = None

    @property
    def compressionLevel(self):
        return self._compressionLevel

    @compressionLevel.setter
    def compressionLevel(self, level):
        if level in range(0, 10):
            self._compressionLevel = level
        else:
            logging.error('Passed compression level out of range, ignored')

    @property
    def writeFormat(self):
        return self._writeFormat

    @writeFormat.setter
    def writeFormat(self, format):
        if format in OUTPUT_FORMATS:
            self._writeFormat = format

    @property
    def startProfileId(self):
        return self._startProfileId

    @startProfileId.setter
    def startProfileId(self, newid):
        self._startProfileId = newid

    @property
    def profileId(self):
        return self._profileId

    @profileId.setter
    def profileId(self, newid):
        self._profileId = newid

    @property
    def nc(self):
        return self._nc

    @nc.setter
    def nc(self, newnc):
        self._nc = newnc

    # abstract, implement in subclass
    def setupOutput(self):
        raise NotImplementedError()

    # abstract, implement in subclass
    def writeOutput(self ):
        raise NotImplementedError()

    # abstract, implement in subclass
    def cleanupOutput(self):
        raise NotImplementedError()
