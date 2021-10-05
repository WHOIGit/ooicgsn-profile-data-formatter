"""
class: remus600Platform

description:

history:
09/21/2021 ppw created
"""
from MobilePlatform.AuvPlatform.auvPlatform import auvPlatform
from FileReader.jsonCfgReader import jsonCfgReader
from FileReader.AuvReader.remus600SubsetDataReader import remus600SubsetDataReader
from DataProcessor.AuvProcessor.remus600Processor import remus600Processor
from FileWriter.NetCDFWriter.dacNetCDFWriter import dacNetCDFWriter

class remus600Platform( auvPlatform ) :


    def __init__( self ) :
        super().__init__()

        self.cfgReader = jsonCfgReader()
        self.dataFileReader = remus600SubsetDataReader()
        self.dataProcessor = remus600Processor
        self.outputFileWriter = dacNetCDFWriter

        # TBD { extract platform specific args here }

    # abstract, implement in subclass
    def validateSettings( self ):
        raise NotImplementedError()

    # abstract, implement in subclass
    def setupFormatting(self):
        raise NotImplementedError()

    # abstract, implement in subclass
    def FormatData(self, args ):
        raise NotImplementedError()

    # abstract, implement in subclass
    def cleanupFormatting(self):
        raise NotImplementedError()
