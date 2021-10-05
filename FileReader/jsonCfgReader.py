"""
class: jsonCfgReader

description:

history:
09/21/2021 ppw created
"""
from FileReader.fileReader import fileReader
import legacy.gliderdac.ooidac.readers.json_config as json_config


class jsonCfgReader( fileReader ) :

    def __init__( self ) :
        super().__init__()

    # read file, return as dictionary
    def readDictionary(self, filePath):
        # Use legacy json_config reader, supports comments
        cfgDict = json_config.load( filePath )
        return cfgDict

    # only support read as dictionary
    def readLines(self, filePath):
        raise NotImplementedError()
    def open(self, filePath):
        raise NotImplementedError()
    def readLine(self):
        raise NotImplementedError()
    def readBytes(self, maxBytes):
        raise NotImplementedError()
    def close(self):
        raise NotImplementedError()
