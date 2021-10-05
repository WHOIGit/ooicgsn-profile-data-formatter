"""
class: auvPlatform

description: Base class for auv platform classes that  instantiate and call
configured file reader, processor and writer object for that platform

history:
09/21/2021 ppw created
"""
from MobilePlatform.mobilePlatform import mobilePlatform

class auvPlatform( mobilePlatform ) :

    def __init__( self ) :
        super().__init__()

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
