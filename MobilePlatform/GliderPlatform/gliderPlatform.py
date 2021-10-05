"""
class: gliderPlatform

description: Base class for glider platform classes that  instantiate and call
configured file reader, processor and writer object for that platform

history:
09/21/2021 ppw created
"""
from MobilePlatform.mobilePlatform import mobilePlatform


class gliderPlatform( mobilePlatform ) :

    def __init__( self ) :
        super().__init__()

    # abstract virtual methods

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
