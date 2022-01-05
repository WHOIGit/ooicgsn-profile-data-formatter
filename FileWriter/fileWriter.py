"""
class: fileWriter

description: Base class for all output file writers/formatters

history:
09/21/2021 ppw created
"""

class fileWriter( ) :

    def __init__( self ) :

        self._outputPath = None
        self._overwriteExistingFiles = False

    @property
    def outputPath(self):
        return self._outputPath

    @outputPath.setter
    def outputPath(self, newpath):
        self._outputPath = newpath

    @property
    def overwriteExistingFiles(self):
        return self._overwriteExistingFiles

    @overwriteExistingFiles.setter
    def overwriteExistingFiles(self, overwrite):
        self._overwriteExistingFiles = overwrite

    # abstract, implement in subclass
    def setupOutput(self):
        raise NotImplementedError()

    # abstract, implement in subclass
    def writeOutput(self ):
        raise NotImplementedError()

    # abstract, implement in subclass
    def cleanupOutput(self):
        raise NotImplementedError()

