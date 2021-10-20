"""
class: slocum20DataReader

description:

history:
09/21/2021 ppw created
"""
import logging
from legacy.gliderdac.ooidac.data_classes import DbaData
from FileReader.GliderReader.gliderDataReader import gliderDataReader


class slocum20DataReader( gliderDataReader ) :

    def __init__( self ) :
        super().__init__()
        # TODO

    def readIntoDbaData(self, dataFilePath ):

        # Parse the dba file
        dba = DbaData( dataFilePath )
        if dba is None or dba.N == 0:
            logging.warning('Empty data file: {:s}'.format( dataFilePath ))

        return dba

