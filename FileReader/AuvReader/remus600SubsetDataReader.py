"""
class: remusXYSubsetDataReader

description: Read REMUS 600 Subset Data files.
Subset files are CSV format, sectioned by message type;
with each section having different number and types of parameters
Sections are read in line by line and then written to individual CSV
files (1/message type) in a temporary output path for subsequent
reading and manipulation using Pandas DataFrames. The AuvData and
AuvMessageData classes are used to encapsulate the use of
temporary data files.

history:
09/21/2021 ppw created
"""
import logging
from os import path
from FileReader.AuvReader.auvDataReader import auvDataReader
from FileReader.AuvReader.remus600SubsetData import remus600SubsetData
from FileReader.AuvReader.remus600SubsetMsgData import remus600SubsetMsgData

class remus600SubsetDataReader( auvDataReader ) :

    def __init__( self ) :
        super().__init__()

    def read(self, dataFile, tempOutputPath):
        """
        Read Remus600 Subset file. Save data for each msg type in
        individual .csv files at tempOutputPath. These will be used
        later to read and return msg (instrument) data in Pandas
        DataFrames.
        :param dataFile: path and file name of the Remus 600 subset file
        :param tempOutputPath: path at which to store msg data files
        :return: remus600SubsetData object
        """

        remusData = remus600SubsetData()
        remusData.tempPath = tempOutputPath

        # Read line by line to separate msg sections
        with open( dataFile, 'r', errors='ignore' ) as infile :

            msgId = None
            outfile = None

            while True:
                line = infile.readline()

                # done processing current output file
                if (not line) or line.startswith('Message') :
                    if outfile:
                        outfile.close()
                    if not line:
                        break   # end of input file

                    # use the msg id in line following hdr to
                    # open new output file
                    hdrLine = line
                    line = infile.readline()
                    if not line:
                        break
                    msgId = line.split(',', 1)[0]
                    outfileName = path.join(tempOutputPath,
                                            "tmp_" + str(msgId) + ".csv")
                    outfile = open( outfileName, "a+" )
                    if not outfile:
                        logging.error("Unable to open temp output file " + outfileName)
                        return None

                    # Insert a new subset msg data object
                    msgData = remus600SubsetMsgData()
                    msgData.msgId = int( msgId )
                    msgData.dataFile = outfileName
                    remusData.msgData[ int(msgId) ] = msgData

                    # write the header line
                    outfile.write( hdrLine )

                # write the most recent data line
                outfile.write( line )

        return remusData

