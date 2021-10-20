"""
Unit test for remus600SubsetDataReader.py
"""
import os
import sys
sys.path.append("..")
import inspect
import tempfile
import unittest
import glob
import FileReader.AuvReader.remus600SubsetDataReader as r600reader
import FileReader.AuvReader.remus600SubsetData as r600data
import FileReader.AuvReader.remus600SubsetMsgData as r600msgdata


class TestRemus600SubsetDataReader(unittest.TestCase):

    def getDataFilePath(self, dataFileName):

        # Find the path of the current test module
        # not cwd, as test can be run from anywhere
        testsPath = os.path.abspath(os.path.dirname(inspect.stack()[0][1]))
        dataPath = os.path.join( testsPath, "auvdata", dataFileName)
        return dataPath

    def test_read(self):

        reader = r600reader.remus600SubsetDataReader()
        tempPathObject = tempfile.TemporaryDirectory()
        tempPath = tempPathObject.name
        infilePath = self.getDataFilePath('20210413_113632_AUVsubset_short.txt')
        data = reader.read( infilePath, tempPath )

        self.assertIsNotNone( data, 'Failed to read data file')

        # check output files
        filelist = glob.glob(os.path.join(tempPath, "*.csv"))
        self.assertEqual(16, len(filelist), "Incorrect count of output files")
        for f in filelist:
            print(f)

        tempPathObject.cleanup()


    def test_MsgData(self):

        # Read the subset file into msg specific temp files
        reader = r600reader.remus600SubsetDataReader()
        tempPathObject = tempfile.TemporaryDirectory()
        tempPath = tempPathObject.name
        infilePath = self.getDataFilePath('20210413_113632_AUVsubset_short.txt')
        data = reader.read(infilePath, tempPath)

        self.assertIsNotNone(data, 'Failed to read data file')

        # Read each msg file into a pandas dataframe
        msgsInTestFile = [ 1000, 1001, 1055, 1071, 1075, 1076, 1087, 1089,
                           1174, 1141, 1173, 1102, 1107, 1109, 1117, 1118 ]
        for id in msgsInTestFile:
            df = data.getDataForMessageId( id )
            self.assertIsNotNone( df, "Failed to get message data for " + str(id) )

        # ToDo validate the details of one or more msgType dataframes

        tempPathObject.cleanup()


if __name__ == '__main__':
    unittest.main()
