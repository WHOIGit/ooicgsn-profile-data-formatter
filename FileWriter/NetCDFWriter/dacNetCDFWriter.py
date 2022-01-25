import logging
import datetime
from dateutil import parser
import os
import shutil
import numpy as np
import tempfile
import uuid
from netCDF4 import Dataset, stringtoarr
from FileWriter.NetCDFWriter.netCDFWriter import netCDFWriter

class dacNetCDFWriter(netCDFWriter) :
    """
    IOOS-DAC specific NetCDF file writer for mobile platform data
    """

    def __init__( self ) :
        super().__init__()

        # initialize data structures:

        self._fileName = ""
        self._profileId = 0
        self._profileStartTime = 0.0
        self._profileEndTime = 0.0
        self._trajectory = ""
        self._trajectoryDateTime = ""
        self._sourceFile = ""

        # variables: list of dictionaries (1/var)
        #    each having name, datatype, dimension, attributes, values, times

        self._vars = []

        # global attributes: dictionary
        #    each attribute consisting of a name and value

        self._globalAttrs = {}

    @property
    def vars(self):
        return self._vars

    @property
    def globalAttrs(self):
        return self._globalAttrs

    @property
    def fileName(self):
        return self._fileName

    @fileName.setter
    def fileName(self, name):
        self._fileName = name

    @property
    def profileId(self):
        return self._profileId

    @profileId.setter
    def profileId(self, id):
        self._profileId = id

    @property
    def profileStartTime(self):
        return self._profileStartTime

    @profileStartTime.setter
    def profileStartTime(self, at):
        self._profileStartTime = at

    @property
    def profileEndTime(self):
        return self._profileEndTime

    @profileEndTime.setter
    def profileEndTime(self, at):
        self._profileEndTime = at

    @property
    def trajectory(self):
        return self._trajectory

    @trajectory.setter
    def trajectory(self, name):
        self._trajectory = name

    @property
    def trajectoryDateTime(self):
        return self._trajectoryDateTime

    @trajectoryDateTime.setter
    def trajectoryDateTime(self, name):
        self._trajectoryDateTime = name

    @property
    def sourceFile(self):
        return self._sourceFile

    @sourceFile.setter
    def sourceFile(self, name):
        self._sourceFile = name

    def resetAll(self):
        """
        Clears instance vars to re-use dacNetCDFWriter for next profile
        :return: none
        """

        self.fileName = ""
        self.profileId = 0
        self.profileStartTime = 0.0
        self.profileEndTime = 0.0
        self.trajectory = ""
        self.sourceFile = ""
        self.globalAttrs.clear()
        self.vars.clear()
        self.nc = None

    def addVariable(self, name, vartype, dimensionVar, attrDict, values, times):
        """
        Store date elements defining an output variable
        :param name:
        :param vartype:
        :param dimensionVar:
        :param attrDict:
        :param values:
        :param times:
        :return: None
        """

        newVar = {}
        newVar['name'] = name
        newVar['type'] = vartype
        newVar['dimension'] = dimensionVar
        newVar['attrs'] = attrDict
        newVar['values'] = values
        newVar['times'] = times
        self.vars.append( newVar )

    def addGlobalAttr(self, name, value):
        """
        Insert attribute in global attribute dictionary
        :param name:
        :param value:
        :return: None
        """

        self.globalAttrs[name] = value

    def setupOutput(self):
        """
        Virtual method to perform pre-processing for writing to netCDF
        :return: None
        """

        # Open the netCDF file
        filePath = os.path.join( self.outputPath, self.fileName)
        if os.path.exists(filePath):
            if self.overwriteExistingFiles == False:
                logging.warning("File exists, overwrite not selected " + self.fileName)
                return

        self.nc = Dataset( filePath, mode='w', format=self.writeFormat)

        # Create dimensions: time (unlimited), traj_strlen, source_file_strlen

        self.nc.createDimension( 'time', None )
        self.nc.createDimension( 'traj_strlen', len(self.trajectory) )
        self.nc.createDimension( 'source_file_strlen', len(self.sourceFile) )

        # Create variables for trajectory and source file strings

        self.addVariable('trajectory', 'S1', 'traj_strlen',
                         {'cf_role':'trajectory_id',
                          'comment':'a single deployment of an AUV',
                          'long_name':'Trajectory/deployment name',
                          '_ChunkSizes': str(len(self.trajectory)) + 'U'},
                         self.trajectory, None )

        self.addVariable('source_file', 'S1', 'source_file_strlen',
                         {'comment': 'Name of the source data file and associated file metadata',
                          'long_name': 'Source data file',
                          '_ChunkSizes': str(len(self.sourceFile)) + 'U'},
                         self.sourceFile, None)

    def writeOutput(self):
        """
        Virtual method to write variables and attributes to netCDF file
        :return: None
        """

        # Write global attributes

        self.writeAttributes()

        # Create time var on 1/10 sec cadence

        times = np.arange( self.profileStartTime, self.profileEndTime, 0.1 )
        timeVar = self.nc.createVariable('time', np.float64, ('time',), fill_value=False)
        timeVar[:] = times

        # add the passed time attributes to the time variable
        for var in self.vars:
            if var['name'] == 'time':
                for key, val in var['attrs'].items():
                    if not self.attrExcluded(key):
                        self.nc.variables['time'].setncattr(key, val)
                break

        # Write passed variables

        for var in self.vars:
            if var['name'] != 'time':

                if var['dimension'] == 'time':
                    self.writeTemporalVariable( var )

                else:
                    self.writeStaticVariable( var )

    def cleanupOutput(self):
        """
        Virtual method to cleanup after writing to NetCDF file
        :return: None
        """

        # Close netCDF file

        self.nc.close()

        pass

    def writeAttributes(self):
        """
        Writes global attributes to NetCDF file
        :return: None
        """

        for key, value in self.globalAttrs.items():
            strVal = None
            if value is not None:
                strVal = str(value)
            self.nc.setncattr( key, strVal )

    def writeTemporalVariable(self, var ):
        """
        Write time dimensioned variable to netCDF file
        :param var:
        :return: None
        """

        if var['times'] is not None:

            # Use default NetCDF4 fill value, unless specified
            varFillValue = self.fillValueToNcFillValue(var)

            ncVar = self.nc.createVariable( var['name'], var['type'], ('time',),
                                            fill_value = varFillValue )

            # Write temporal variable's data on individual cadence
            # rounded to 1/10 second

            timeIndices = ((var['times'] - self.profileStartTime) / 0.1).astype(int)
            ncVar[ timeIndices ] = var['values']

            # Set the variable's passed attributes

            for key, val in var['attrs'].items():
                if not self.attrExcluded(key):
                    ncVar.setncattr(key, val)

        else:
            logging.warning('Time based variable ' + var['name'] +
                            ' encountered. Ignored')

    def writeStaticVariable(self, var):
        """
        Write static variable and attributes to NetCDF file
        :param var:
        :return: None
        """

        # Use default NetCDF4 fill value, unless specified
        varFillValue = self.fillValueToNcFillValue(var)

        # dimension may be unset or set to None
        if var.get('dimension') is None:
            ncVar = self.nc.createVariable( var['name'], var['type'], (),
                                            fill_value=varFillValue)
        else:
            ncVar = self.nc.createVariable( var['name'], var['type'], (var['dimension'],),
                                            fill_value=varFillValue)

        # Set any passed value for the variable (should be none, unless
        # string value)
        if var['values'] is not None:
            if var['dimension'] is None:
                ncVar.assignValue( var['values'] )
            else:
                ncVar[:] = stringtoarr( var['values'], len(var['values'] ))

        # Set the variable's passed attributes

        for key, val in var['attrs'].items():
            if not self.attrExcluded( key ):
                ncVar.setncattr(key, val)


    def fillValueToNcFillValue(self, var ):
        """
        Convert fill value attribute to NetCDF fill value
        :param var:
        :return: None if no fill value attribute, else numeric value
        """

        varFillValue = None
        if "_FillValue" in var['attrs']:
            if var['type'] == 'f8':
                varFillValue = float(var['attrs']['_FillValue'])
            elif var['type'] == 'i4':
                varFillValue = int( var['attrs']['_FillValue'])

        return varFillValue

    def attrExcluded(self, key ):
        """
        Attribute key names not to put in output CDF file
        :param key: 
        :return: True - do not insert, False - insert
        """

        return key in [ '_FillValue',
                        'subset_msg_id',
                        'subset_field']
    