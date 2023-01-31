"""
class: dataExplorerNetCDFWriter

description: Output file writer/formatter for OOI-Data Explorer compatible
NetCDF format output files (ie: NetCDF syntax, OOI-Data Explorer content)
This NetCDF writer piggybacks off the dacNetCDFWriter in that it takes as
input, one or more netcdf files representing single profiles of a trajectory
created using that class and converts them into a single output netCDF file
representing the whole trajectory. This is implemented this way in order to
also support Data Explorer output format for Glider data files, which use a
legacy implementation that would otherwise require an additional formatter.

The input netCDF files use a single dimension, time. The output netCDF file
utilizes three dimensions: trajectory, profile and observation. The output
netCDF mimics NetCDF NetCDF files exported from the GliderDAC that are
directly importable into Data Explorer.

history:
09/21/2021 ppw created
"""
from FileWriter.NetCDFWriter.netCDFWriter import netCDFWriter
import logging
import os
from netCDF4 import Dataset, stringtoarr
import datetime

class dataExplorerNetCDFWriter( netCDFWriter ) :

    def __init__( self ) :
        super().__init__()

        self._deploymentId = 'R00000'
        self._trajectoryName = ''
        self._trajectoryDateTime = ''
        self._sourceFile = ""
        self._inputFiles = []

        # internal variables
        self.profileIdList = []
        self.maxObsPerProfile = 0

        # geospatial extent
        self.lonMin = 361.0
        self.lonMax = -361.0
        self.latMin = 91.0
        self.latMax = -91.0
        self.depthMin = 99999.0
        self.depthMax = -1.0

        # temporal extent
        self.dateTimeMin = 9999999999
        self.dateTimeMax = 0
        self.timeResolution = 0.0

    @property
    def deploymentId(self):
        return self._deploymentId

    @deploymentId.setter
    def deploymentId(self, id):
        self._deploymentId = id

    @property
    def trajectoryName(self):
        return self._trajectoryName

    @trajectoryName.setter
    def trajectoryName(self, name):
        self._trajectoryName = name

    @property
    def trajectoryDateTime(self):
        return self._trajectoryDateTime

    @trajectoryDateTime.setter
    def trajectoryDateTime(self, dt):
        self._trajectoryDateTime = dt

    @property
    def sourceFile(self):
        return self._sourceFile

    @sourceFile.setter
    def sourceFile(self, name):
        self._sourceFile = name

    @property
    def inputFiles(self):
        return self._inputFiles

    @inputFiles.setter
    def inputFiles(self, fileList):
        self._inputFiles = fileList

    # virtual method for preparing to write output
    def setupOutput(self):
        """
        Open new NetCDF file and create dimensions
        :return:
        """

        if len(self.trajectoryName) == 0 or \
                len(self.trajectoryDateTime) == 0 or \
                len(self.sourceFile) == 0 or \
                len(self.inputFiles) == 0 :
            logging.error("Uninitialized inputs in setupOutput")
            return

        # Find all profile ids and the profile
        # having the most time steps for sizing
        # the time and observations dimensions

        # clear out computed geospatial and temporal extents

        self.lonMin = 361.0
        self.lonMax = -361.0
        self.latMin = 91.0
        self.latMax = -91.0
        self.depthMin = 99999.0
        self.depthMax = -1.0
        self.dateTimeMin = 9999999999
        self.dateTimeMax = 0
        self.timeResolution = 0.0


        self.profileIdList, self.maxObsPerProfile = self.computeProfileDimensions()

        # Open output netcdf file for trajectory

        filePath = os.path.join( self.outputPath, self.trajectoryName + '_' + self.deploymentId + '.nc' )
        if os.path.exists(filePath):
            if self.overwriteExistingFiles == False:
                logging.warning("File exists, overwrite not selected " + filePath)
                return

        self.nc = Dataset( filePath, mode='w', format=self.writeFormat)

        # Create dimensions: observations profiles, trajectory,
        # traj_strlen, and source_file_strlen

        self.nc.createDimension( 'obs', self.maxObsPerProfile )
        self.nc.createDimension( 'profile', len( self.profileIdList ))
        self.nc.createDimension( 'trajectory', 1 )
        self.nc.createDimension( 'traj_strlen', len(self.trajectoryName) )
        self.nc.createDimension( 'source_file_strlen', len(self.sourceFile) )


    # virtual method for writing output
    def writeOutput(self ):
        """
        Stores data from netCDF input files into a netCDF output file, in
        format and dimensions compatible with GliderDAC netCDF output.
        :return:
        """

        if len(self.profileIdList) == 0 or  \
                self.maxObsPerProfile == 0 :
            logging.error("WriteOutput called before setupOutput")
            return

        # Read a single input file to create all
        # global attributes and variables w/ new dimensions

        self.createVariablesAndAttributes()

        # Traverse all input files, populating
        # the data values of variables

        self.insertVariableValues()

        # Set the trajectory geospatial extent attributes

        self.setGeospatialExtentAttrs()

    # virtual method for output cleanup tasks
    def cleanupOutput(self):
        """
        Close output NetCDF file
        :return:
        """

        # Close the new OOI format netCDF file

        self.nc.close()

    def computeProfileDimensions(self):
        """
        Gather all profile ids from input files and find the one with the
        most time steps for creating the observations dimension
        :return: profileIdList, maxTimesPerProfile
        """

        profileIdList = []
        maxTimesPerProfile = 0

        # Find all profile ids and max time steps in any profile

        for filename in self.inputFiles :

            # Open the netCDF file
            filePath = os.path.join(self.outputPath, filename)
            if os.path.exists(filePath):
                ds = Dataset(filePath, mode='r', format=self.writeFormat)

                for dim in ds.dimensions.values():
                    if dim.name == 'time':
                        if dim.size > maxTimesPerProfile:
                            maxTimesPerProfile = dim.size
                        if maxTimesPerProfile > 1:
                            self.timeResolution = int(ds.variables['time'][1] - \
                                                  ds.variables['time'][0])
                        if ds.variables['time'][0] < self.dateTimeMin:
                            self.dateTimeMin = int(ds.variables['time'][0])
                        if ds.variables['time'][-1] > self.dateTimeMax:
                            self.dateTimeMax = int(ds.variables['time'][-1])
                        break

                for var in ds.variables.values():
                    if var.name == 'profile_id':
                        profileIdList.append( var.getValue() )
                        break

                ds.close()

        return profileIdList, maxTimesPerProfile

    def createVariablesAndAttributes(self) :
        """
        Use a single profile input file to dimension and instantiate variables
        and global attributes. Data for variables will later be populated
        from all profile input files.
        :return:
        """

        # Open the first input netCDF file
        filePath = os.path.join(self.outputPath, self.inputFiles[0])
        if os.path.exists(filePath):
            dsIn = Dataset(filePath, mode='r', format=self.writeFormat)

            # Copy the global attributes to the output netcdf file
            inAttrs = dsIn.ncattrs()
            for attrName in inAttrs:
                self.nc.setncattr( attrName, dsIn.getncattr( attrName ))

            # Create variables corresponding to the trajectory, profile dimensions
            trajVar = self.nc.createVariable( 'trajectory', 'S1', ('trajectory', 'traj_strlen',) )
            trajVar[0,:] = stringtoarr( self.trajectoryName, len(self.trajectoryName))
            trajVar.setncattr( '_ChunkSizes', len(self.trajectoryName))
            #trajVar.setncattr( '_Encoding', 'ISO-885901')
            trajVar.setncattr( 'cf_role', 'trajectory_id')
            trajVar.setncattr( 'comment', 'A trajectory is one deployment')
            trajVar.setncattr( 'ioos_category', 'Identifier')
            trajVar.setncattr( 'long_name', 'Trajectory Name')

            profileIdVar = self.nc.createVariable( 'profile_id', 'i4',
                                                   ('trajectory','profile',),
                                                   fill_value=-999 )
            profileIdVar[:] = self.profileIdList
            profileIdVar.setncattr( 'actual_range', len(self.profileIdList))
            profileIdVar.setncattr( 'ancillary_variables', 'time')
            profileIdVar.setncattr( 'cf_role', 'profile_id')
            profileIdVar.setncattr( 'comment', 'Sequential profile number within the trajectory')
            profileIdVar.setncattr( 'ioos_category', 'Identifier')
            profileIdVar.setncattr( 'long_name', 'Profile ID')
            profileIdVar.setncattr( 'valid_max', 2147483647 )
            profileIdVar.setncattr( 'valid_min', 1 )

            # Traverse input variables, correctly dimension each with
            # combinations of trajectory, profile and observation, as appropriate
            for inVarName, inVar in dsIn.variables.items():

                # Skip pre-configured vars
                if inVarName == 'trajectory' or inVarName == 'profile_id':
                    continue

                if inVar.ndim == 0:
                    if self.isScalarInExplorer( inVar ):
                        # non-dimensional "scalar" -> ()
                        outDims = ()
                    else:
                        # scalar -> profile specific (trajectory, profile)
                        outDims = ('trajectory', 'profile',)
                else:
                    if 'time' in inVar.dimensions:
                        # time -> (trajectory, profile, observation)
                        outDims = ('trajectory', 'profile', 'obs',)
                    else:
                        # strings -> (trajectory, profile, stringlen)
                        outDims = ('trajectory', 'profile', ) + inVar.dimensions

                # _FillValue attribute unique in that it is set on creation
                # only, and if default used, does not show in list of attributes

                fillValue = None
                if "_FillValue" in inVar.ncattrs():
                    fillValue = inVar._FillValue

                outVar = self.nc.createVariable( self.dacVarNameToOoiVarName(inVar.name),
                                                 inVar.dtype,
                                                 outDims,
                                                 fill_value=fillValue )
                for attrName in inVar.ncattrs():
                    if attrName != "_FillValue":
                        if attrName != "ancillary_variables":
                            outVar.setncattr( attrName, inVar.getncattr(attrName))
                        else:
                            outVar.setncattr( attrName,
                               self.dacAttrValsToOoiAttrVals( inVar.getncattr(attrName)) )

            dsIn.close()

        else:
            logging.error('No input files found, unable to create output attributes, variables')

    def isScalarInExplorer(self, inVar ):
        """
        Only some DAC scalar vars remain scalar in Explorer
        :param inVar:
        :return:
        """

        isScalar = False
        if inVar.name == 'crs' or \
            ('type' in inVar.ncattrs() and \
             (inVar.getncattr('type') == 'platform' or \
              inVar.getncattr('type') == 'instrument')):
            isScalar = True

        return isScalar

    def insertVariableValues(self):
        """
        Inserts profile data values from corresponding variables in
        multiple input netCDF files, into a single, n dimensional
        netCDF output file variable containing a whole trajectory
        of profiles.
        :return:
        """

        # Traverse input files to add variable values to output file

        for filename in self.inputFiles :

            # Open the netCDF file
            filePath = os.path.join(self.outputPath, filename)
            if os.path.exists(filePath):
                ds = Dataset(filePath, mode='r', format=self.writeFormat)

                # Retrieve the profile id for data in this file
                for inVarName, inVar in ds.variables.items():
                    if inVarName == 'profile_id':
                        profileId = inVar.getValue()
                        break

                # Insert data value(s), indexed by trajectory and profile
                trajectoryIndex = 0
                profileIndex = profileId - 1

                for inVarName, inVar in ds.variables.items():

                    if inVarName == 'trajectory':
                        continue

                    for outVarName, outVar in self.nc.variables.items():

                        if outVarName == self.dacVarNameToOoiVarName( inVarName ):

                            # string
                            if outVar.dtype == 'S1':
                                strlen = min(outVar.size, inVar.size)
                                outVar[trajectoryIndex, profileIndex, 0:strlen] = \
                                    stringtoarr( inVar[0:strlen], strlen)

                            # temporal
                            elif outVar.ndim == 3:
                                outVar[trajectoryIndex, profileIndex, 0:inVar.size] = inVar[:]

                            # profile specific
                            elif outVar.ndim == 2:
                                outVar[trajectoryIndex, profileIndex] = inVar[:]

                            # scalar
                            else:
                                outVar.assignValue( inVar.getValue() )
                            break

                    if inVarName == 'lat' or inVarName == 'lon' or inVarName == 'depth':
                        self.updateGeospatialExtent( inVar )

                ds.close()

    def dacVarNameToOoiVarName(self, dacVarName):
        """
        Some variables are renamed by DAC. This method maps dac to ooi names
        :param dacVarName:
        :return: ooiVarName
        """

        ooiVarName = dacVarName

        if dacVarName == 'time':
            ooiVarName = 'time'

        elif dacVarName == 'time_qc':
            ooiVarName = 'time_qc'

        elif dacVarName == 'lat':
            ooiVarName = 'precise_lat'

        elif dacVarName == 'lat_qc':

          ooiVarName = 'precise_lat_qc'

        elif dacVarName == 'lon':
            ooiVarName = 'precise_lon'

        elif dacVarName == 'lon_qc':
            ooiVarName = 'precise_lon_qc'

        elif dacVarName == 'profile_time':
            ooiVarName = 'profile_time'

        elif dacVarName == 'profile_time_qc':
            ooiVarName = 'profile_time_qc'

        elif dacVarName == 'profile_lat':
            ooiVarName = 'lat'

        elif dacVarName == 'profile_lat_qc':
            ooiVarName = 'lat_qc'

        elif dacVarName == 'profile_lon':
            ooiVarName = 'lon'

        elif dacVarName == 'profile_lon_qc':
            ooiVarName = 'lon_qc'

        elif dacVarName == 'platform':
            ooiVarName = 'platform_meta'

        return ooiVarName

    def dacAttrValsToOoiAttrVals(self, attrListString ):
        """
        Translates names in a string list from DAC to OOI names
        :param attrListString:
        :return: updated attrListString
        """

        attrVals = attrListString.split(', ')

        outAttrVals = []
        for attrVal in attrVals:
            outAttrVals.append( self.dacVarNameToOoiVarName( attrVal ))

        return ", ".join( outAttrVals )

    def updateGeospatialExtent(self, geoVar ):
        """
        Computes geospatial extent for entire trajectory
        :param geoVar:
        :return: none
        """

        # retrieve variable extent
        varMin = geoVar[:].min()
        varMax = geoVar[:].max()

        if geoVar.name == 'lat':
            if varMin < self.latMin:
                self.latMin = varMin
            if varMax > self.latMax:
                self.latMax = varMax

        elif geoVar.name == 'lon':
            if varMin < self.lonMin:
                self.lonMin = varMin
            if varMax > self.lonMax:
                self.lonMax = varMax

        elif geoVar.name == 'depth':
            if varMin < self.depthMin:
                self.depthMin = varMin
            if varMax > self.depthMax:
                self.depthMax = varMax

    def setGeospatialExtentAttrs(self):
        """
        Define global attributes related go computed geospatial extent
        :return: none
        """

        self.nc.setncattr('Easternmost_Easting', self.lonMax)
        self.nc.setncattr('Westernmost_Easting', self.lonMin)
        self.nc.setncattr('Northernmost_Northing', self.latMax)
        self.nc.setncattr('Southernmost_Northing', self.latMin)

        self.nc.setncattr('geospatial_lat_max', self.latMax )
        self.nc.setncattr('geospatial_lat_min', self.latMin )
        self.nc.setncattr('geospatial_lat_units', "degrees_north")
        self.nc.setncattr('geospatial_lon_max', self.lonMax )
        self.nc.setncattr('geospatial_lon_min', self.lonMin )
        self.nc.setncattr('geospatial_lon_units', "degrees_east" )
        self.nc.setncattr('geospatial_vertical_max', self.depthMax )
        self.nc.setncattr('geospatial_vertical_min', self.depthMin )
        self.nc.setncattr('geospatial_vertical_positive', "down" )
        self.nc.setncattr('geospatial_vertical_units', "m" )

        boundsStr = 'POLYGON ((' + \
            str(self.latMin) + ' ' + str(self.lonMin) + ', ' + \
            str(self.latMax) + ' ' + str(self.lonMin) + ', ' + \
            str(self.latMax) + ' ' + str(self.lonMax) + ', ' + \
            str(self.latMin) + ' ' + str(self.lonMax) + ', ' + \
            str(self.latMin) + ' ' + str(self.lonMin) + '))'

        self.nc.setncattr('geospatial_bounds', boundsStr)
        self.nc.setncattr('geospatial_bounds_crs', 'EPSG:4326')
        self.nc.setncattr('geospatial_bounds_vertical_crs', 'EPSG:5831')

        # Insert time coverage attributes
        self.nc.setncattr( 'time_coverage_start', \
            datetime.datetime.fromtimestamp( self.dateTimeMin ).strftime('%Y%m%dT%H%MZ'))
        self.nc.setncattr( 'time_coverage_end', \
            datetime.datetime.fromtimestamp( self.dateTimeMax ).strftime('%Y%m%dT%H%MZ'))
        self.nc.setncattr( 'time_coverage_duration', \
            'PT' + str( self.dateTimeMax - self.dateTimeMin ) + 'S' )
        self.nc.setncattr( 'time_coverage_resolution', \
                           'PT' + str(self.timeResolution) + 'S' )
