"""
class: dataExplorerNetCDFWriter

description: Output file writer/formatter for OOI-Data Explorer compatible
NetCDF format output files (ie: NetCDF syntax, OOI-Data Explorer content)
This NetCDF writer piggybacks off the dacNetCDFWriter in that it takes as
input, one or more netcdf files generated for a single trajectory by that
class and converts them into a single output netCDF file for the whole
trajectory, having different dimensions and importable into the OOI
Data Explorer.

history:
09/21/2021 ppw created
"""
from FileWriter.NetCDFWriter.netCDFWriter import netCDFWriter
import logging
import os
from netCDF4 import Dataset, stringtoarr


class dataExplorerNetCDFWriter( netCDFWriter ) :

    def __init__( self ) :
        super().__init__()

        self._trajectoryName = ''
        self._trajectoryDateTime = ''
        self._sourceFile = ""
        self._inputFiles = []

        # internal variables
        self.profileIdList = []
        self.maxObsPerProfile = 0

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

        self.profileIdList, self.maxObsPerProfile = self.computeProfileDimensions()

        # Open output netcdf file for trajectory

        filePath = os.path.join( self.outputPath, self.trajectoryName + '.nc' )
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

        pass

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
            trajVar = self.nc.createVariable( 'trajectory', 'S1', ('trajectory','traj_strlen',) )
            trajVar[:] = stringtoarr( self.trajectoryName, len(self.trajectoryName))
            trajVar.setncattr( '_ChunkSizes', len(self.trajectoryName))
            trajVar.setncattr( '_Encoding', 'ISO-885901')
            trajVar.setncattr( 'cf_role', 'trajectory_id')
            trajVar.setncattr( 'comment', 'A trajectory is one deployment')
            trajVar.setncattr( 'ioos_category', 'Identifier')
            trajVar.setncattr( 'long_name', 'Trajectory Name')

            profileIdVar = self.nc.createVariable( 'profile_id', 'i4',
                                                   ('trajectory','profile',),
                                                   fill_value=-999 )
            profileIdVar[:] = self.profileIdList
            profileIdVar.setncattr( 'actual_range', len(self.profileIdList))
            profileIdVar.setncattr( 'ancillary_variables', 'profile_time')
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
                    # non-dimensional -> (trajectory, profile)
                    outDims = ( 'trajectory', 'profile',)
                else:
                    if 'time' in inVar.dimensions:
                        # time -> (trajectory, profile, observation)
                        outDims = ('trajectory', 'profile', 'obs',)
                    else:
                        # strings -> (trajectory, profile, string)
                        outDims = ('trajectory', 'profile', ) + inVar.dimensions

                # _FillValue attribute unique in that it is set on creation
                # only, and if default used, does not show in list of attributes

                fillValue = None
                if "_FillValue" in inVar.ncattrs():
                    fillValue = inVar._FillValue

                print( self.dacVarNameToOoiVarName(inVar.name) )
                outVar = self.nc.createVariable( self.dacVarNameToOoiVarName(inVar.name),
                                                 inVar.dtype,
                                                 outDims,
                                                 fill_value=fillValue )
                for attrName in inVar.ncattrs():
                    if attrName != "_FillValue":
                        outVar.setncattr( attrName, inVar.getncattr(attrName))

            dsIn.close()

        else:
            logging.error('No input files found, unable to create output attributes, variables')

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

            print(filename)

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

                print( 'trajectoryIndex: ' + str(trajectoryIndex))
                print( 'profileIndex: ' + str(profileIndex))

                for inVarName, inVar in ds.variables.items():

                    print( 'input var: ' + inVarName)

                    for outVarName, outVar in self.nc.variables.items():

                        if outVarName == self.dacVarNameToOoiVarName( inVarName ):

                            print('output var: ' + outVarName)

                            if outVar.dtype == 'S1':
                                print( 'copy 1 string' )
                                outVar[trajectoryIndex, :] = inVar[:]

                            elif outVar.ndim == 3:
                                print('copy n values')
                                outVar[trajectoryIndex, profileIndex, 0:inVar.size] = inVar[:]
                            else:
                                print('copy 1 value')
                                outVar[trajectoryIndex, profileIndex] = inVar.getValue()
                            break

                ds.close()

    def dacVarNameToOoiVarName(self, dacVarName):
        """
        Some variables are renamed by DAC. This method maps dac to ooi names
        :param dacVarName:
        :return: ooiVarName
        """

        ooiVarName = dacVarName

        if dacVarName == 'time':
            ooiVarName = 'precise_time'

        elif dacVarName == 'time_qc':
            ooiVarName = 'precise_time_qc'

        elif dacVarName == 'lat':
            ooiVarName = 'precise_lat'

        elif dacVarName == 'lat_qc':

          ooiVarName = 'precise_lat_qc'

        elif dacVarName == 'lon':
            ooiVarName = 'precise_lon'

        elif dacVarName == 'lon_qc':
            ooiVarName = 'precise_lon_qc'

        elif dacVarName == 'profile_time':
            ooiVarName = 'time'

        elif dacVarName == 'profile_time_qc':
            ooiVarName = 'time_qc'

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
