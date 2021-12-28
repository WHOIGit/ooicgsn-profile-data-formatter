"""
class: dacLegacyNetCDFWriter

description: Output file writer/formatter for IOOS-DAC compatible
NetCDF format output files (ie: NetCDF syntax, IOOS-DAC content)

history:
09/21/2021 ppw created
"""
import logging
import datetime
from dateutil import parser
import os
import shutil
import numpy as np
import tempfile
import uuid
from FileWriter.NetCDFWriter import netCDFWriter
from netCDF4 import Dataset, stringtoarr
from shapely.geometry import Polygon

from legacy.gliderdac.ooidac.constants import REQUIRED_SENSOR_DEFS_KEYS, NC_FILL_VALUES
from FileWriter.NetCDFWriter.netCDFWriter import netCDFWriter

class dacLegacyNetCDFWriter(netCDFWriter) :
    """
    IOOS-DAC specific NetCDF file writer for mobile platform data
    """

    def __init__( self ) :
        super().__init__()

        self._fileType = "rt"    # realtime (vs delayed)
        self._attributes = { 'deployment': {},
                             'global': {},
                             'instruments': {}}
        self._sensors = {}
        self._dimensionSensor = None
        self._nc = None
        self._out_nc = None
        self._profileId = None
        self._cdm_data_type = 'Profile'
        self._trajectory = None

    @property
    def fileType(self):
        return self._fileType

    @fileType.setter
    def fileType(self, ftype):
        self._fileType = ftype

    @property
    def deploymentAttributes(self):
        return self._attributes['deployment']

    @deploymentAttributes.setter
    def deploymentAttributes(self, attribs):
        self._attributes['deployment'] = attribs

    @property
    def globalAttributes(self):
        return self._attributes['global']

    @globalAttributes.setter
    def globalAttributes(self, attribs):
        self._attributes['global'] = attribs

    @property
    def instrumentAttributes(self):
        return self._attributes['instruments']

    @instrumentAttributes.setter
    def instrumentAttributes(self, attribs):
        self._attributes['instruments'] = attribs

    @property
    def sensors(self):
        return self._sensors

    @sensors.setter
    def sensors(self, sensorDefs):
        self._sensors = sensorDefs

    @property
    def dimensionSensor(self):
        return self._dimensionSensor

    @dimensionSensor.setter
    def dimensionSensor(self, sensor):
        self._dimensionSensor = sensor

    def init_nc(self, tmp_out_nc, nc_filename):
        """Initialize a new NetCDF file (netCDF4.Dataset):
        (unfortunately duplicated from legacy code)

        1. Open the file in write mode
        2. Create the record dimension
        3. Set all global attributes
        4. Update the history global attribute
        5. Create the platform variable
        6. Create the instrument variable
        7. Close the file
        """

        if self.nc:
            logging.error('Existing netCDF4.Dataset: {}'.format(self.nc))
            return

        try:
            self.nc = Dataset(
                tmp_out_nc, mode='w', clobber=True, format=self.writeFormat)
        except OSError as e:
            logging.critical(
                'Error initializing {:s} ({})'.format(tmp_out_nc, e)
            )
            return

        if not self.dimensionSensor:
            logging.error(
                'No record dimension found in sensor definitions')
            return

        # Create the record dimension
        self.nc.createDimension(
            self.dimensionSensor['nc_var_name'],
            size=self.dimensionSensor['dimension_length']
        )

        # Store the NetCDF destination name
        self._out_nc = tmp_out_nc

        # Write global attributes
        # Add date_created, date_modified, date_issued globals
        nc_create_ts = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        self._attributes['global']['date_created'] = nc_create_ts
        self._attributes['global']['date_issued'] = nc_create_ts
        self._attributes['global']['date_modified'] = nc_create_ts
        # Add history attribute if not present in self._attributes['global']
        if 'history' not in self._attributes['global']:
            self._attributes['global']['history'] = ' '
        if 'id' not in self._attributes['global']:
            self._attributes['global']['id'] = ' '

        # Add the global cdm_data_type attribute
        # MUST be 'Trajectory'
        self._attributes['global']['cdm_data_type'] = self._cdm_data_type
        # Add the global featureType attribute
        # MUST be 'trajectory'
        self._attributes['global']['featureType'] = self._cdm_data_type.lower()

        # Write the NetCDF global attributes
        self.set_global_attributes()

        # Update global history attribute
        self.update_history('{:s}.nc created'.format(nc_filename))

        # Create platform container variable
        self.set_platform()

        # Create instrument container variables
        self.set_instruments()

        # Generate and add a UUID global attribute
        self.nc.setncattr('uuid', '{:s}'.format(str(uuid.uuid4())))

        self.nc.close()

    def open_nc(self):
        """Open the current NetCDF file (self._nc) in append mode and set the
        record dimension array index for appending data.
        (unfortunately duplicated from legacy code)
        """

        if not self._out_nc:
            logging.error('The NetCDF file has not been initialized')
            return

        if self.nc and self.nc.isopen():
            logging.error(
                'netCDF4.Dataset is already open: {:s}'.format(self._nc)
            )
            return
            # raise GliderNetCDFWriterException(
            #   'netCDF4.Dataset is already open: {:s}'.format(self._nc)
            #   )

        # Open the NetCDF in append mode
        self.nc = Dataset(self._out_nc, mode='a')

    def finish_nc(self):
        """Close the NetCDF file permanently, updates some global attributes and
        delete instance properties to prevent any further appending to the file.
        The file must be reopened with self.open_nc if further appending is
        required.
        (unfortunately duplicated from legacy code)
        """

        if not self._out_nc or not os.path.isfile(self._out_nc):
            logging.error('No output NetCDF file specified')
            return

        if not self.nc:
            logging.error('The NetCDF file has not been initialized')
            return

        if not self.nc.isopen():
            logging.warning(
                'The NetCDF file is already closed: {:s}'.format(self._out_nc)
            )
            return

        # Set profile variables
        self._update_profile_vars()
        # Update global geospatial attributes
        self._update_geospatial_global_attributes()
        # Update global time_coverage attributes
        self._update_time_coverage_global_attributes()

        self.nc.close()

        self.nc = None

        return self._out_nc

    def setup(self):
        """
        create a temporary directory to hold temporary files during writing
        these are moved to the final output directory upon completion without
        errors
        """
        self.tempDir = os.path.join(self.outputPath, "tempfiles")
        if not os.path.isdir(self.tempDir):
            os.mkdir(self.tempDir)
        logging.debug('Temporary NetCDF directory: {:s}'.format(
            self.tempDir))

        # Set trajectory name from deployment attributes
        # Use the trajectory_name, if present, in deployment.json.
        # Otherwise, use trajectory_datetime.
        if 'trajectory_name' in self.deploymentAttributes and len(
                self.deploymentAttributes['trajectory_name'].strip()) > 0:
            self._trajectory = self.deploymentAttributes['trajectory_name']
        else:
            if 'trajectory_datetime' not in self.deploymentAttributes:
                self._logger.error(
                    'No trajectory_datetime key in deployment.json: '
                    '{:s}'.format(self._deployment_config_path)
                )
            else:
                try:
                    trajectory_dt = parser.parse(
                        self.deploymentAttributes['trajectory_datetime'])
                    self._trajectory = '{:s}-{:s}'.format(
                        self.deploymentAttributes['glider'],
                        trajectory_dt.strftime('%Y%m%dT%H%M')
                    )
                except ValueError as e:
                    self._logger.error(
                        'Error parsing deployment trajectory_date: {:s} '
                        '({:s})'.format(
                            self.deploymentAttributes['trajectory_date'], e)
                    )


    def cleanup(self):
        """
        Delete the temporary directory once files have been moved
        :return:
        """

        try:
            logging.debug('Removing temporary files:')
            shutil.rmtree(self.tempDir)
        except OSError as e:
            logging.error(e)

    def write_profile(self, profile, scalarVars):
        """
        Formats Slocum 2.0 specific data into IOOS-DAC compatible NetCDF files
        (Copied from legacy netCDFWriter, modified as needed)

        :param profile: A GliderData instance that contains a profile's data
        :param scalarVars: A dictionary of scalar variables to write to the
            netcdf in addition to the GliderData instance
        :return:
        """

        # compute mean profile time for id
        profile_times = profile.getdata('llat_time')
        # Calculate and convert profile mean time to a datetime
        prof_start_time = float(profile_times[0])
        mean_profile_epoch = float(np.nanmean([profile_times[0],
                                               profile_times[-1]]))
        if np.isnan(mean_profile_epoch):
            logging.warning('Profile mean timestamp is Nan')
            return

        # If start profile id is set to 0, on the command line,
        # use the mean_profile_epoch as the profile_id since it will be
        # unique to this profile and deployment
        if self.startProfileId < 1:
            self.profileId = int(prof_start_time)

        pro_mean_dt = datetime.datetime.utcfromtimestamp(mean_profile_epoch)
        prof_start_dt = datetime.datetime.utcfromtimestamp(prof_start_time)

        # Create the output NetCDF path
        pro_mean_ts = pro_mean_dt.strftime('%Y%m%dT%H%M%SZ')
        prof_start_ts = prof_start_dt.strftime('%Y%m%dT%H%M%SZ')
        profile_filename = '{:s}_{:s}_{:s}'.format(
            self.deploymentAttributes['glider'], prof_start_ts,
            self.fileType)
        # Path to temporarily hold file while we create it
        tmp_fid, tmp_nc = tempfile.mkstemp(
            dir=self.tempDir, suffix='.nc',
            prefix=os.path.basename(profile_filename)
        )
        os.close(tmp_fid)  # comment why this is necessary?

        out_nc_file = os.path.join(self.outputPath, '{:s}.nc'.format(
            profile_filename))
        if os.path.isfile(out_nc_file):
            if self.overwriteExistingFiles:
                logging.info(
                    'Clobbering existing NetCDF: {:s}'.format(out_nc_file))
            else:
                logging.warning(
                    'Skipping existing NetCDF: {:s}'.format(out_nc_file))
                return

        # Initialize the temporary NetCDF file
        try:
            self.init_nc(tmp_nc, profile_filename)
        except (OSError, IOError) as e:
            logging.error('Error initializing {:s}: {}'.format(tmp_nc, e))
            return

        try:
            self.open_nc()

        except (OSError, IOError) as e:
            logging.error('Error opening {:s}: {}'.format(tmp_nc, e))
            os.unlink(tmp_nc)
            return

        # Create and set the trajectory
        # trajectory_string = '{:s}'.format(ncw.trajectory)
        self.set_trajectory_id()
        # Update the global title attribute with the name of the source
        # dba file
        self.set_title(
            '{:s}-{:s} Vertical Profile'.format(
                self.deploymentAttributes['glider'],
                pro_mean_ts
            )
        )

        # Create the source file scalar variable
        self.set_source_file_var(
            profile.file_metadata['filename_label'], profile.file_metadata)

        # Update the self.nc_sensors_defs with the dba sensor definitions
        self.update_data_file_sensor_defs(profile.sensors)

        # Update the sensor defs and add the scalar variables
        for scalar in scalarVars:
            self.update_sensor_def(scalar['nc_var_name'], scalar)
            self.set_scalar(scalar['nc_var_name'], scalar['data'])

        # Find and set container variables
        self.set_container_variables()

        # Create variables and add data
        nc_sensor_names = list(self.sensors.keys())
        sensors_to_write = np.intersect1d(
            profile.sensor_names, nc_sensor_names)
        for var_name in sensors_to_write:
            var_data = profile.getdata(var_name)
            logging.debug('Inserting {:s} data array'.format(var_name))
            self.insert_var_data(var_name, var_data)

        # Write scalar profile variable and permanently close the NetCDF
        # file

        nc_file = self.finish_nc()

        if nc_file:
            try:
                shutil.move(tmp_nc, out_nc_file)
                # --Removing the chmod line because it is bad form to presume
                # --permissions, plus it fails across remote drives with
                # --different file systems.
                # os.chmod(out_nc_file, 0o755)
            except IOError as e:
                logging.error(
                    'Error moving temp NetCDF file {:s}: {:}'.format(
                        tmp_nc, e)
                )
                return

        # If all is successful, and profile_id is sequential, increment
        if self.startProfileId > 0:
            self.profileId += 1

        return out_nc_file

    # *** Methods below copied directly from legacy NetCDFWriter    ***
    # *** and modified as needed (Structure of NetCDFWriter did not ***
    # *** lend itself to direct integration.                        ***

    def set_global_attributes(self):
        """ Sets a dictionary of values as global attributes
        """

        for key, value in sorted(self._attributes['global'].items()):
            try:
                self._nc.setncattr(key, value)
            except TypeError as e:
                logging.error(
                    'Error setting global attribute {:s} ({:})'.format(key, e)
                )

    def update_history(self, message):
        """ Updates the global history attribute with the message appended to
        and ISO8601:2004 timestamp
        """

        # Get timestamp for this access
        now_time_ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        history_string = '{:s}: {:s}\n'.format(now_time_ts, message)
        if 'history' not in self.nc.ncattrs():
            self.nc.setncattr('history', history_string)
            return

        previous_history = self.nc.history.strip()
        if not previous_history:
            self.nc.history = history_string
        else:
            self.nc.history += history_string

    def sensor_def_exists(self, sensor):
        """Return the sensor definition for the specified sensor if it exists
        and is properly configured.  Returns None otherwise or if the sensor
        definition is missing any of REQUIRED_SENSOR_DEFS_KEYS """

        if sensor not in self.sensors:
            logging.debug('No {:s} sensor definition found'.format(sensor))
            return None

        sensor_def = self.sensors[sensor]
        for required_key in REQUIRED_SENSOR_DEFS_KEYS:
            if required_key not in sensor_def:
                logging.warning(
                    '{:s} sensor definition is missing the required key:'
                    '{:s}'.format(sensor, required_key)
                )
                sensor_def = None

        return sensor_def

    def check_datatype_exists(self, sensor):
        """Creates the NetCDF variable for sensor using the sensor definition in
        self._sensors[sensor] if the sensor definition exists

        Parameters:
            sensor: native glider sensor name

        Returns:
            sensor_def: the sensor definition from self._sensors[sensor]

            or

            None if the sensor definition does not exist or if there was an
            error creating the NetCDF variable
        """

        sensor_def = self.sensor_def_exists(sensor)
        if not sensor_def:
            return sensor_def

        if sensor_def['nc_var_name'] in self._nc.variables:
            logging.debug(
                'NetCDF variables {:s} already exists'.format(
                    sensor_def['nc_var_name'])
            )
            var_exists = sensor_def
        else:
            var_exists = self.set_datatype(sensor, sensor_def)

        # may return None if the variable was not created by set_datatype
        return var_exists

    def set_scalar(self, sensor, value=None):
        """Create the NetCDF scalar variable specified in
        self._sensors[sensor]

        Parameters:
            sensor: native glider sensor name
            value: optional scalar value to store in the variable

        Returns:
            True if created, None if not created
        """
        sensor_def = self.check_datatype_exists(sensor)
        if not sensor_def:
            logging.warning(
                'No sensor definition found for NetCDF scalar: {:s}'.format(
                    sensor)
            )
            return

        # Store the value in the scalar if there is one
        if value:
            self._nc.variables[sensor_def['nc_var_name']].assignValue(value)

        return True

    def set_platform(self):
        """ Creates a variable that describes the glider
        """

        self.set_scalar('platform')
        for key, value in sorted(
                self._attributes['deployment']['platform'].items()):
            self._nc.variables['platform'].setncattr(key, value)

    def _set_instrument(self, name, var_type, attrs):
        """ Adds a description for a single instrument
        """

        if name not in self._nc.variables:
            self._nc.createVariable(
                name,
                var_type,
                fill_value=NC_FILL_VALUES[var_type]
            )

        for key, value in sorted(attrs.items()):
            self._nc.variables[name].setncattr(key, value)

    def set_instruments(self):
        """ Adds a list of instrument descriptions to the dataset
        """

        for description in self._attributes['instruments']:
            self._set_instrument(
                description['nc_var_name'],
                description['type'],
                description['attrs']
            )

    def set_profile_var(self):
        """ Sets Profile ID in NetCDF File
        """

        self.set_scalar('profile_id', self.profileId)

    def _update_profile_vars(self):
        """ Internal function that updates all profile variables
        before closing a file
        """

        logging.debug('Updating profile scalar variables')

        # Set the profile_id variable
        self.set_profile_var()

        time_sensor_def = self.sensor_def_exists('llat_time')
        if not time_sensor_def:
            logging.warning('Skipping creation of profile_time variable')
        else:
            time_var_name = time_sensor_def['nc_var_name']
            if time_var_name in self._nc.variables:
                self.set_scalar(
                    'profile_time',
                    np.mean(self._nc.variables[time_var_name][[0, -1]])
                )
            else:
                logging.warning(
                    'Cannot set profile_time '
                    '(missing {:s} variable)'.format(time_var_name)
                )

        # Longitude sensor definition
        lon_sensor_def = self.sensor_def_exists('llat_longitude')
        # depth-average current longitude sensor definition
        lon_uv_sensor_def = self.sensor_def_exists('lon_uv')
        # Latitude sensor definition
        lat_sensor_def = self.sensor_def_exists('llat_latitude')
        # depth-averaged current latitude sensor definition
        lat_uv_sensor_def = self.sensor_def_exists('lat_uv')
        if not lon_sensor_def:
            logging.warning('Skipping creation of profile_lon')
        else:
            lon_var_name = lon_sensor_def['nc_var_name']
            if lon_var_name in self._nc.variables:
                mean_lon = np.nanmean(self._nc.variables[lon_var_name][:])
                self.set_scalar('profile_lon', mean_lon)
                if lon_uv_sensor_def:
                    if not self._nc.variables['lon_uv'][:]:
                        self.set_scalar('lon_uv', mean_lon)
                    else:
                        logging.debug(
                            '_update_profile_vars: lon_uv data already exists')
                else:
                    logging.debug(
                        'lon_uv not created: sensor definition does not exist')
            else:
                logging.warning(
                    'Cannot set profile_lon '
                    '(missing {:s} variable)'.format(lon_var_name)
                )

        if not lat_sensor_def:
            logging.warning('Skipping creation of profile_lat')
        else:
            lat_var_name = lat_sensor_def['nc_var_name']
            if lat_var_name in self._nc.variables:
                mean_lat = np.nanmean(self._nc.variables[lat_var_name][:])
                self.set_scalar('profile_lat', mean_lat)
                if lat_uv_sensor_def:
                    if not self._nc.variables['lat_uv'][:]:
                        self.set_scalar('lat_uv', mean_lat)
                    else:
                        logging.debug(
                            '_update_profile_vars: lat_uv data already exists')
                else:
                    logging.debug(
                        'lat_uv not created: sensor definition does not exist')
            else:
                logging.warning(
                    'Cannot set profile_lat '
                    '(missing {:s} variable)'.format(lat_var_name)
                )

    def _update_time_coverage_global_attributes(self):
        """Update all global time_coverage attributes.  The following global
        attributes are created/updated:
            time_coverage_start
            time_coverage_end
            time_coverage_duration
        """

        # time_var_name = self.sensor_def_exists('drv_timestamp')
        time_sensor_def = self.sensor_def_exists('llat_time')
        if not time_sensor_def:
            logging.warning(
                'Failed to set global time_coverage_start/end attributes')
            return

        time_var_name = time_sensor_def['nc_var_name']
        min_timestamp = self._nc.variables[time_var_name][:].min()
        max_timestamp = self._nc.variables[time_var_name][:].max()
        try:
            dt0 = datetime.datetime.utcfromtimestamp(min_timestamp)
        except ValueError as e:
            logging.error(
                'Error parsing min {:s}: {:s} ({:s})'.format(
                    time_var_name, min_timestamp, e)
            )
            logging.error('If it made it this far with '
                               'incorrect timestamps, that would be '
                               'impressive, but the function must fail here.')
            return
        try:
            dt1 = datetime.datetime.utcfromtimestamp(max_timestamp)
        except ValueError as e:
            logging.error(
                'Error parsing max {:s}: {:s} ({:s})'.format(
                    time_var_name, max_timestamp, e)
            )
            return

        self._nc.setncattr(
            'time_coverage_start', dt0.strftime('%Y-%m-%dT%H:%M:%SZ'))
        self._nc.setncattr(
            'time_coverage_end', dt1.strftime('%Y-%m-%dT%H:%M:%SZ'))
        self._nc.setncattr(
            'time_coverage_duration', self.delta_to_iso_duration(dt1 - dt0))

        # Calculate the approximate time_coverage_resolution
        num_seconds = (dt1 - dt0).total_seconds()
        data_length = self._nc.variables[time_var_name].size
        resolution_seconds = num_seconds / data_length

        self._nc.setncattr(
            'time_coverage_resolution',
            self.delta_to_iso_duration(resolution_seconds)
        )

    def _update_geospatial_global_attributes(self):
        """Update all global geospatial_ min/max attributes.  The following
        global attributes are created/updated:
            geospatial_lat_min
            geospatial_lat_max
            geospatial_lon_min
            geospatial_lon_max
            geospatial_bounds
            geospatial_vertical_min
            geospatial_vertical_max
        """

        min_lat = " "
        max_lat = " "
        min_lon = " "
        max_lon = " "
        min_depth = " "
        max_depth = " "
        depth_resolution = " "
        polygon_wkt = u'POLYGON EMPTY'

        lon_sensor_def = self.sensor_def_exists('llat_longitude')
        lat_sensor_def = self.sensor_def_exists('llat_latitude')
        if not lon_sensor_def or not lat_sensor_def:
            logging.warning('Failed to set geospatial global attributes')
        else:

            lat_var_name = lat_sensor_def['nc_var_name']
            lon_var_name = lon_sensor_def['nc_var_name']

            if (lat_var_name in self._nc.variables
                    and lon_var_name in self._nc.variables):
                min_lat = self._nc.variables[lat_var_name][:].min()
                max_lat = self._nc.variables[lat_var_name][:].max()
                min_lon = self._nc.variables[lon_var_name][:].min()
                max_lon = self._nc.variables[lon_var_name][:].max()

                # Make sure we have non-Nan for all values
                if not np.any(np.isnan([min_lat, max_lat, min_lon, max_lon])):
                    # Create polygon WKT and set geospatial_bounds
                    coords = ((max_lat, min_lon),
                              (max_lat, max_lon),
                              (min_lat, max_lon),
                              (min_lat, min_lon),
                              (max_lat, min_lon))
                    polygon = Polygon(coords)
                    polygon_wkt = polygon.wkt

        # Set the global attributes
        self.nc.setncattr('geospatial_lat_min', min_lat)
        self.nc.setncattr('geospatial_lat_max', max_lat)
        self.nc.setncattr('geospatial_lon_min', min_lon)
        self.nc.setncattr('geospatial_lon_max', max_lon)
        self.nc.setncattr('geospatial_bounds', polygon_wkt)

        depth_sensor_def = self.sensor_def_exists('llat_depth')
        if not depth_sensor_def:
            logging.warning(
                'Failed to set global geospatial_vertical attributes')
        else:
            depth_var_name = depth_sensor_def['nc_var_name']
            if depth_var_name in self.nc.variables:
                try:
                    min_depth = np.nanmin(self.nc.variables[depth_var_name][:])
                    max_depth = np.nanmax(self.nc.variables[depth_var_name][:])
                    depth_resolution = (
                            (max_depth - min_depth)
                            / self.nc.variables[depth_var_name].size
                    )
                except (TypeError, ValueError) as e:
                    logging.warning('{:s}: {:}'.format(self._out_nc, e))
                    depth_resolution = np.nan

        self._nc.setncattr('geospatial_vertical_min', min_depth)
        self._nc.setncattr('geospatial_vertical_max', max_depth)
        self._nc.setncattr('geospatial_vertical_resolution', depth_resolution)

    def set_trajectory_id(self):
        """ Sets or updates the trajectory dimension and variable for the
        dataset and the global id attribute

        Input:
            - glider: Name of the glider deployed.
            - deployment_date: String or DateTime of when glider was
                first deployed.
        """

        if 'trajectory' not in self._nc.variables:
            # Setup Trajectory Dimension
            self._nc.createDimension('traj_strlen', len(self._trajectory))

            # Setup Trajectory Variable
            trajectory_var = self._nc.createVariable(
                u'trajectory',
                'S1',
                ('traj_strlen',),
                zlib=True,
                complevel=self.compressionLevel
            )

            attrs = {
                'cf_role': 'trajectory_id',
                'long_name': 'Trajectory/Deployment Name',  # NOQA
                'comment': (
                    'A trajectory is a single deployment of a glider and may '
                    'span multiple data files.'
                )  # NOQA
            }
            for key, value in sorted(attrs.items()):
                trajectory_var.setncattr(key, value)
        else:
            trajectory_var = self._nc.variables['trajectory']

        # Set the trajectory variable data
        trajectory_var[:] = stringtoarr(self._trajectory, len(self._trajectory))

        if not self._nc.getncattr('id').strip():
            self._nc.id = self._trajectory  # Global id variable

    def set_datatype(self, sensor, sensor_def):
        """ Create the sensor NetCDF variable using the specified sensor
        definition

        Parameters:
            sensor: the glider sensor name
            sensor_def: glider sensor definition dictionary

        Returns:
            True if the variable was created or None if the variable is not
            created
        """

        # Confirm dimension
        if 'dimension' not in sensor_def:
            logging.warning(
                '{:s} sensor definition has no dimension key'.format(sensor)
            )
            return
        elif not sensor_def['dimension'] or sensor_def['dimension'] is None:
            dimension = ()
        elif type(sensor_def['dimension']) == list:
            dimension = tuple(sensor_def['dimension'])
        else:
            dimension = (sensor_def['dimension'],)

        if 'attrs' not in sensor_def:
            sensor_def['attrs'] = {}

        # Check for user-specified _FillValue or missing_value
        if ('_FillValue' in sensor_def['attrs']
                and sensor_def['attrs']['_FillValue']):
            var_fill_value = sensor_def['attrs']['_FillValue']
        elif ('missing_value' in sensor_def['attrs']
              and sensor_def['attrs']['missing_value']):
            var_fill_value = sensor_def['attrs']['missing_value']
        else:
            try:
                var_fill_value = NC_FILL_VALUES[sensor_def['type']]
            except KeyError:
                logging.error(
                    'Invalid netCDF4 _FillValue type for {:s}: {:s}'.format(
                        sensor, sensor_def['type'])
                )
                return

        try:
            nc_var = self._nc.createVariable(
                sensor_def['nc_var_name'],
                sensor_def['type'],
                dimensions=dimension,
                zlib=True,
                complevel=self.compressionLevel,
                fill_value=var_fill_value
            )
        except (AttributeError, ValueError) as e:
            logging.error(
                'Error in set_datatype for variable {:s}: {:}'.format(sensor, e)
            )
            logging.warning(
                'Dimension for {:s} is {:s}'.format(
                    sensor, self._record_dimension['nc_var_name'])
            )
            return

        # Add attribute to note the variable name used in the source data file
        if ('long_name' not in sensor_def['attrs']
                or not sensor_def['attrs']['long_name'].strip()):
            sensor_def['attrs']['long_name'] = sensor
        for k, v in sorted(sensor_def['attrs'].items()):
            if k.lower() == '_fillvalue' or k.lower() == 'missing_value':
                continue
            nc_var.setncattr(k, v)

        return sensor_def

    def set_title(self, title_string):
        """Set the NetCDF global title attribute to title_string"""
        self._nc.title = title_string

    def set_source_file_var(self, source_file_string, attrs=None):
        """ Sets the trajectory dimension and variable for the dataset and the
        global id attribute

        Input:
            - glider: Name of the glider deployed.
            - deployment_date: String or DateTime of when glider was
                first deployed.
        """

        if 'source_file' not in self._nc.variables:
            # Setup Trajectory Dimension
            self._nc.createDimension(
                'source_file_strlen', len(source_file_string))

            # Setup Trajectory Variable
            source_file_var = self._nc.createVariable(
                u'source_file',
                'S1',
                ('source_file_strlen',),
                zlib=True,
                complevel=self.compressionLevel
            )

            if attrs:
                attrs['long_name'] = 'Source data file'
                attrs['comment'] = (
                    'Name of the source data file and associated file metadata'
                )
                for key, value in sorted(attrs.items()):
                    source_file_var.setncattr(key, value)
        else:
            source_file_var = self._nc.variables['source_file']

        # Set the trajectory variable data
        source_file_var[:] = stringtoarr(
            source_file_string, len(source_file_string))

        if not self._nc.getncattr('source').strip():
            self._nc.source = (
                'Observational Slocum glider data from source dba file '
                '{:s}'.format(source_file_string)  # Global source variable
            )

    def update_sensor_def(self, sensor, new_def, override=False):
        """Adds missing key/value pairs in the nc._nc_sensor_defs['sensor'][
        'attrs'] sensor definition with the key,value pairs in new_def[
        'attrs'].  The only exception is the 'units' attribute.  The value
        for this attribute is left as specified in the sensor_defs.json file
        to allow for proper UDUNITS.
        """

        if sensor not in self.sensors:
            self.sensors[sensor] = new_def
            return

        if 'attrs' not in new_def:
            return

        if 'attrs' not in self.sensors[sensor]:
            logging.warning(
                'Creating sensor attributes dictionary: {:s}'.format(sensor)
            )
            self.sensors[sensor]['attrs'] = {}

        for k, v in new_def['attrs'].items():
            if k in self.sensors:
                continue

            if (k in self.sensors[sensor]['attrs'] and
                    self.sensors[sensor]['attrs'][k]):
                if not override:
                    continue
                else:
                    logging.debug(
                        'Replacing existing {:s} variable attribute: '
                        '{:s}'.format(sensor, k)
                    )

            self.sensors[sensor]['attrs'][k] = v

    def update_data_file_sensor_defs(self, sensor_defs, override=False):
        """Update the NetCDF sensor definitions with any additional
        attributes created from parsing the raw data file.  If override is
        set to True, existing attributes from self.nc_sensor_defs[sensor] are
        replaced with the corresponding attribute from the raw data file
        reader variable attributes.
        """

        # Add the derived sensor definitions
        #  OOI update 2019-04-22 to use sensor_defs dict instead of list
        for sensor in sensor_defs:
            if sensor not in self.sensors:
                continue

            self.update_sensor_def(sensor, sensor_defs[sensor],
                                   override=override)

    def set_container_variables(self):

        if not self._nc:
            logging.warning(
                'NetCDF file must be initialized before adding container '
                'variables')
            return

        container_variables = [
            sensor for sensor in self.sensors.keys()
            if 'dimension' in self.sensors[sensor]
               and not self.sensors[sensor]['dimension']
        ]
        for container_variable in container_variables:
            if 'nc_var_name' not in self.sensors[container_variable]:
                logging.warning(
                    '{:s} sensor definition does not contain an nc_var_name '
                    'key'.format(container_variable)
                )
                continue

            nc_var_name = self.sensors[
                container_variable]['nc_var_name']

            if nc_var_name in self._nc.variables:
                continue
            elif 'attrs' not in self.sensors[container_variable]:
                continue

            self.set_scalar(container_variable)
            for k, v in sorted(
                    self.sensors[container_variable]['attrs'].items()):
                if k.lower() == '_fillvalue' or k.lower() == 'missing_value':
                    continue
                self._nc.variables[nc_var_name].setncattr(k, v)

    def insert_var_data(self, var_name, var_data):

        datatype = self.check_datatype_exists(var_name)

        if not datatype:
            logging.debug(
                'NetCDF variable {:s} not created'.format(var_name))
            return

        # Add the variable data
        try:
            self._nc.variables[datatype['nc_var_name']][:] = var_data
        except TypeError as e:
            logging.error('NetCDF variable {:s}: {:}'.format(var_name, e))
            return

        return True

    @staticmethod
    def delta_to_iso_duration(timeobj):
        """Convert a duration in number of seconds `timeobj` and return an
        ISO 8601:2004 duration formatted string
        E.g. P1DT2H17M32.54S = 1 day, 2 Hours, 17 Minutes and 32.54 Seconds

        :param : timeobj, a duration in seconds as either a
            datetime.timedelta object, or a scalar number (int or float).

        """

        if isinstance(timeobj, datetime.timedelta):
            seconds = timeobj.total_seconds()
        elif isinstance(timeobj, (int, float)):
            # make sure it is a float for a later conditional
            seconds = float(timeobj)
        else:
            return
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        days, hours, minutes = map(int, (days, hours, minutes))
        seconds = round(seconds, 6)

        # build date
        date = ''
        if days:
            date = '%sD' % days

        # build time
        time = u'T'
        # hours
        bigger_exists = date or hours
        if bigger_exists:
            time += '{:02}H'.format(hours)
        # minutes
        bigger_exists = bigger_exists or minutes
        if bigger_exists:
            time += '{:02}M'.format(minutes)
        # seconds
        if seconds.is_integer():
            seconds = '{:02}'.format(int(seconds))
        else:
            # 9 chars long w/leading 0, 6 digits after decimal
            seconds = '%09.6f' % seconds
        # remove trailing zeros
        seconds = seconds.rstrip('0')
        time += '{}S'.format(seconds)
        return u'P' + date + time

