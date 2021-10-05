"""
class: slocum20Processor

description: Slocum 2.0 Glider specific data processor class
Data processors perform all computations of derived sensor data and any
required updates to measured sensor data prior to formatting for output

history:
09/21/2021 ppw created
"""
import logging

from DataProcessor.GliderProcessor.gliderProcessor import gliderProcessor
import legacy.gliderdac.ooidac.processing as processing
from legacy.gliderdac.ooidac.data_checks import check_file_goodness
from legacy.gliderdac.ooidac.constants import SCI_CTD_SENSORS
from legacy.gliderdac.ooidac.profiles import Profiles


class slocum20Processor( gliderProcessor ) :
    """
    Slocum 2.0 specific Data Processor class
    Performs all calculations and computations of new
    and updated data values
    """

    def __init__( self ) :
        super().__init__()

        self._cfgSensorDefs = {}
        self._dataFiles = []
        self._dataFile = None

    @property
    def cfgSensorDefs(self):
        return self._cfgSensorDefs

    @cfgSensorDefs.setter
    def cfgSensorDefs(self, defs):
        self._cfgSensorDefs = defs

    @property
    def dataFiles(self):
        return self._dataFiles

    @dataFiles.setter
    def dataFiles(self, dataFiles):
        self._dataFiles = dataFiles

    @property
    def dataFile(self):
        return self._dataFile

    @dataFile.setter
    def dataFile(self, dataFile):
        self._dataFile = dataFile

    # virtual function, implemented here
    def processData(self, dba, scalars, varsToCalculate):
        """
        Perform all calculations do Slocum 2.0 data here
        :param dba: Slocum 2.0 DbaData object
        :param scalars: list of scalar variables to update
        :param varsToCalculate: list of calculation inputs per variable
        :return: Profiles (updated dba data broken into profiles
        """

        # check the data file for the required sensors and that science data
        # exists in the file.  True or False returned.  See data_checks.py
        file_check = check_file_goodness(dba)
        if not file_check.file_good or len(dba.underwater_indices) == 0:
            logging.warning(
                'File {:s} either does not have enough science data, lacks '
                'the required sensors, or does not have any dives deep enough '
                'to produce DAC formatted profiles'.format(dba.source_file)
            )
            return None

        # remove any sci bay initialization zeros that may occur (where all
        # science instrument sensors are 0.0)
        dba = processing.remove_sci_init_zeros(
            dba, file_check.avail_sci_data)

        # for the rare instance where an instrument has been removed from
        # proglets.dat, the variables/sensors associated are removed from the
        # segment data file.  If it is missing and is in the DATA_CONFIG_LIST in
        # the gdac configuration file, then we add it back in here as an
        # array of NaNs.
        dba = processing.replace_missing_sensors(
            dba, file_check.avail_sci_data)

        # This processing step adds time dependent coordinate variables
        # (designated by the prefix llat [lat, lon, altitude, time]) which are
        # created and added to the data instance with metadata attributes.
        # This step is required before the other processing module steps.  The
        # variable llat_time is derived from `m_present_time`,
        # llat_latitude/longitude are filled by linear interpolation from
        # `m_gps_lat/lon`, llat_pressure is derived from converting
        # `sci_water_pressure` to dbar, and llat_depth is derived from
        # `llat_pressure` converted to depth using the Python TEOS-10 GSW
        # package
        dba = processing.create_llat_sensors(dba)
        if dba is None:
            return None

        # Convert m_pitch and m_roll variables to degrees, and add back to
        # the data instance with metadata attributes
        if 'm_pitch' in dba.sensor_names and 'm_roll' in dba.sensor_names:
            dba = processing.pitch_and_roll(dba)
            if dba is None:
                return None

        # Convert `sci_water_cond/temp/ & pressure` to `salinity` and `density`
        # and adds them back to the data instance with metadata attributes.
        # Requires `llat_latitude/longitude` variables are in the data
        # instance from the `create_llat_sensors` method
        dba = processing.ctd_data(dba, SCI_CTD_SENSORS)
        if dba is None:
            return None

        # Process `sci_oxy4_oxygen` to OOI L2 compensated for salinity and
        # pressure and converted to umol/kg.
        if 'corrected_oxygen' in self.cfgSensorDefs:
            dba = processing.check_and_recalc_o2(
                dba,
                calc_type= varsToCalculate['corrected_oxygen'][
                    'calculation_type'],
                cal_dict= varsToCalculate['corrected_oxygen']['cal_coefs']
            )
            dba = processing.o2_s_and_p_comp(dba, 'temp_corrected_oxygen')
            oxy = dba['oxygen']
            oxy['sensor_name'] = 'corrected_oxygen'
            dba['corrected_oxygen'] = oxy
        elif 'sci_oxy4_oxygen' in dba.sensor_names:
            dba = processing.o2_s_and_p_comp(dba)
            if dba is None:
                return None

        # Re_calculate chlorophyll
        if 'corrected_chlor' in self.cfgSensorDefs:
            dba = processing.recalc_chlor(
                dba,
                self.cfgSensorDefs['corrected_chlor']['attrs']['dark_offset'],
                self.cfgSensorDefs['corrected_chlor']['attrs']['scale_factor']
                # **varsToCalculate['corrected_chlor']
                # dark_offset=corrections['corrected_chlor']['dark_offset'],
                # scale_factor=corrections['corrected_chlor']['scale_factor']
            )
            if dba is None:
                return None

        # Re_calculate PAR
        if 'corrected_par' in self.cfgSensorDefs:
            # par_sensor_dark = corrections['corrected_par']['sensor_dark']
            # par_sf = corrections['corrected_par']['scale_factor']
            dba = processing.recalc_par(
                dba, **varsToCalculate['corrected_par']
                # sensor_dark=par_sensor_dark,
                # scale_factor=par_sf
            )
            if dba is None:
                return None

        bksctr_vars = [x for x in self.cfgSensorDefs if 'backscatter' in x]
        for var_name in bksctr_vars:
            if var_name in varsToCalculate:
                bksctr_args = varsToCalculate[var_name]
                assert "source_sensor" in bksctr_args, (
                    'In the "processing" dictionary for variable "{:s}" in '
                    'sensor_defs.json, a "source_sensor" field must be present '
                    'with the glider sensor name to use for '
                    'processing.'.format(var_name)
                )
                bb_sensor = bksctr_args.pop('source_sensor')

            else:
                # for now assume we are using flbbcds with 700 nm wavelength as
                # the default, the input values to the backscatter_total
                # function also default to the flbbcd values.
                bksctr_args = {}
                wavelength = 700.0
                bb_sensor = 'sci_flbbcd_bb_units'

            dba = processing.backscatter_total(
                dba, bb_sensor, var_name, **bksctr_args)
            if dba is None:
                return None

        # Add radiation wavelength variables as a paired variable to
        # the backscatter variables
        # if wavelength is not present, assume FLBB 700 nm
        # Note: this will likely change in the future to only include
        #   'radiation_wavelength' as an attribute to the backscatter variable.
        #   It is too much to have a variable for each if there are multiple
        #   backscatter variables.  Although the future release will control if
        #   you want it to be a variable or not just by the processing
        #   dictionary
        rw_vars = [x for x in self.cfgSensorDefs
                   if 'radiation_wavelength' in x]
        for rw_var in rw_vars:
            sdef = self.cfgSensorDefs[rw_var]
            if 'radiation_wavelength' in sdef['attrs']:
                wl = sdef['attrs']['radiation_wavelength']
            else:
                wl = 700.0
            radiation_wavelength = {
                'data': wl,
                'attrs': {'units': 'nm'},
                'nc_var_name': rw_var}
            scalars.append(radiation_wavelength)

        # If Depth Averaged Velocity (DAV) data available, (i.e. any of the
        # `*_water_vx/vy` sensors are in the data) get the values and calculate
        # the mean position and time of the segment as the postion and time
        # for the velocity estimation
        if file_check.dav_sensors:
            # get segment mean time, segment mean lat, and segment mean lon
            # (underwater portion only)
            seg_time, seg_lat, seg_lon = processing.get_segment_time_and_pos(
                dba)
            if seg_time is None or seg_lat is None or seg_lon is None:
                return None

            # if data is the recovered data, this tries to get
            # `m_final_water_vx/vy` from the next 2 segment data files where
            # the calculation occurs, if it cannot, it will get either
            # `m_initial_water_vx/vy` or `m_water_vx/vy` from the current
            # segement file.
            dba_index = self.dataFiles.index(self.dataFile)
            next2files = self.dataFiles[dba_index + 1:dba_index + 3]
            vx, vy = processing.get_u_and_v(dba, check_files=next2files)

            scalars.extend([seg_time, seg_lat, seg_lon, vx, vy])

        # The Profiles class discovers the profiles and filters them based on
        # user configurable filters written in profile_filters.py (Instructions
        # found in the module) and returns each profile as a new GliderData
        # instance that is the data subset of the main data instance
        profiles = Profiles(dba)

        profiles.find_profiles_by_depth()

        # See profile_filters.py for which filters are applied
        profiles.filter_profiles()

        if len(profiles.indices) == 0:
            logging.info('No profiles indexed: {:s}'.format(self.dataFile))
            return None

        return profiles

    def reduceProfileToScienceData(self, profile ):
        """
        Invoke legacy code for Slocum 2.0
        :param profile:
        :return:
        """

        return processing.reduce_to_sci_data( profile )