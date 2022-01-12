"""
class: profileDataFormatter.py
description: Main module for app that takes oceanographic instrument data from
vertical profiling platforms (gliders, auvs) and formats the data into netCSV
format files for import into data repositories, such as IOOS-DAC and OOI Data
Explorer.
history:
09/21/2021 ppw created
"""
import os
import sys
import logging
import argparse
import json
import glob
from common.constants import SUPPORTED_PLATFORMS, OUTPUT_TARGETS, OUTPUT_FORMATS
from common.constants import LOG_HEADER_FORMAT
import MobilePlatform.GliderPlatform.slocum20Platform as slocum20
import MobilePlatform.AuvPlatform.remus600Platform as remus600


def validateCommonArguments( args ) :
    """
    Error check command-line arguments not requiring platform specific validation
    :param args - Namespace output from argparser
    :return 0 - success, -1 validation error, see logfile
    """
    ret = 0   # arguments all valid

    # Config and output paths must exist

    if not os.path.isdir( args.config_path ):
        logging.error( "Configuration path must be a valid path" )
        ret = -1

    if not os.path.isdir( args.output_path ):
        logging.error( "Output path must be a valid path" )
        ret = -1

    # Platform must be in supported list

    if args.mobile_platform not in SUPPORTED_PLATFORMS:
        logging.error( "Unsupported mobile platform passed")
        ret = -1

    # Host target for NetCDF output must be in supported list

    if args.target_repository not in OUTPUT_TARGETS:
        logging.error( "Unsupported host target passed")
        ret = -1

    # Output format must be in supported formats

    if args.nc_format not in OUTPUT_FORMATS:
        logging.error( "Unsupported output format passed")
        ret = -1

    return ret


def dataFilelistWildcardExpansion( args ) :
    """
    Support glob expansion of file list arguments containing wildcards
    :param args: Namespace, output from argparse
    :return:
    """

    # Support wildcards in data file list, expand to list of files

    if len( args.data_files ) == 1:
        args.data_files = glob.glob( args.data_files[0] )



def platformArgsStringToDict( argString ) :
    """
    Convert platform specific argument list from Json dictionary string to dictionary
    :param argString: platform specific arguments as Json dictionary string
    :return: dictionary
    """

    # Converts a string in json dictionary format to dictionary

    argsDict = {}
    if argString is not None:
        argsDict = json.loads( argString )
    return argsDict


def nameToPlatform( platformName ) :
    """
    Instantiate platform instance corresponding to passed name
    :param platformName: name of platform see common/constants.py
    :return: instance derived from MobilePlatform
    """

    platform = None

    # Instantiate the appropriate subclass of MobilePlatform
    if platformName == 'Slocum Glider 2.0':
        platform = slocum20.slocum20Platform()

    elif platformName == 'Remus 600 AUV':
        platform = remus600.remus600Platform()

    return platform


def main( args ) :
    """
    Main processing entry point for profileDataFormatter
    :param args: Namespace, from argparse
    :return: 0: success, -1 processing failure
    """
    ret = 0

    try:
    
        # Set up logging
        logging.basicConfig(filename='./profileDataFormatter.log', level= getattr( logging, args.log_level.upper() ))
        formatter = logging.Formatter( LOG_HEADER_FORMAT )
        logging.getLogger().handlers[0].setFormatter( formatter )
        logging.info('----- starting profileDataFormatter -----')
        logargs = ' '.join(str(k)+":"+str(v) for k, v in vars( args ).items())
        logging.info( logargs )

        # validate parameters
        if validateCommonArguments( args ) != 0:
            ret =  -1

        else:
            # if data file list contains wildcard(s), expand to list all files
            dataFilelistWildcardExpansion( args )
            if args.data_files is None or len(args.data_files) == 0:
                logging.error("No valid data files found to process")
                ret = -1

            else:

                # Instantiate the desired mobile platform class to process data
                platform = nameToPlatform( args.mobile_platform )

                # Insert cmdline args into platform settings
                platform.cfgPath = args.config_path
                platform.dataFiles = args.data_files
                platform.targetHost = args.target_repository
                if args.platform_args is not None:
                    cleanString = args.platform_args.replace('\'', "\"")
                    platform.platformArgs = platformArgsStringToDict( cleanString )
                platform.outputPath = args.output_path
                platform.replaceOutputFiles = args.clobber
                platform.outputFormat = args.nc_format
                platform.outputCompression = args.compression_level
                platform.suppressOutput = args.suppress_output

                # Allow platform to further validate arguments
                if platform.validateSettings() != 0:
                    ret =  -1

                else:

                    # Perform pre-formatting tasks
                    platform.setupFormatting()

                    # Have platform perform data formatting
                    ret = platform.FormatData()

                    # Perform post-formatting cleanup tasks
                    platform.cleanupFormatting()

    except Exception as e:
        logging.error( "Uncaught exception: " + str(e))
        ret = -1

    if ret == 0:
        print('formatting completed successfully')
    else:
        print('Errors encountered, see log file for details')

    return ret


if __name__ == "__main__" :
    """
    profileDataFormatter entry point
    """

    arg_parser = argparse.ArgumentParser(
        description=str( __doc__ ),
        formatter_class = argparse.ArgumentDefaultsHelpFormatter
    )

    # more TBD

    arg_parser.add_argument('-c', '--config_path',
                            help='Location of deployment configuration files',
                            required=True )

    arg_parser.add_argument('-d', '--data_files',
                            help='Platform data files to process',
                            nargs='+')

    arg_parser.add_argument('-m', '--mobile_platform',
                            help='Mobile platform that is source of input data',
                            choices=SUPPORTED_PLATFORMS,
                            default='Slocum Glider 2.0'
                            )

    arg_parser.add_argument('-t', '--target_repository',
                            help='Target host repository type for formatted data',
                            choices=OUTPUT_TARGETS,
                            default='IOOS-DAC')

    arg_parser.add_argument('-p', '--platform_args',
                            help=('Platform specific arguments formatted as '
                                  'json dictionary string ie: "{ctd_sensor_prefix" : '
                                  '"sci", "start_profile_id" : 15}'))

    arg_parser.add_argument('-o', '--output_path',
                            help=(
                                'NetCDF destination directory, which must '
                                'exist. Current directory if not specified'),
                            default='.' )

    arg_parser.add_argument('-k', '--clobber',
                            help='Clobber existing output files if they exist',
                            action='store_true')

    arg_parser.add_argument('-f', '--format',
                            dest='nc_format',
                            help='NetCDF file format',
                            choices=OUTPUT_FORMATS,
                            default='NETCDF4_CLASSIC')

    arg_parser.add_argument('-cl', '--compression_level',
                            help='NetCDF4 compression level',
                            choices=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                            default=1)

    arg_parser.add_argument('-s', '--suppress_output',
                            help=(
                                'Check configuration and create output file '
                                'writer, but does not process any files'),
                            action='store_true')

    arg_parser.add_argument('-l', '--log_level',
                            help='Verbosity level',
                            type=str,
                            choices=[
                                'debug', 'info', 'warning',
                                'error', 'critical'],
                            default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit( main( parsed_args ))
