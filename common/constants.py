import legacy.gliderdac.ooidac.constants as lgoc
import FileReader.AuvReader.remusConstants as farc

LOG_HEADER_FORMAT = ('%(levelname)s:%(module)s: [line %(lineno)d]'
                     '   %(message)s')

LOG_PROCESSING_FORMAT = ('    %(levelname)s:%(module)s: [line %(lineno)d]'
                         '        %(message)s')

SLOCUM_20_PLATFORM = 'Slocum Glider 2.0'
REMUS_600_PLATFORM = 'Remus 600 AUV'
SUPPORTED_PLATFORMS = [ SLOCUM_20_PLATFORM, REMUS_600_PLATFORM ]

IOOS_DAC_TARGET = 'IOOS-DAC'
OOI_EXPLORER_TARGET = 'OOI-EXPLORER'
OUTPUT_TARGETS = [ IOOS_DAC_TARGET, OOI_EXPLORER_TARGET ]

OUTPUT_FORMATS = lgoc.NETCDF_FORMATS

INPUT_FORMATS = lgoc.SLOCUM_DELAYED_MODE_EXTENSIONS + \
                lgoc.SLOCUM_REALTIME_MODE_EXTENSIONS + \
                farc.REMUS_DATA_FILE_EXTENSIONS

OCEAN_DEPTH_M = 11034.0
