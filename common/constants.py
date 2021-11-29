import legacy.gliderdac.ooidac.constants as lgoc
import FileReader.AuvReader.remusConstants as farc

LOG_HEADER_FORMAT = ('%(levelname)s:%(module)s: [line %(lineno)d]'
                     '\n%(message)s')

LOG_PROCESSING_FORMAT = ('    %(levelname)s:%(module)s: [line %(lineno)d]'
                         '\n        %(message)s')

SUPPORTED_PLATFORMS = [ 'Slocum Glider 2.0', 'Remus 600 AUV' ]

OUTPUT_TARGETS = ['IOOS-DAC',
                  'OOI-EXPLORER']

OUTPUT_FORMATS = lgoc.NETCDF_FORMATS

INPUT_FORMATS = lgoc.SLOCUM_DELAYED_MODE_EXTENSIONS + \
                lgoc.SLOCUM_REALTIME_MODE_EXTENSIONS + \
                farc.REMUS_DATA_FILE_EXTENSIONS

OCEAN_DEPTH_M = 11034.0
