"""
class: dataExplorerNetCDFWriter

description: Output file writer/formatter for OOI-Data Explorer compatible
NetCDF format output files (ie: NetCDF syntax, OOI-Data Explorer content)

history:
09/21/2021 ppw created
"""
from FileWriter.NetCDFWriter.netCDFWriter import netCDFWriter


class dataExplorerNetCDFWriter( netCDFWriter ) :

    def __init__( self ) :
        super().__init__()
