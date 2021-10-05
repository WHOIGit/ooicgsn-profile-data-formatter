"""
class: fileReader

description:

history:
09/21/2021 ppw created
"""


class fileReader():

    def __init__(self):
        self._fid = None

    # read file, return as dictionary
    # abstract, implement in subclass
    def readDictionary(self, filePath):
        raise NotImplementedError()

    # read file, return as list of strings (no eol)
    # abstract, implement in subclass
    def readLines(self, filePath):
        raise NotImplementedError()

    # open file for reading piecemeal
    # abstract, implement in subclass
    def open(self, filePath):
        raise NotImplementedError()

    # read line, return as string (no eol)
    # abstract, implement in subclass
    def readLine(self):
        raise NotImplementedError()

    # read binary, return as byte[]
    # abstract, implement in subclass
    def readBytes(self, maxBytes):
        raise NotImplementedError()

    # abstract, implement in subclass
    def close(self):
        raise NotImplementedError()
