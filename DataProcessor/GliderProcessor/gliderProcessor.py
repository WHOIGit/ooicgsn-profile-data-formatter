"""
class: gliderProcessor

description:

history:
09/21/2021 ppw created
"""
from DataProcessor.dataProcessor import dataProcessor


class gliderProcessor( dataProcessor ) :

    def __init__( self ) :
        super().__init__()
        # TODO

    # abstract virtual function
    def processData(self, theData, invariantData, varsToCalculate):
        raise  NotImplementedError