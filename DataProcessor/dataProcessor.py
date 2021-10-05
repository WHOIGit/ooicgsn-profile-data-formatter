"""
class: dataProcessor

description:

history:
09/21/2021 ppw created
"""

class dataProcessor( ) :

    def __init__( self ) :

        # TBD
        pass

    # abstract virtual function
    def processData(self, theData, invariantData, varsToCalculate):
        raise NotImplementedError