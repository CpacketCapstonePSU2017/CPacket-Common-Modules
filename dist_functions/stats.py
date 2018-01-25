"""
    A simple function for Distributions
    The byte count is accessed from ._ByteCount for each new object created.
"""

import numpy as np

class stats:

    def __init__(self,Size,Max=None,Min=0,Shape=None,Type=None):
        '''
            Creates the specified distribution into an Array
            :param: Min: Should almost always be 0 as we don't use negative values
            :param: Max: Can change varying on function. Should usually be infinity.
            :param: Size: How many elements to generate
            :param: Shape: The shape of the distribution.
            :param: Type: The distribution function to use. For now 'None' will default to Poisson.
        '''
        
        # Redundantance
        self._Min = Min
        self._Max = Max
        self._Size = Size
        #self._Shape = Shape
        self._Func_Type = Type
        # public
        self.Dist_Array = None
        
        # Default function type will be Poisson.
        if self._Func_Type == None:
            if self._Max == None: 
                # TODO: Could be moved to non-hardcoded value.
                self._Max = 1.25 * 10000000
                Lamda = self._Max - (self._Max / 4)
            
            # TODO: error check Max and Size
            self.Dist_Array = np.random.poisson(lam=(self._Max), size=(self._Size))
            for x in self.Dist_Array:
                if x > self._Max:
                    #print x
                    x = self._Max
    
    # Used for getting a second byte count later in development
    def NewByteCount(self):
        # Dummy for testing to have index changed properly
        return self._DistArray[np.random.randint(0,self.Size)]
