"""
    A simple function for Distributions
    The byte count is accessed from ._ByteCount for each new object created.
"""

import numpy as np

class stats:
    _FuncType = None
    _ByteCount = 0
    _DistArray = None
    _Time = 0
    _Max = 0
    _Min = 0
    _Shape = None

    def __init__(self, Max, Time, Min=0, Shape=None,Type=None):
        '''
            Creates the specified distribution into an Array
            Then grabs a random value from the array as the byte count
            :param: Min: Should almost always be 0 as we don't use negative values
            :param: Max: Can change varying on function. Should usually be infinity.
            :param: Time: This is somehow factored into the distribution.
            :param: Shape: The shape of the distribution.
            :param: Type: The distribution function to use. For now 'None' will default to Poisson.
        '''
        
        # Redundant but helpful for long term development
        _Min = Min
        _Max = Max
        _Time = Time
        _Shape = Shape
        _Type = Type

        if _Type == None:
            #Set max to Infinity
            # This doesn't work as a Lam value.
            _Max = float('inf')

            #TODO: Implement.
            """
            random.poisson() takes the shape of two Params Lam and Size.
            Min, Max, and Time need to be arranged in the specified shape.
            """
            # _DistArray = np.random.poisson(lam=(???),size=(25))
            
            # Dummy for testing. Max needs to be Inf but it can't be Lamda
            _DistArray = np.random.poisson(lam=(1000), size=(25))

            # Grab random index in distribution as byte count
            _ByteCount = _DistArray[np.random.randint(0,25)]

    
    # Used for getting a second byte count later in development
    def NewByteCount(self):
        # Dummy for testing to have index changed properly
        _ByteCount = _DistArray[np.random.randint(0,25)]
        return _ByteCount
