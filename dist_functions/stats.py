"""
    A simple function for Distributions
    The byte count is accessed from ._ByteCount for each new object created.
"""

import numpy as np


class Stats:

    def __init__(self, size, maximum=None, minimum=0, shape=None, functype=None):
        """
            Creates the specified distribution into an Array
            :param: Min: Should almost always be 0 as we don't use negative values
            :param: Max: Can change varying on function. Should usually be infinity.
            :param: Size: How many elements to generate
            :param: Shape: The shape of the distribution.
            :param: Type: The distribution function to use. For now 'None' will default to Poisson.
        """
        
        # Redundant
        self._Min = minimum
        self._Max = maximum
        self._Size = size
        self._Shape = shape
        self._Func_Type = functype
        # public
        self.Dist_Array = None
        
        # Default function type will be Poisson.
        if self._Func_Type is None:
            if self._Max is None:
                # TODO: Could be moved to non-hardcoded value.
                self._Max = 1.25 * 10000000

            lamda = self._Max - (self._Max / 4)

            # TODO: error check Max and Size
            self.Dist_Array = np.random.poisson(lam=lamda, size=self._Size)
            for x in self.Dist_Array:
                if x > self._Max:
                    # print(x)
                    x = self._Max
