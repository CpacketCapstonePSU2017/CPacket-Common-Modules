import unittest
import sys
sys.path.append("../dist_functions/")
import stats

class DistFunctionsTestCase(unittest.TestCase):

    def setUp(self):
        self.test = stats(100)

class DistArraySizeTestCase(DistFunctionsTestCase):

    def runTest(self):
        self.assertEqual(len(self.test.Dist_Array),100)

# runs the unit test
if __name__ == '__main__':
    unittest.main();
