import unittest
import sys
sys.path.append("../dist_functions/")
import stats

class DistFunctionsTestCase(unittest.TestCase):

    def setUp(self):
        self.test = stats.stats(100)

class DistArraySizeTestCase(DistFunctionsTestCase):

    def runTest(self):
        self.assertEqual(len(self.test.Dist_Array),100)

class DistArrayMaxTestCase(DistFunctionsTestCase):

    def runTest(self):
        for x in self.test.Dist_Array:
            self.assertTrue(x <= 1.25 * 10000000)

# runs the unit test
if __name__ == '__main__':
    unittest.main()
