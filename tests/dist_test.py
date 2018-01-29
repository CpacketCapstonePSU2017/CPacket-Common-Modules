import dist_functions.stats
from unittest import TestCase


class TesterStats(TestCase):

    def setUp(self):
        self.test = stats.Stats(100)

    def tearDown(self):
        self.test = None

    def test_DistArraySize(self):
        # print("Array Size:")
        # print(self.test.Dist_Array)
        self.assertEqual(len(self.test.Dist_Array), 100, 'Array not correct size')

    def test_DistArrayMax(self):
        # print("Array Max:")
        # print(self.test.Dist_Array)
        for x in self.test.Dist_Array:
            self.assertLessEqual(x, 1.25*10000000, 'Value larger than Max')
