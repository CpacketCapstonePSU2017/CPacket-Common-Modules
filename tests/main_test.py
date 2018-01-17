from unittest import TestCase


# Just for making Travis trigger tests
class TestSample(TestCase):
    def test_add(self):
        self.assertEqual(9, 4+5)
