import unittest
from wvlet.runner import WvletCompiler

class TestWvletRunner(unittest.TestCase):

    def test_wvlet_invalid_path(self):
        with self.assertRaises(ValueError, msg="Invalid executable_path: invalid"):
            WvletCompiler(executable_path="invald")

    def test_wvlet_not_found(self):
        with self.assertRaises(NotImplementedError, msg="This binding currently requires wvc executable"):
            WvletCompiler()
