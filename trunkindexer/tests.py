import unittest

from trunkindexer.cli import main


class TestTrunkIndexer(unittest.TestCase):
    def test_cli(self):
        """A sample test..."""

        self.assertTrue(main())
