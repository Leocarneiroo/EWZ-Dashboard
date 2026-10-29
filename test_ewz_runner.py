import unittest
from unittest.mock import patch

from ewz_runner import confirm_publish


class ConfirmPublishTests(unittest.TestCase):
    def test_only_explicit_yes_confirms_publish(self):
        with patch("builtins.input", return_value="s"):
            self.assertTrue(confirm_publish())

        with patch("builtins.input", return_value=""):
            self.assertFalse(confirm_publish())


if __name__ == "__main__":
    unittest.main()
