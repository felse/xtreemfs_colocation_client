import unittest
import random

from xtreemfs_client import osd


class TestOsd(unittest.TestCase):
    def test_get_smallest_folder(self):
        test_folders = [("folder_1", 1), ("folder_2", 2), ("folder_3", 3), ("folder_4", 4)]
        for i in range(0, 10):
            test_osd = osd.OSD("osd_uuid")
            random.shuffle(test_folders)
            for folder_id, folder_size in test_folders:
                test_osd.add_folder(folder_id, folder_size)
            self.assertEqual(("folder_1", 1), test_osd.get_smallest_folder())
