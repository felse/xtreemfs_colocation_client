import subprocess
import unittest
import os
import shutil

from xtreemfs_client import OSDManager
from xtreemfs_client import div_util
from xtreemfs_client import verify


class TestDivUtil(unittest.TestCase):
    def test_remove_leading_trailing_slashes(self):
        s = '/hello/world////'
        self.assertEqual(div_util.remove_leading_trailing_slashes(s), 'hello/world')

        s = '/just_one_tile'
        self.assertEqual(div_util.remove_leading_trailing_slashes(s), 'just_one_tile')


class TestOSDManager(unittest.TestCase):
    def setUp(self):
        # fields for unit testing without an xtreemfs instance
        self.volume_name = 'volume'
        self.path_to_mount = '/mnt/x_mnt'
        self.path_on_volume = 'one/two/three'

        self.some_folder = 'some/folder'
        self.some_sub_folder = 'subfolder'

        self.absolute_path_to_managed_folder = os.path.join(self.path_to_mount, self.path_on_volume)

        self.absolute_path_random = '/la/la/la/la/la/la/la'

        self.value_map = {'path_on_volume': self.path_on_volume, 'path_to_mount': self.path_to_mount,
                          'volume_name': self.volume_name, 'osd_selection_policy': None, 'data_distribution': None}

    def test_init_no_xtreemfs_volume(self):
        dummy_path = '/'
        with self.assertRaises(OSDManager.NotAXtreemFSVolume):
            OSDManager.OSDManager(dummy_path)

    def test_path_on_volume(self):
        osd_man = OSDManager.OSDManager(self.absolute_path_to_managed_folder, value_map=self.value_map)

        self.assertEqual(os.path.join(self.volume_name, self.path_on_volume, self.some_folder),
                         osd_man.get_path_on_volume(
                             os.path.join(self.path_to_mount, self.path_on_volume, self.some_folder)))

        with self.assertRaises(OSDManager.PathNotManagedException):
            osd_man.get_path_on_volume(self.absolute_path_random)

    def test_get_target_dir(self):
        osd_man = OSDManager.OSDManager(self.absolute_path_to_managed_folder, value_map=self.value_map)

        folder_id = os.path.join(self.volume_name, self.path_on_volume, self.some_folder, self.some_sub_folder)
        target_dir = osd_man.get_target_dir(folder_id)
        expected_target_dir = os.path.join(self.path_to_mount, self.path_on_volume, self.some_folder)

        self.assertEqual(expected_target_dir, target_dir)


class TestOSDManagerWithXtreemFS(unittest.TestCase):
    def setUp(self):
        # fields for unit testing with a running xtreemfs instance and real files
        # two mount points of the same xtreemfs volume
        self.mount_point_1 = '/dev/shm/xfs_mnt_1'
        self.mount_point_2 = '/dev/shm/xfs_mnt_2'

        self.mount_point_1 = '/home/felix/git/file_placement2/experiments/scripts/local/mnt'
        self.mount_point_2 = self.mount_point_1

        self.path_on_mount_point = 'x/y/folder'

        self.tmp_folder = '/tmp/python-1298324809321934'
        self.test_files_folder = 'test_folder'

        self.depth_1_name = 'dir_'
        self.depth_2_name = 'FOLDER_'
        self.depth_3_name = 'img_'
        self.file_name = 'file_'

        self.num_depth_1_dirs = 2
        self.num_depth_2_dirs = 3
        self.num_depth_3_dirs = 2
        self.num_files = 5

        self.depth_1_dirs = []
        self.depth_2_dirs = []
        self.depth_3_dirs = []

        self.file_sizes = {1: 1, 2: 2, 3: 4, 4: 8, 5: 16}
        self.file_size_multiplier = 1024 * 8

        self.create_test_files()

    def tearDown(self):
        clean_up_volume(self.mount_point_1)
        clean_up_folder(os.path.join(self.tmp_folder, self.test_files_folder))

    def create_test_files(self):
        clean_up_volume(self.mount_point_1)
        clean_up_folder(os.path.join(self.tmp_folder, self.test_files_folder))

        for i in range(0, self.num_depth_1_dirs):
            depth_1_dir = os.path.join(self.tmp_folder, self.test_files_folder, self.depth_1_name + str(i))
            self.depth_1_dirs.append(depth_1_dir)
            for j in range(0, self.num_depth_2_dirs):
                depth_2_dir = os.path.join(depth_1_dir, self.depth_2_name + str(j))
                self.depth_2_dirs.append(depth_2_dir)
                for k in range(0, self.num_depth_3_dirs):
                    depth_3_dir = os.path.join(self.tmp_folder, self.test_files_folder,
                                               self.depth_1_name + str(i),
                                               self.depth_2_name + str(j),
                                               self.depth_3_name + str(k))
                    self.depth_3_dirs.append(depth_3_dir)
                    os.makedirs(depth_3_dir)

                    for l in range(0, self.num_files):
                        file = open(os.path.join(depth_3_dir, self.file_name + str(l)), 'w')
                        for m in range(0, self.file_sizes[j + 1] * self.file_size_multiplier):
                            file.write("1")

    def test_copy_folders(self):
        managed_folder = os.path.join(self.mount_point_1, self.path_on_mount_point)
        os.makedirs(managed_folder)

        x_man = OSDManager.OSDManager(managed_folder)
        x_man.copy_folders(self.depth_2_dirs)

        self.assertTrue(verify.verify_gms_folder(managed_folder))

        self.assertEqual(count_folder_and_files(managed_folder),
                         count_folder_and_files(os.path.join(self.tmp_folder, self.test_files_folder)))

    def test_create_empty_folders(self):
        managed_folder = os.path.join(self.mount_point_1, self.path_on_mount_point)
        os.makedirs(managed_folder)

        new_dirs = []
        copy_tuples = []
        for depth_2_dir in self.depth_2_dirs:
            new_dir = os.path.join(os.path.split(os.path.split(depth_2_dir)[0])[1],
                                   os.path.split(depth_2_dir)[1])
            new_dir = os.path.join(managed_folder, new_dir)
            new_dirs.append(new_dir)
            copy_tuples.append((depth_2_dir, new_dir))

        x_man = OSDManager.OSDManager(managed_folder)
        x_man.create_empty_folders(new_dirs)

        # now copy files manually and check whether the data layout is good

        for src, dst in copy_tuples:
            shutil.rmtree(dst, ignore_errors=True)
            # shutil.copytree requires that the target directory does not exist
            shutil.copytree(src, dst)

        self.assertEqual(count_folder_and_files(managed_folder),
                         count_folder_and_files(os.path.join(self.tmp_folder, self.test_files_folder)))
        self.assertTrue(verify.verify_gms_folder(managed_folder))

    def test_update(self):
        managed_folder = os.path.join(self.mount_point_1, self.path_on_mount_point)
        os.makedirs(managed_folder)

        new_dirs = []
        copy_tuples = []
        for depth_2_dir in self.depth_2_dirs:
            new_dir = os.path.join(os.path.split(os.path.split(depth_2_dir)[0])[1],
                                   os.path.split(depth_2_dir)[1])
            new_dir = os.path.join(managed_folder, new_dir)
            new_dirs.append(new_dir)
            copy_tuples.append((depth_2_dir, new_dir))

        x_man = OSDManager.OSDManager(managed_folder)
        x_man.create_empty_folders(new_dirs)

        for src, dst in copy_tuples:
            shutil.rmtree(dst, ignore_errors=True)
            # shutil.copytree requires that the target directory does not exist
            shutil.copytree(src, dst)

        du_source = subprocess.run(["du", "-s", os.path.join(self.tmp_folder, self.test_files_folder)],
                                   stdout=subprocess.PIPE, universal_newlines=True)
        size_source = int(str(du_source.stdout).split()[0])

        total_size_in_distribution = 0
        for osd in x_man.distribution.OSDs.values():
            total_size_in_distribution += osd.total_folder_size

        self.assertNotEqual(size_source, total_size_in_distribution)

        x_man.update()

        du_source = subprocess.run(["du", "-s", os.path.join(self.tmp_folder, self.test_files_folder)],
                                   stdout=subprocess.PIPE, universal_newlines=True)
        size_source = int(str(du_source.stdout).split()[0])

        total_size_in_distribution = 0
        for osd in x_man.distribution.OSDs.values():
            total_size_in_distribution += osd.total_folder_size

        self.assertEqual(size_source, total_size_in_distribution)

    def test_read_write(self):
        managed_folder_1 = os.path.join(self.mount_point_1, self.path_on_mount_point)
        os.makedirs(managed_folder_1)

        x_man_1 = OSDManager.OSDManager(managed_folder_1)
        x_man_1.copy_folders(self.depth_2_dirs)

        total_size_in_distribution_1 = 0
        for osd in x_man_1.distribution.OSDs.values():
            total_size_in_distribution_1 += osd.total_folder_size

        managed_folder_2 = os.path.join(self.mount_point_2, self.path_on_mount_point)
        x_man_2 = OSDManager.OSDManager(managed_folder_2)

        total_size_in_distribution_2 = 0
        for osd in x_man_2.distribution.OSDs.values():
            total_size_in_distribution_2 += osd.total_folder_size

        self.assertEqual(total_size_in_distribution_1, total_size_in_distribution_2)


def count_folder_and_files(top_dir):
    # calculate and return some number, based on a directory tree rooted at top_dir
    num = 0
    for item in os.walk(top_dir):
        for directory in item[1]:
            if not directory.startswith('.'):
                num += 1
        for file in item[2]:
            if not file.startswith('.'):
                num += 1
    return num


def clean_up_volume(mount_point):
    clean_up_folder(mount_point)
    subprocess.run(["xtfsutil",
                    "--set-pattr", "1004.filenamePrefix", "--value", "clear", mount_point],
                   stdout=subprocess.PIPE, universal_newlines=True)


def clean_up_folder(folder):
    try:
        for item in os.listdir(folder):
            if os.path.isdir(os.path.join(folder, item)):
                shutil.rmtree(os.path.join(folder, item), ignore_errors=True)
            else:
                os.remove(os.path.join(folder, item))

        assert len(os.listdir(folder)) == 0
    except FileNotFoundError:
        pass
