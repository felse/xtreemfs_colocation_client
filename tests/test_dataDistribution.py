import random
import unittest

from xtreemfs_client import dataDistribution
from xtreemfs_client import osd
from xtreemfs_client import folder

osd_id_prefix = 'osd_'
folder_id_prefix = 'folder_'
osd_capacity_key = 'capacity'


class TestDataDistribution(unittest.TestCase):
    def setUp(self):
        osd_capacity = 100
        capacity_key = 'capacity'
        capacities = {}
        for i in range(0, 2):
            new_osd = osd.OSD('osd' + str(i))
            capacities[new_osd.uuid] = {}
            capacities[new_osd.uuid][capacity_key] = osd_capacity

        random.seed(9234)

    def test_totally_random_distribution(self):
        # generate some random distributions and check whether they are different
        max_osd_total_folder_size = 0
        felix_and_farouk_different = False

        num_osds = 3
        osds_capacities = [100]
        num_folders = 10
        folder_sizes = [20]

        for i in range(0, 100):
            distribution_felix = dataDistribution.DataDistribution()
            distribution_felix.add_osd_list(create_test_osd_list(num_osds, osds_capacities))
            distribution_felix.add_folders(create_test_folder_list(num_folders, folder_sizes),
                                           random_osd_assignment=True,
                                           ignore_osd_capacities=True)

            distribution_farouk = dataDistribution.DataDistribution()
            distribution_farouk.add_osd_list(create_test_osd_list(num_osds, osds_capacities))
            distribution_farouk.add_folders(create_test_folder_list(num_folders, folder_sizes),
                                            random_osd_assignment=True,
                                            ignore_osd_capacities=True)

            osds_felix = distribution_felix.get_osd_list()
            osds_felix_total_folder_sizes = list(
                map(lambda x: distribution_felix.OSDs[x].total_folder_size, osds_felix))

            osds_farouk = distribution_farouk.get_osd_list()
            osds_farouk_total_folder_sizes = list(
                map(lambda x: distribution_farouk.OSDs[x].total_folder_size, osds_farouk))

            if osds_felix_total_folder_sizes[0] != osds_farouk_total_folder_sizes[0]:
                felix_and_farouk_different = True

            max_felix = max(osds_felix_total_folder_sizes)
            max_farouk = max(osds_farouk_total_folder_sizes)

            max_osd_total_folder_size = max(max_osd_total_folder_size, max_felix, max_farouk)

        self.assertTrue(felix_and_farouk_different)
        self.assertTrue(max_osd_total_folder_size > osds_capacities[0])

    def test_random_distribution_respecting_capacities(self):
        # generate some random distributions and check whether they all respect the OSD capacities
        num_osds = 3
        osds_capacities = [100]
        num_folders = 10
        folder_size = [20]

        max_osd_total_folder_size = 0

        for i in range(0, 100):
            distribution = dataDistribution.DataDistribution()
            distribution.add_osd_list(create_test_osd_list(num_osds, osds_capacities))
            distribution.set_osd_capacities(create_osd_information(num_osds, osds_capacities))

            distribution.add_folders(create_test_folder_list(num_folders, folder_size),
                                     random_osd_assignment=True,
                                     ignore_osd_capacities=False)

            osds = distribution.get_osd_list()
            total_folder_sizes = list(map(lambda x: distribution.OSDs[x].total_folder_size, osds))
            max_osd_total_folder_size = max(max(total_folder_sizes), max_osd_total_folder_size)

        self.assertTrue(max_osd_total_folder_size <= osds_capacities[0])

    def test_random_round_robin_distribution(self):
        # generate some random distributions
        # and check whether OSDs are almost-equally loaded and whether they are different
        num_osds = 3
        osd_capacities = [0]
        num_folders = 10
        folder_sizes = [1]

        a_b_different = False

        for i in range(0, 100):
            distribution_a = dataDistribution.DataDistribution()
            distribution_a.add_osd_list(create_test_osd_list(num_osds, osd_capacities))
            distribution_a.add_folders(create_test_folder_list(num_folders, folder_sizes),
                                       random_osd_assignment=True,
                                       ignore_folder_sizes=True)
            distribution_b = dataDistribution.DataDistribution()
            distribution_b.add_osd_list(create_test_osd_list(num_osds, osd_capacities))
            distribution_b.add_folders(create_test_folder_list(num_folders, folder_sizes),
                                       random_osd_assignment=True,
                                       ignore_folder_sizes=True)

            osds_a = distribution_a.get_osd_list()
            total_folder_sizes_a = list(map(lambda x: distribution_a.OSDs[x].total_folder_size, osds_a))
            self.assertTrue(max(total_folder_sizes_a) is not min(total_folder_sizes_a))

            osds_b = distribution_b.get_osd_list()
            total_folder_sizes_b = list(map(lambda x: distribution_b.OSDs[x].total_folder_size, osds_b))
            self.assertTrue(max(total_folder_sizes_b) is not min(total_folder_sizes_b))

            if list(list(distribution_a.OSDs.values())[0].folders.keys())[0] \
                    != list(list(distribution_b.OSDs.values())[0].folders.keys())[0]:
                a_b_different = True

        self.assertTrue(a_b_different)

    def test_lpt_distribution(self):
        folder_sizes = [3, 7, 11]
        num_folders = 4
        num_osds = 4
        osd_capacities = [0]

        # test for equally-sized OSDs
        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_capacities))
        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes))
        osds = distribution.get_osd_list()
        total_folder_sizes = list(map(lambda x: distribution.OSDs[x].total_folder_size, osds))
        self.assertTrue(min(total_folder_sizes) == max(total_folder_sizes))

        # test 1 for differently-sized OSDs
        osd_bandwidths_1 = [10, 20]
        folder_sizes = [4, 4, 4, 4, 4, 4]

        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_bandwidths_1))
        distribution.set_osd_bandwidths(create_osd_information(num_osds, osd_bandwidths_1))

        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes))
        osds = distribution.get_osd_list()
        total_folder_sizes = list(map(lambda x: distribution.OSDs[x].total_folder_size, osds))
        self.assertTrue(2 * min(total_folder_sizes) == max(total_folder_sizes))

        # test 2 for differently-sized OSDs. the expected result is that the 4 large OSD receive 2 files each,
        # while the 4 small OSDs receive no files.
        osd_bandwidths_2 = [10, 30]
        folder_sizes = [1]
        num_folders = 8

        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_bandwidths_2))
        distribution.set_osd_bandwidths(create_osd_information(num_osds, osd_bandwidths_2))

        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes))
        osds = distribution.get_osd_list()
        total_folder_sizes = list(map(lambda x: distribution.OSDs[x].total_folder_size, osds))

        self.assertEqual(0, min(total_folder_sizes))
        self.assertEqual(2, max(total_folder_sizes))

    def test_average_osd_processing_time(self):
        folder_sizes = [48, 123, 1, 7]
        num_folders = 2
        num_osds = 4
        osd_bandwidths = [10, 15]

        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_bandwidths))
        distribution.set_osd_bandwidths(create_osd_information(num_osds, osd_bandwidths))

        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes))

        average = 3.05
        self.assertEqual(average, distribution.get_average_processing_time())

    def test_average_total_folder_size(self):
        folder_sizes = [49, 123, 1, 7]
        num_folders = 2
        num_osds = 4
        osd_capacities = [100, 150]
        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_capacities))
        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes),
                                 create_osd_information(num_osds, osd_capacities),
                                 osd_capacity_key)

        average = (sum(folder_sizes) * num_folders) / (num_osds * len(osd_capacities))
        self.assertEqual(average, distribution.get_average_load())

    def test_rebalance_lpt(self):
        folder_sizes = [1]
        num_folders = 8
        osd_capacities = [10]
        num_osds = 4

        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_capacities))
        distribution.set_osd_capacities(create_osd_information(num_osds, osd_capacities))

        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes), random_osd_assignment=True)
        distribution.rebalance_lpt()

        osds = distribution.get_osd_list()
        total_folder_sizes = list(map(lambda x: distribution.OSDs[x].total_folder_size, osds))
        # we should obtain a perfectly balanced distribution
        self.assertEqual(min(total_folder_sizes), max(total_folder_sizes))

    def test_rebalance_one_folder(self):
        folder_sizes = [1]
        num_folders = 8
        osd_bandwidths = [10]
        num_osds = 4

        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_bandwidths))
        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes), random_osd_assignment=True)

        distribution.rebalance_one_folder()

        osds = distribution.get_osd_list()
        total_folder_sizes = list(map(lambda x: distribution.OSDs[x].total_folder_size, osds))
        # we should obtain a perfectly balanced distribution
        self.assertEqual(min(total_folder_sizes), max(total_folder_sizes))

        osd_bandwidths = [10, 30]
        folder_sizes = [1]
        num_folders = 8
        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_bandwidths))
        distribution.set_osd_bandwidths(create_osd_information(num_osds, osd_bandwidths))

        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes), random_osd_assignment=True)

        distribution.rebalance_one_folder()
        osds = distribution.get_osd_list()
        total_folder_sizes = list(map(lambda x: distribution.OSDs[x].total_folder_size, osds))

        # all folders should now be on the 'large' OSDs
        self.assertEqual(0, min(total_folder_sizes))
        self.assertEqual(2, max(total_folder_sizes))

    def test_rebalance_two_steps(self):
        folder_sizes = [1]
        num_folders = 12
        osd_bandwidths = [10]
        num_osds = 4

        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_bandwidths))
        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes), random_osd_assignment=True)

        distribution.rebalance_two_steps_optimal_matching()

        osds = distribution.get_osd_list()
        total_folder_sizes = list(map(lambda x: distribution.OSDs[x].total_folder_size, osds))

        # 12 unit size folders on 4 OSD => each should have 3 files
        self.assertEqual(3, min(total_folder_sizes))
        self.assertEqual(3, max(total_folder_sizes))
        self.assertEqual(12, sum(total_folder_sizes))

        # same test with one more folder size
        folder_sizes = [1, 2]

        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_bandwidths))
        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes), random_osd_assignment=True)

        distribution.rebalance_two_steps_optimal_matching()

        osds = distribution.get_osd_list()
        total_folder_sizes = list(map(lambda x: distribution.OSDs[x].total_folder_size, osds))

        self.assertEqual(9, min(total_folder_sizes))
        self.assertEqual(9, max(total_folder_sizes))
        self.assertEqual(36, sum(total_folder_sizes))

    def test_get_lower_bound_on_makespan(self):
        folder_sizes = [1]
        num_folders = 5
        osd_bandwidths = [1]
        num_osds = 4

        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_bandwidths))
        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes), random_osd_assignment=True)

        self.assertEqual(5 / 4, distribution.get_lower_bound_on_makespan())

        folder_sizes = [1]
        num_folders = 60
        osd_bandwidths = [3, 2, 1]
        osd_capacities = [2, 5, 100]
        num_osds = 4

        distribution = dataDistribution.DataDistribution()
        distribution.add_osd_list(create_test_osd_list(num_osds, osd_bandwidths))
        distribution.set_osd_bandwidths(create_osd_information(num_osds, osd_bandwidths))
        distribution.set_osd_capacities(create_osd_information(num_osds, osd_capacities))

        distribution.add_folders(create_test_folder_list(num_folders, folder_sizes))

        self.assertEqual(8, distribution.get_maximum_processing_time()[1])


def create_test_osd_list(num_osds, osd_capacities):
    test_osds = []
    for i in range(0, num_osds * len(osd_capacities)):
        for osd_capacity in osd_capacities:
            test_osds.append(create_osd_id(i))
    return test_osds


def create_test_folder_list(num_folders, folder_sizes):
    test_folders = []
    for i in range(0, num_folders):
        for folder_size in folder_sizes:
            new_folder = folder.Folder(folder_id_prefix + "_" + str(folder_size) + "_" + str(i), folder_size, None)
            test_folders.append(new_folder)
    random.shuffle(test_folders)
    return test_folders


def create_osd_information(num_osds, osd_capacities):
    osd_information = {}
    i = 0
    for osd_capacity in osd_capacities:
        for j in range(0, num_osds):
            osd_information[create_osd_id(i)] = osd_capacity
            i += 1
    return osd_information


def create_osd_id(index):
    return osd_id_prefix + "_" + str(index)
