import random
import copy

from ortools.graph import pywrapgraph

from xtreemfs_client import osd
from xtreemfs_client import folder


class DataDistribution(object):
    """
    class to keep track of the osd (object storage device) locations of different folders, i.e.,
    their physical location.

    this class also allows to calculate several data distributions, e.g., mappings from folders to OSDs (each folder
    gets mapped to one OSD).
    """

    def __init__(self):
        self.OSDs = {}

    def add_new_osd(self, osd_uuid):
        """
        create a new empty osd and add it to the existing OSDs.
        """
        if osd_uuid in self.OSDs:
            print("key: " + osd_uuid + " is already present!")
            return
        new_osd = osd.OSD(osd_uuid)
        self.OSDs[osd_uuid] = new_osd

    def add_osd(self, new_osd):
        """
        add the given OSD (object) to the existing OSDs.
        """
        if new_osd.uuid in self.OSDs:
            print("key: " + new_osd.uuid + " is already present!")
            return
        self.OSDs[new_osd.uuid] = new_osd

    def add_osd_list(self, osd_list):
        """
        add the given list of OSDs (objects) to the existing OSDs.
        """
        for osd_uuid in osd_list:
            if osd_uuid not in self.OSDs:
                new_osd = osd.OSD(osd_uuid)
                self.OSDs[osd_uuid] = new_osd

    def replace_osd(self, new_osd):
        """
        replaces the osd with uuid new_osd.uuid by new_osd
        :param new_osd:
        :return:
        """
        assert new_osd.uuid in self.OSDs.keys()
        self.OSDs[new_osd.uuid] = new_osd

    def set_osd_capacities(self, osd_capacities):
        """
        set osd capacities
        :param osd_capacities: map from osd uuids to osd capacities
        :return:
        """
        # make sure that the keyset of self.OSDs matches the keyset of osd_capacities
        for osd_uuid in osd_capacities:
            assert osd_uuid in self.OSDs.keys()
        assert len(self.OSDs) == len(osd_capacities)
        for one_osd in self.OSDs.values():
            assert type(osd_capacities[one_osd.uuid]) is int
            one_osd.capacity = osd_capacities[one_osd.uuid]

    def set_osd_bandwidths(self, osd_bandwidths):
        """
        set osd bandwidths
        :param osd_bandwidths:
        :return:
        """
        for one_osd in self.OSDs.values():
            one_osd.bandwidth = osd_bandwidths[one_osd.uuid]

    def get_osd_list(self):
        """
        get a list of all existing OSD uuids.
        """
        osd_list = []
        for osd_name in self.OSDs.keys():
            osd_list.append(osd_name)
        return osd_list

    def get_containing_osd(self, folder_id):
        """
        get the OSD containing the given folder_id, or None if the folder is not assigned to any OSD.
        """
        for checked_osd in self.OSDs.values():
            if checked_osd.contains_folder(folder_id):
                return checked_osd
        return None

    def get_folder_size(self, folder_id):
        containing_osd = self.get_containing_osd(folder_id)
        assert containing_osd is not None
        return containing_osd.get_folder_size(folder_id)

    def assign_new_osd(self, folder_id, new_osd):
        """
        assign folder_id to new_osd. if folder_id already is assigned to an OSD, this old assignment is deleted.
        """
        old_osd = self.get_containing_osd(folder_id)
        if old_osd is None:
            self.OSDs[new_osd].add_folder(folder_id, self.get_average_folder_size())
        else:
            self.OSDs[new_osd].add_folder(folder_id, self.OSDs[old_osd.uuid].folders[folder_id])
            self.OSDs[old_osd.uuid].remove_folder(folder_id)

    def get_total_folder_size(self):
        total_size = 0
        for one_osd in self.OSDs.values():
            total_size += one_osd.total_folder_size
        return total_size

    def get_total_bandwidth(self):
        total_bandwidth = 0
        for one_osd in self.OSDs.values():
            total_bandwidth += one_osd.bandwidth
        return total_bandwidth

    def get_total_capacity(self):
        total_capacity = 0
        for one_osd in self.OSDs.values():
            total_capacity += one_osd.capacity
        return total_capacity

    def get_average_folder_size(self):
        """
        get the average folder size of all folders of all OSDs.
        """
        total_size = 0
        total_number_of_folders = 0
        for one_osd in self.OSDs.values():
            total_size += one_osd.total_folder_size
            total_number_of_folders += len(one_osd.folders)
        if total_number_of_folders == 0:
            return 0
        return total_size / total_number_of_folders

    def get_average_load(self):
        """
        calculate the average OSD load, that is, the average of their total_folder_size.
        """
        total_folder_size = 0
        for osd in self.OSDs.values():
            total_folder_size += osd.get_load()
        return total_folder_size / len(self.OSDs)

    def get_maximum_load(self):
        """
        calculate the maximum OSD load, that is, the maximum of their total_folder_size.
        """
        maximum_load = 0
        maximum_osd = None
        for osd in self.OSDs.values():
            load = osd.total_folder_size
            if maximum_osd is None or load > maximum_load:
                maximum_load = load
                maximum_osd = osd
        return maximum_osd, maximum_load

    def get_average_processing_time(self):
        """
        calculate the average OSD processing time, that is, the average of their (total_folder_size / bandwidth).
        :return:
        """
        total_processing_time = 0
        for osd in self.OSDs.values():
            total_processing_time += osd.get_processing_time()
        return total_processing_time / len(self.OSDs)

    def get_maximum_processing_time(self):
        """
        calculate the maximum OSD processing time, also known as makespan
        """
        maximum_processing_time = 0
        maximum_osd = None
        for osd in self.OSDs.values():
            processing_time = osd.get_processing_time()
            if maximum_osd is None or processing_time > maximum_processing_time:
                maximum_processing_time = processing_time
                maximum_osd = osd
        return maximum_osd, maximum_processing_time

    def get_lower_bound_on_makespan(self):
        return self.compute_relaxed_assignment().get_maximum_processing_time()[1]

    def compute_relaxed_assignment(self):
        total_remaining_file_size = self.get_total_folder_size()
        relaxed_assignment = copy.deepcopy(self)
        for tmp_osd in relaxed_assignment.OSDs.values():
            for a_folder in list(tmp_osd.folders.keys()):
                tmp_osd.remove_folder(a_folder)

        while total_remaining_file_size > 0:
            free_OSDs = list(filter(lambda x: x.get_free_capacity() > 0, relaxed_assignment.OSDs.values()))
            total_bandwidth = sum(list(map(lambda x: x.bandwidth, free_OSDs)))
            assigned_file_size = 0
            for free_OSD in free_OSDs:
                optimal_share = (free_OSD.bandwidth / total_bandwidth) * total_remaining_file_size
                assignable_share = min(optimal_share, free_OSD.get_free_capacity())
                free_OSD.add_folder("dummy_id", assignable_share)
                assigned_file_size += assignable_share
            total_remaining_file_size -= assigned_file_size

        return relaxed_assignment

    def add_folders(self, folders,
                    ignore_osd_capacities=True,
                    random_osd_assignment=False,
                    ignore_folder_sizes=False,
                    debug=False,
                    random_seed=None):
        """
        adds a list of folders to the data distribution.
        if not specified otherwise, the assignments are calculated using the LPT algorithm.
        returns a list of assignments from folders to OSDs, for which (folders) there was previously no assignment.

        if capacities and bandwidths are set for the OSDs, folders are assigned accordingly
        (capacities are respected and OSDs with higher bandwidth obtain more/larger files).

        if random_osd_assignment=True and ignore_osd_capacities=True, a totally random OSD assignment generated.

        if random_osd_assignment=True and ignore_folder_sizes=True,
        folders are randomly assigned to OSDs such that all OSDs have the same number of folders (if possible).

        the assignment is stable (i.e., folders already assigned to an OSD are not reassigned to another OSD).
        """

        # find out which folders are not assigned yet
        new_folders = []
        for a_folder in folders:
            # TODO adding folders to OSDs might violate their capacity
            containing_osd = self.get_containing_osd(a_folder.id)
            if containing_osd is not None:
                containing_osd.add_folder(a_folder.id, a_folder.size)
            else:
                new_folders.append(a_folder)

        if debug:
            print("dataDistribution: random_osd_assignment: " + str(random_osd_assignment))

        # keep track of which unassigned folder gets assigned to which OSD.
        # this information must be returned
        osds_for_new_folders = []

        if random_osd_assignment:
            random.seed(random_seed)
            if debug and random_seed is not None:
                print("using random seed: " + str(random_seed))

        # totally random OSD assignment, ignoring OSD capacities
        # (might lead to I/O errors when too many groups are assigned to an OSD)
        if random_osd_assignment and ignore_osd_capacities and not ignore_folder_sizes:
            if debug:
                print("using totally random osd assignment")
            for a_folder in new_folders:
                random_osd = random.choice(list(self.OSDs.values()))
                random_osd.add_folder(a_folder.id, a_folder.size)
                osds_for_new_folders.append((a_folder.id,
                                             random_osd.uuid))
            return osds_for_new_folders

        # random OSD assignment respecting OSD capacities
        elif random_osd_assignment and not ignore_osd_capacities:
            if debug:
                print("using random osd assignment, respecting osd capacities")
            for a_folder in new_folders:
                suitable_osds = self.get_suitable_osds(a_folder.size)  # list of OSDs with enough capacity
                suitable_random_osd = random.choice(suitable_osds)
                suitable_random_osd.add_folder(a_folder.id, a_folder.size)
                osds_for_new_folders.append((a_folder.id,
                                             suitable_random_osd.uuid))
            return osds_for_new_folders

        # random OSD assignment ignoring folder sizes // round-robin style distribution with some randomness
        elif random_osd_assignment and ignore_folder_sizes:
            if debug:
                print("using random osd assignment ignoring folder sizes")

            average_folder_size = self.get_average_folder_size()
            if average_folder_size == 0:
                average_folder_size = 1

            modified_folders = list(map(lambda f: folder.Folder(f.id, average_folder_size, f.origin), folders))
            random.shuffle(modified_folders)
            return self.add_folders(modified_folders)

        # balanced deterministic OSD assignment (LPT)
        # (following largest processing time first, also called post-greedy approach)
        list.sort(new_folders, key=lambda x: x.size, reverse=True)

        # for each folder calculate the best OSD and add it to it
        for a_folder in new_folders:
            least_used_osd, _ = self.get_lpt_osd(a_folder.size)
            least_used_osd.add_folder(a_folder.id, a_folder.size)
            osds_for_new_folders.append((a_folder.id,
                                         least_used_osd.uuid))
        return osds_for_new_folders

    def rebalance_lpt(self, rebalance_factor=1):
        """
        rebalance folders to OSDs by assigning folders to new OSDs using the following strategy:
                1. 'unroll' the assignment. this means that, for each OSD, folders are removed until the OSD has less
                processing time than the average processing time of this distribution multiplied by rebalance_factor.
                2. reassign the removed folders using the LPT strategy.
        """
        total_folder_size = self.get_total_folder_size()
        movements = {}
        folders_to_be_reassigned = []

        # for each OSD, remove the smallest folder until its total_folder_size does not exceed the reassignment_limit
        # unrolling
        relaxed_assignment = self.compute_relaxed_assignment()
        for osd in self.OSDs.values():
            # self.get_total_folder_size / self.get_total_bandwidth() is the optimal processing time for each OSD:
            # this value is a lower bound for the makespan
            # reassignment_limit = self.get_rebalance_limit(rebalance_factor, total_folder_size)
            reassignment_limit = relaxed_assignment.OSDs[osd.uuid].get_processing_time() * rebalance_factor
            while osd.get_processing_time() > reassignment_limit:
                folder_id, folder_size = osd.get_smallest_folder()
                folders_to_be_reassigned.append(folder.Folder(folder_id, folder_size, None))
                movements[folder_id] = osd.uuid
                osd.remove_folder(folder_id)

        # reassignment
        new_assignments = self.add_folders(folders_to_be_reassigned)

        for folder_id, target in new_assignments:
            if movements[folder_id] == target:
                del movements[folder_id]
            else:
                movements[folder_id] = (movements[folder_id], target)

        return movements

    def get_rebalance_limit(self, factor, total_folder_size):
        return factor * (total_folder_size / self.get_total_bandwidth())

    def rebalance_one_folder(self):
        """
        rebalance folders to OSDs by assigning folders to new OSDs using the following strategy:
                1. find OSD with the highest processing time
                2. get folder with smallest size on this OSD
                3. find new OSD for this folder using get_lpt_osd
                4. if the processing time on the new OSD is lower than on the original OSD,
                move the folder to the new OSD. otherwise, return.
        one open question is whether getting the folder with smallest size in step 2 is a clever choice
        (in principle, all folders of the OSD with the highest load are eligible).

        this optimization scheme classifies as local search. two distributions are neighbors if one can be transformed
        into the other by moving one folder from one OSD to another. note, however, that we do not search the whole
        neighborhood of a distribution.
        but it might be possible to show that if there is no improvement step of the type that we check for,
        there is no improvement step at all.
        """
        movements = {}

        while True:
            # find OSD with the highest processing time (origin)
            origin_osd, maximum_processing_time = self.get_maximum_processing_time()

            # pick a folder of this OSD
            # there are several ways to pick a folder (like largest, smallest, constrained by the resulting load of the
            # origin OSD, random...), it is not clear which way is a good way
            # for now pick the smallest folder on origin OSD
            smallest_folder_id, smallest_folder_size = self.OSDs[origin_osd.uuid].get_smallest_folder()

            # find other OSD best suited for the picked folder (target)
            # check whether moving folder from origin to target decreases the maximum load of all OSDs (makespan).
            best_osd, best_osd_processing_time = self.get_lpt_osd(smallest_folder_size)

            if best_osd_processing_time < maximum_processing_time:
                self.assign_new_osd(smallest_folder_id, best_osd.uuid)
                movements[smallest_folder_id] = (origin_osd.uuid, best_osd.uuid)
            else:
                break

        return movements

    def rebalance_two_steps_optimal_matching(self):
        """
        rebalance the distribution in two steps:
            1. calculate new distribution, independently of the current one
            2. use a minimum weight matching to transform the current distribution into the new distribution.
            minimum weight perfect matching on bipartite graphs can be solved using the successive shortest path
            algorithm.
        while any algorithm (solving/approximating that kind of problem) could be used for the first step,
        we here only implement the LPT algorithm, as it is a pretty good approximation with extremely good running time.
        :return:
        """
        virtual_distribution = copy.deepcopy(self)
        virtual_distribution.rebalance_lpt(rebalance_factor=0)

        # create a mincostflow object
        min_cost_flow = pywrapgraph.SimpleMinCostFlow()

        # define the directed graph for the flow
        # arcs are added individually, and are added implicitly
        # nodes (OSDs) have to be given by numeric id
        # so we need some conversion logic between current/virtual osds and node ids

        current_osds_list = list(self.OSDs.values())
        current_osds_list.sort(key=lambda x: x.uuid)
        virtual_osds_list = list(virtual_distribution.OSDs.values())
        virtual_osds_list.sort(key=lambda x: x.uuid)

        # conversion logic:
        # n = len(current_osd_list) = len(virtual_osd_list)
        # 0 = source, 1 = sink
        # 2, ..., n + 1: current OSDs
        # n + 2, ..., 2n + 1: virtual OSDs
        num_osds = len(current_osds_list)
        assert num_osds == len(virtual_osds_list)

        # edges between the two partitions
        for i in range(0, num_osds):
            for j in range(0, num_osds):
                current_osd = current_osds_list[i]
                virtual_osd = virtual_osds_list[j]
                # calculate the total size of folders that the current OSD has to fetch if the virtual OSD is assigned
                # to it
                edge_cost = 0
                for folder_id in virtual_osd.folders.keys():
                    if not current_osd.contains_folder(folder_id):
                        edge_cost += virtual_osd.folders[folder_id]
                tail = 2 + i  # current OSD
                head = num_osds + 2 + j  # virtual OSD
                min_cost_flow.AddArcWithCapacityAndUnitCost(tail, head, 1, edge_cost)

        # (artificial) edges between the source node and the current OSDs
        for i in range(0, num_osds):
            edge_cost = 0
            tail = 0
            head = i + 2
            min_cost_flow.AddArcWithCapacityAndUnitCost(tail, head, 1, edge_cost)

        # (artificial) edges between the virtual OSDs and the sink node
        for j in range(0, num_osds):
            edge_cost = 0
            tail = num_osds + 2 + j
            head = 1
            min_cost_flow.AddArcWithCapacityAndUnitCost(tail, head, 1, edge_cost)

        # define the supplies (which equals the number of OSDs)
        min_cost_flow.SetNodeSupply(0, num_osds)
        min_cost_flow.SetNodeSupply(1, -num_osds)

        # solve the min cost flow problem
        min_cost_flow.Solve()

        # we need to transform the calculated optimal assignment into a rebalanced distribution, including the necessary
        # movements
        current_to_virtual_osd_matching = []
        for arc in range(min_cost_flow.NumArcs()):
            tail = min_cost_flow.Tail(arc)
            head = min_cost_flow.Head(arc)
            if tail != 0 and head != 1 and min_cost_flow.Flow(arc) == 1:
                current_osd = current_osds_list[tail - 2]
                virtual_osd = virtual_osds_list[head - num_osds - 2]
                current_to_virtual_osd_matching.append((current_osd, virtual_osd))

        movements = {}
        for current_osd, virtual_osd in current_to_virtual_osd_matching:
            # iterate over virtual folders and check whether they are on the correct OSD.
            # the correct OSD is current_osd, as it is the one that is matched with virtual_osd.
            # if it is not present on current_osd, assign it to it.
            # this also removes it from the origin osd.
            for virtual_folder in virtual_osd.folders.keys():
                if not current_osd.contains_folder(virtual_folder):
                    origin_osd = self.get_containing_osd(virtual_folder).uuid
                    target_osd = current_osd.uuid
                    movements[virtual_folder] = (origin_osd, target_osd)

        for current_osd, virtual_osd in current_to_virtual_osd_matching:
            virtual_osd.uuid = current_osd.uuid
            self.replace_osd(virtual_osd)

        return movements

    def rebalance_two_steps_random_matching(self):
        """
        rebalance the distribution in two steps:
            1. calculate new distribution, independently of the current one
            2. the OSDs of the new (virtual) matching are randomly assigned to the actual (current OSDs), i.e.,
            no matter which OSD has which folders.
        while any algorithm (solving/approximating that kind of problem) could be used for the first step,
        we here only implement the LPT algorithm, as it is a pretty good approximation with extremely good running time.
        :return:
        """
        virtual_distribution = copy.deepcopy(self)
        virtual_distribution.rebalance_lpt(rebalance_factor=0)

        movements = {}

        for virtual_osd in virtual_distribution.OSDs.values():
            for virtual_folder in virtual_osd.folders.keys():
                if not self.OSDs[virtual_osd.uuid].contains_folder(virtual_folder):
                    movements[virtual_folder] = (self.get_containing_osd(virtual_folder).uuid, virtual_osd.uuid)

        for virtual_osd in virtual_distribution.OSDs.values():
            self.replace_osd(virtual_osd)

        return movements

    def get_suitable_osds(self, folder_size):
        """
        create a list of OSDs with at least folder_size free capacity.
        :return:
        """
        suitable_osds = []
        for one_osd in self.OSDs.values():
            if one_osd.capacity - one_osd.total_folder_size - folder_size >= 0:
                suitable_osds.append(one_osd)
        if len(suitable_osds) == 0:
            print("no suitable OSD found!")
            print("total OSD capacity: " + str(self.get_total_capacity()))
            print("current total folder size: " + str(self.get_total_folder_size()))
        return suitable_osds

    def get_lpt_osd(self, folder_size):
        """
        calculate the processing time of all OSDs, using the sum of their current total_folder_size and folder_size.
        return (OSD with the smallest such value, the smallest value)
        """
        best_processing_time = None
        best_processing_time_osd = -1
        for one_osd in self.get_suitable_osds(folder_size):
            processing_time = (one_osd.total_folder_size + folder_size) / one_osd.bandwidth
            if (best_processing_time is None) or processing_time < best_processing_time_osd:
                best_processing_time = one_osd
                best_processing_time_osd = processing_time
        return best_processing_time, best_processing_time_osd

    def update_folder(self, folder, size):
        """
        updates the size of a given folder
        """
        found_containing_osd = False
        for one_osd in self.OSDs.values():
            if folder in one_osd.folders.keys():
                one_osd.update_folder(folder, size)
                found_containing_osd = True
                break
        if not found_containing_osd:
            print("update_folder: could not find a containing OSD for folder id: " + str(folder))
        assert found_containing_osd is True

    def description(self):
        """
        generates a string describing this data distribution
        """
        string = ""
        for one_osd in self.OSDs.values():
            string += str(one_osd)
            string += "\n"
            string += "folders : " + str(one_osd.folders)
            string += "\n"
        string += "average folder size: " + str(self.get_average_folder_size())
        return string

    def __str__(self):
        string_representation = "DataDistribution has " + str(len(self.OSDs)) \
                                + " osds: \n"
        for key, value in self.OSDs.items():
            string_representation += str(value) + " \n"
        return string_representation
