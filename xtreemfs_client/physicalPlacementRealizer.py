import datetime
import os
import random

import time

from xtreemfs_client import OSDManager
from xtreemfs_client import div_util


class FileToMove(object):
    def __init__(self, absolute_file_path, origin_osd, target_osd,
                 policy_command, create_replica_command, delete_replica_command):
        self.absolute_file_path = absolute_file_path
        self.origin_osd = origin_osd
        self.target_osd = target_osd
        self.policy_command = policy_command
        self.create_replica_command = create_replica_command
        self.delete_replica_command = delete_replica_command


max_processes_change_policy = 200
max_processes_add_replica = 200
max_processes_delete_replica = 200


class PhysicalPlacementRealizer(object):
    def __init__(self, osd_manager: OSDManager, debug=False, repeat_delete_interval_secs=15,
                 max_files_in_progress=10000, max_files_in_progress_per_osd=200, max_execute_repetitions=5):
        self.osd_manager = osd_manager
        self.files_to_be_moved = {}
        self.iterations = 0
        self.max_files_in_progress_total = max_files_in_progress
        self.max_files_in_progress_per_osd = max_files_in_progress_per_osd
        self.debug = debug
        self.repeat_delete_interval_secs = repeat_delete_interval_secs
        self.max_execute_repetitions = max_execute_repetitions

    def realize_placement(self, strategy='osd_balanced'):
        """
          fixes the physical layout, such that it matches the data distribution described in self.distribution
          we use the following strategy: first, determine which files needs to be moved to another OSD, and create three lists.
          for each file to be moved, one contains the command to set the default replication policy (1), one contains the command
          to create a new replica on the target OSD (2) and one contains the command to delete the replica on the original OSD (3).
          the command-lists are executed in order: 1, 2, 3, commands of list i only being executed if all commands of list i-1
          have returned. the commands of any one list are executed in parallel, with an upper bound of 200 parallel executions
          to not violate the maximum number of processes of the OS.

          the commands of list 3 might return an error, as deleting the replica on the original OSD is not possible until
          the new replica on the target OSD is complete. theses commands are collected and re-executed after a certain delay.
          this process is repeated until no error is returned, which should eventually happen (as soon as all new replicas on
          the target OSDs are complete).
        """
        iteration = 0
        self.calculate_files_to_be_moved()

        while len(list(self.files_to_be_moved.keys())) > 0:
            if self.debug:
                print("starting to fix physical layout...this is fix-iteration " + str(iteration))

            if strategy == 'osd_balanced':
                self.move_files_osd_balanced()
            elif strategy == 'random':
                self.move_files_randomly()
            self.update_files_to_be_moved()
            iteration += 1

    def get_list_of_all_files_to_be_moved(self):
        files_to_be_moved_list = []
        for list_per_key in self.files_to_be_moved.values():
            for file_to_be_moved in list_per_key:
                files_to_be_moved_list.append(file_to_be_moved)

        return files_to_be_moved_list

    def update_files_to_be_moved(self):
        """
        update self.files_to_be_moved: check for all elements whether they still need to be moved.
        right now, we simply recalculate it from scratch, yielding the correct result, but being inefficient.
        :return:
        """
        # TODO implement
        self.calculate_files_to_be_moved()

    def calculate_files_to_be_moved(self):
        """
        method to populate self.files_to_be_moved.
        for each file in self.osd_manager.managed_folder, it is checked whether the file is on the OSD assigned by
        self.osd_manager.distribution. if this is not the case, the file is added to self.files_to_be_moved.
        more precisely, it is appended to the list at key (origin_osd, target_osd) in self.files_to_be_moved.
        :return:
        """
        self.files_to_be_moved = {}
        managed_folders = self.osd_manager.get_depth_2_subdirectories()
        for managed_folder in managed_folders:
            for directory in os.walk(managed_folder):
                for filename in directory[2]:
                    policy_command = None
                    create_command = None
                    delete_command = None
                    absolute_file_path = os.path.join(directory[0], filename)
                    osds_of_file = div_util.get_osd_uuids(absolute_file_path)
                    path_on_volume = self.osd_manager.get_path_on_volume(absolute_file_path)
                    containing_folder_id = self.osd_manager.get_containing_folder_id(path_on_volume)
                    osd_for_file = self.osd_manager.distribution.get_containing_osd(containing_folder_id).uuid

                    file_on_correct_osd = False
                    osd_of_file = None  # this assignment will always be overwritten,
                    # as there cannot be files in XtreemFS that do not have an OSD
                    for osd_of_file in osds_of_file:
                        if osd_of_file != osd_for_file:
                            # delete all replicas on wrong OSDs
                            delete_command = div_util.create_delete_replica_command(absolute_file_path, osd_of_file)
                        else:
                            file_on_correct_osd = True

                    if not file_on_correct_osd and len(osds_of_file) < 2:
                        # only one replica on a wrong OSD => need to set replication policy.
                        # otherwise, there is a unique replica on the correct OSD => no change necessary,
                        # OR there are multiple replicas => replication policy must be set.
                        policy_command = div_util.create_replication_policy_command(absolute_file_path)

                    if not file_on_correct_osd:
                        # create a replica on the correct osd
                        create_command = div_util.create_create_replica_command(absolute_file_path, osd_for_file)

                    # in python, strings are also booleans!!! :)
                    if policy_command or create_command or delete_command:
                        # create FileToMove object and add it to the corresponding list in the map
                        file_to_move = FileToMove(absolute_file_path,
                                                  osd_of_file,
                                                  osd_for_file,
                                                  policy_command,
                                                  create_command,
                                                  delete_command)

                        movement_key = (osd_of_file, osd_for_file)
                        if not movement_key in self.files_to_be_moved.keys():
                            self.files_to_be_moved[movement_key] = []
                        self.files_to_be_moved[movement_key].append(file_to_move)

    def get_next_files(self, movement_key):
        """
        get the next files to be moved from self.files_to_be_moved, that are contained in the list found at movement_key
        if any list becomes empty, it is removed from self.files_to_be_moved
        :param movement_key:
        :return:
        """
        next_files_to_move = []
        num_files = 0
        while len(self.files_to_be_moved[movement_key]) > 0 and num_files < self.max_files_in_progress_per_osd:
            next_files_to_move.append(self.files_to_be_moved[movement_key].pop())
            num_files += 1
        if len(self.files_to_be_moved[movement_key]) == 0:
            del self.files_to_be_moved[movement_key]
        return next_files_to_move

    def move_files_osd_balanced(self):
        """
        executes the necessary commands in order to move all files in self.files_to_be_moved to their target OSD.
        files are processed in such order that OSDs are more or less equally loaded at any point of time.
        :return:
        """
        num_movement_keys = len(list(self.files_to_be_moved.keys()))
        if num_movement_keys * self.max_files_in_progress_per_osd > self.max_files_in_progress_total:
            self.max_files_in_progress_per_osd = int(self.max_files_in_progress_total / num_movement_keys)
            if self.debug:
                print("setting max_files_in_progress_per_osd on: " + str(self.max_files_in_progress_per_osd))

        while len(list(self.files_to_be_moved.keys())) > 0:
            files_to_move_now = []
            for movement_key in list(self.files_to_be_moved.keys()):
                files_to_move_now.extend(self.get_next_files(movement_key))

            random.shuffle(files_to_move_now)
            self.execute_command_list(self.transform_files_to_move_into_three_command_lists(files_to_move_now))

    def move_files_randomly(self):
        """
        executes the necessary commands in order to move all files in self.files_to_be_moved to their target OSD.
        at most self.max_total_files_in_progress are treated in the same time.
        files are processed in random order.
        :return:
        """
        files_to_be_moved = self.get_list_of_all_files_to_be_moved()
        if self.debug:
            print("number of files that need to be moved: " + str(len(files_to_be_moved)))
        random.shuffle(files_to_be_moved)
        while len(files_to_be_moved) > 0:
            files_to_be_moved_now = []
            for i in range(0, self.max_files_in_progress_total):
                files_to_be_moved_now.append(files_to_be_moved.pop())
                if len(files_to_be_moved) == 0:
                    break
            self.execute_command_list(self.transform_files_to_move_into_three_command_lists(files_to_be_moved_now))

    def transform_files_to_move_into_three_command_lists(self, files_to_move):
        change_policy_command_list = []
        create_replica_command_list = []
        delete_replica_command_list = []

        for file_to_be_moved in files_to_move:
            if file_to_be_moved.policy_command is not None:
                change_policy_command_list.append(file_to_be_moved.policy_command)
            if file_to_be_moved.create_replica_command is not None:
                create_replica_command_list.append(file_to_be_moved.create_replica_command)
            if file_to_be_moved.delete_replica_command is not None:
                delete_replica_command_list.append(file_to_be_moved.delete_replica_command)

        return change_policy_command_list, create_replica_command_list, delete_replica_command_list

    def execute_command_list(self, command_list_triple):
        """
        executes three list of commands, given in the  that are typically needed for moving files in XtreemFS from their current OSD
        to the designated target OSD. the lists are treated as their order in the argument list. commands from the next
        list are only executed when all commands of the previous list have returned (or been killed).
        deletion commands are repeated a number of times, as the target OSD requires some time to create the new replica
        (for fetching all objects, i.e., file content).
        :param a triple (change_policy_command_list, create_replica_command_list, delete_replica_command_list)
        with: change_policy_command_list:list of change replication policy commands to be executed.
         create_replica_command_list: list of create replica commands to be executed.
         delete_replica_command_list: list of delete replica commands to be executed.
        :return:
        """
        change_policy_command_list = command_list_triple[0]
        create_replica_command_list = command_list_triple[1]
        delete_replica_command_list = command_list_triple[2]

        start_time = time.time()
        if self.debug:
            print("starting execution of " + str(len(change_policy_command_list)) + " change policy commands...")
            print(str(datetime.datetime.now()))
        errored_processes = div_util.run_commands(change_policy_command_list, max_processes_change_policy)
        end_time = time.time()
        if self.debug:
            print("executing " + str(len(change_policy_command_list)) + " change policy commands done in " +
                  str(round(end_time - start_time)) + " sec.")
            # div_util.print_process_list(processes)

        start_time = time.time()
        if self.debug:
            print("starting execution of " + str(len(create_replica_command_list)) + " create replica commands...")
            print(str(datetime.datetime.now()))
        random.shuffle(create_replica_command_list)
        errored_processes = div_util.run_commands(create_replica_command_list, max_processes_add_replica)
        end_time = time.time()
        if self.debug:
            print("executing " + str(len(create_replica_command_list)) + " create replica commands done in " +
                  str(round(end_time - start_time)) + " sec.")
            # div_util.print_process_list(processes)

        start_time = time.time()
        if self.debug:
            print("starting execution of " + str(len(delete_replica_command_list)) + " delete replica commands...")
            print(str(datetime.datetime.now()))
        errored_processes = div_util.run_commands(delete_replica_command_list, max_processes_delete_replica,
                                                  print_errors=False)

        # run and repeat delete commands, until they return no error
        # (if an error is returned for another reason than that one would delete the last complete replica,
        # this will probably not work.
        iterations = 0
        while True:
            if iterations >= self.max_execute_repetitions:
                print("results of last iteration: ")
                div_util.print_process_list(errored_processes)
                print("Original replicas could not be deleted after " + str(
                    self.max_execute_repetitions) + ". Aborting...")
                break

            if self.debug:
                print("executing " + str(len(delete_replica_command_list))
                      + " delete replica commands done. This is delete-iteration "
                      + str(iterations))
                # div_util.print_process_list(processes)

            errored_deletions = []

            for process in errored_processes:
                # check the return code. if it is one, the replica could not be deleted, so we try again later.
                if process[2] != 0:
                    errored_deletions.append(process[0])
                    if self.debug:
                        print("errored command: ")
                        print("command: " + str(process[0]))
                        print("stdoud: " + str(process[1][0]))
                        print("stderr: " + str(process[1][1]))
                        print("retcode: " + str(process[2]))

            if len(errored_deletions) == 0:
                break

            time.sleep(self.repeat_delete_interval_secs)

            if self.debug:
                print("rerunning " + str(
                    len(errored_deletions)) + " commands because replica could not be deleted...")

            errored_processes = div_util.run_commands(errored_deletions, max_processes_change_policy,
                                                      print_errors=False)
            iterations += 1

        if self.debug:
            end_time = time.time()
            print("deleting replicas done in in " + str(round(end_time - start_time)) + " sec.")
