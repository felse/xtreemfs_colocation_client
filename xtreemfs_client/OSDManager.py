import os
import pickle
import subprocess
from urllib import request
import urllib.error
import shutil
import time
import datetime
import random

from xtreemfs_client import dataDistribution
from xtreemfs_client import div_util
from xtreemfs_client import folder
from xtreemfs_client import dirstatuspageparser
from xtreemfs_client import physicalPlacementRealizer

'''
xOSDManager - a python module to manage OSD selection in XtreemFS
currently only depth (level) 2 subdirectories can be managed
only unix-based OSs are supported
'''

max_processes_change_policy = 200
max_processes_add_replica = 200
max_processes_delete_replica = 200


class OSDManager(object):
    # TODO add support for arbitrary subdirectory level
    # (currently depth=2 is hardcoded, which is fine for GeoMultiSens purposes)
    def __init__(self, path_to_managed_folder, config_file='.das_config', value_map=None, debug=False):

        self.managed_folder = path_to_managed_folder
        self.config_file = config_file
        self.debug = debug

        if value_map is None:

            if not div_util.check_for_executable('xtfsutil'):
                raise ExecutableNotFoundException("No xtfsutil found. Please make sure it is contained in your PATH.")

            output_1 = subprocess.run(["xtfsutil", self.managed_folder], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                      universal_newlines=True)

            if output_1.stderr.startswith("xtfsutil failed: Path doesn't point to an entity on an XtreemFS volume!"):
                raise NotAXtreemFSVolume("The specified folder '" + path_to_managed_folder +
                                         "' is not part of an XtreemFS volume!")

            if len(output_1.stderr) > 0:
                raise Exception("xtfsutil produced some error: " + output_1.stderr)

            self.path_on_volume = div_util.remove_leading_trailing_slashes(
                str(output_1.stdout).split("\n")[0].split()[-1])
            self.path_to_mount_point = self.managed_folder[0:(len(self.managed_folder) - len(self.path_on_volume) - 1)]

            output_2 = subprocess.run(["xtfsutil", self.path_to_mount_point],
                                      stdout=subprocess.PIPE, universal_newlines=True)

            self.volume_information = div_util.extract_volume_information(output_2.stdout)
            self.volume_name = self.volume_information[0]
            osd_list = list(map(lambda x: x[0], self.volume_information[1]))
            self.osd_selection_policy = self.volume_information[2]
            self.volume_address = self.volume_information[3]

            self.distribution = None
            if not self.__read_configuration():
                self.distribution = dataDistribution.DataDistribution()

            self.distribution.add_osd_list(osd_list)

            self.osd_information = None

            try:
                answer = request.urlopen(div_util.get_http_address(self.volume_address))
                html_data = answer.read().decode('UTF-8')

                parser = dirstatuspageparser.DIRStatusPageParser()
                parser.feed(html_data)

                # filter out data sets without last update time or wrong service type
                filtered_data_sets = list(filter(lambda x: int(x['last updated'].split()[0]) != 0, parser.dataSets))
                filtered_data_sets = list(filter(lambda x: x['type'] == 'SERVICE_TYPE_OSD', filtered_data_sets))

                self.osd_information = {}

                for data_set in filtered_data_sets:
                    uuid = data_set['uuid']
                    current_osd = {}
                    current_osd['usable_space'] = int(data_set['usable'].split()[0])
                    current_osd['total_space'] = int(data_set['total'].split()[0])

                    self.osd_information[uuid] = current_osd

            except urllib.error.URLError as error:
                print("osd information could not be fetched! Probably the http status page could not be found at:",
                      div_util.get_http_address(self.volume_address))
                print(error)
        else:
            try:
                self.path_on_volume = value_map['path_on_volume']
                self.path_to_mount_point = value_map['path_to_mount']
                self.volume_name = value_map['volume_name']
                self.osd_selection_policy = value_map['osd_selection_policy']
                self.distribution = value_map['data_distribution']
                self.volume_address = value_map['volume_address']
                self.osd_information = value_map['osd_information']
            except KeyError as error:
                print('key not found:', error)
                print('leaving in OSDManager field empty!')

    def __read_configuration(self):
        assert self.distribution is None
        path_to_config = os.path.join(self.managed_folder, self.config_file)
        try:
            f = open(path_to_config, "rb")
            self.distribution = pickle.load(f)
            return True
        except IOError:
            return False

    def __write_configuration(self):
        path_to_config = os.path.join(self.managed_folder, self.config_file)
        f = open(path_to_config, "wb")
        pickle.dump(self.distribution, f)

    def create_distribution_from_existing_files(self,
                                                fix_layout_internally=True, max_files_in_progress=10000,
                                                apply_layout=True,
                                                environment='LOCAL',
                                                movement_strategy='osd_balanced'):
        """
        create a good data distribution out of data already present in the file system.
        the created data distribution will then be transferred to the physical layer,
        i.e., all files will be moved to their corresponding OSD,
        using XtreemFS' read-only replication strategy.
        """
        start_time = time.time()

        if self.debug:
            print("creating distribution from existing files. osd manager: " + str(self))

        if not div_util.check_for_executable('du'):
            raise ExecutableNotFoundException("No du found. Please make sure it is contained in your PATH.")

        existing_folders = self.get_depth_2_subdirectories()
        new_folders = []
        for one_folder in existing_folders:
            du = subprocess.run(["du", "-s", one_folder], stdout=subprocess.PIPE,
                                universal_newlines=True)
            folder_size = int(du.stdout.split()[0])
            if folder_size == 0:
                folder_size = 1
            new_folder = folder.Folder(self.get_path_on_volume(one_folder),
                                       folder_size,
                                       None)
            new_folders.append(new_folder)

        new_assignments = self.distribution.add_folders(new_folders, debug=self.debug)

        if apply_layout:
            self.apply_osd_assignments(new_assignments)
        elif self.debug:
            print("NOT applying data layout!")
        if apply_layout:
            self.__write_configuration()

        if self.debug:
            print("osd manager after new folders have been added to data distribution:")
            print(str(self))

        if self.debug:
            total_time = round(time.time() - start_time)
            print("calculated distribution on existing files in secs: " + str(total_time))

        start_time = time.time()

        if fix_layout_internally:
            placement_realizer = \
                physicalPlacementRealizer.PhysicalPlacementRealizer(self, debug=self.debug,
                                                                    max_files_in_progress=max_files_in_progress)
            placement_realizer.realize_placement(strategy=movement_strategy)
        else:
            if environment == 'SLURM':
                osd_list = self.distribution.get_osd_list()
                osd_to_folders_map = {}
                all_folders = []
                for osd in osd_list:
                    osd_to_folders_map[osd] = []
                    for osds_folder in self.distribution.OSDs[osd].folders:
                        all_folders.append(osds_folder)
                for input_folder in all_folders:
                    osd_for_tile = self.distribution.get_containing_osd(input_folder).uuid
                    osd_to_folders_map[osd_for_tile].append(input_folder)

                move_commands = self.__generate_move_commands_slurm(osd_to_folders_map)
                self.__execute_commands(move_commands)
            else:
                self.fix_physical_layout_externally()

        if self.debug:
            total_time = round(time.time() - start_time)
            print("fixed physical layout of existing files in secs: " + str(total_time))

    def rebalance_existing_assignment(self,
                                      rebalance_algorithm='lpt',
                                      fix_layout_internally=True, max_files_in_progress=10000,
                                      environment='LOCAL',
                                      movement_strategy='osd_balanced'):
        if self.debug:
            print("rebalancing existing distribution... osd manager: \n" + str(self))

        start_time = time.time()
        self.update()
        update_time = round(time.time() - start_time)
        if self.debug:
            print("updated folder sizes in secs: " + str(update_time))

        start_time = time.time()

        if rebalance_algorithm is 'rebalance_one':
            movements = self.distribution.rebalance_one_folder()
        elif rebalance_algorithm is 'two_step_opt':
            movements = self.distribution.rebalance_two_steps_optimal_matching()
        elif rebalance_algorithm is 'two_step_rnd':
            movements = self.distribution.rebalance_two_steps_random_matching()
        else:
            movements = self.distribution.rebalance_lpt()

        if self.debug:
            rebalance_time = round(time.time() - start_time)
            print("rebalanced assignment in secs: " + str(rebalance_time))

            print("movements:")
            print(str(movements))

            total_movement_size = 0
            for folder_id in movements:
                total_movement_size += self.distribution.get_folder_size(folder_id)

            print("total movement size: " + str(total_movement_size))

            print("rebalanced osd manager: \n" + str(self))

        new_assignments = list(map(lambda item : (item[0], item[1][1]), list(movements.items())))
        if self.debug:
            print("new assignments: " + str(new_assignments))
        self.apply_osd_assignments(new_assignments)

        start_time = time.time()

        if fix_layout_internally:
            # TODO use movements to make realize_placement more efficient
            # TODO (for the first calculation of files that need to be moved)
            placement_realizer = \
                physicalPlacementRealizer.PhysicalPlacementRealizer(self, debug=self.debug,
                                                                    max_files_in_progress=max_files_in_progress)
            placement_realizer.realize_placement(strategy=movement_strategy)

        elif environment == 'SLURM':
            target_balanced = 1  # 0 is origin balanced, 1 is target balanced
            # we use self.__generate_move_commands_slurm() to do so.
            # this either ignores the origin or the target information.
            # TODO can we do this more intelligently?
            osd_to_folders_map = {}
            for folder_to_move in movements.keys():
                # we have a choice: origin or target balanced.
                osd_id = movements[folder_to_move][target_balanced]
                if osd_id in osd_to_folders_map:
                    osd_to_folders_map[osd_id].append(folder_to_move)
                else:
                    osd_to_folders_map[osd_id] = [folder_to_move]

            move_commands = self.__generate_move_commands_slurm(osd_to_folders_map)
            self.__execute_commands(move_commands)

        else:
            self.fix_physical_layout_externally()

        self.__write_configuration()

        if self.debug:
            total_time = round(time.time() - start_time)
            print("fixed physical layout of existing files in secs: " + str(total_time))

    def fix_physical_layout_externally(self):
        """
        fixes the physical layout, such that it matches the data distribution described in self.distribution.
        this is realized by calling move_folder_to_osd on all folders managed by this distribution.
        """
        if self.debug:
            print("fixing physical layout externally...")
        managed_folders = self.get_assigned_folder_ids()
        for folder_id in managed_folders:
            osd_for_folder = self.distribution.get_containing_osd(folder_id)
            self.move_folder_to_osd(folder_id, osd_for_folder.uuid)

    def create_empty_folders(self, folders):
        """
        create empty folders and assign OSDs.
        """
        average_size = int(self.distribution.get_average_folder_size())
        if average_size <= 0:
            average_size = 1

        tiles = []

        for input_folder in folders:
            new_tile = folder.Folder(self.get_path_on_volume(input_folder), average_size, None)
            tiles.append(new_tile)

        new_tiles = self.distribution.add_folders(tiles)

        self.apply_osd_assignments(new_tiles)

        for input_folder in folders:
            os.makedirs(input_folder, exist_ok=True)

        self.__write_configuration()

    def copy_folders(self, folders, environment='LOCAL', remote_source=None, sshfs_mount_dir='/tmp/sshfs_tmp_mnt',
                     apply_layout=True, execute_copy=True, random_osd_assignment=False, random_seed=None):
        """
        copy a list of given folders into the managed folder, assigning OSDs to new folders and updating
        self.dataDistribution
        """
        if self.debug:
            print("calling copy_folders with:")
            print("folders: " + str(folders))
            print("environment: " + str(environment))
            print("remote_source: " + str(remote_source))
            print("sshfs_mount_dir: " + str(sshfs_mount_dir))
            print("apply_layout: " + str(apply_layout))
            print("execute_copy: " + str(execute_copy))
            print("random_osd_assignemnt: " + str(random_osd_assignment))

        if not div_util.check_for_executable('du'):
            raise ExecutableNotFoundException("No du found. Please make sure it is contained in your PATH.")

        if remote_source is not None:
            if not div_util.check_for_executable('sshfs'):
                raise ExecutableNotFoundException("No sshfs found. Please make sure it is contained in your PATH.")
            if not div_util.check_for_executable('scp'):
                raise ExecutableNotFoundException("No scp found. Please make sure it is contained in your PATH.")
            if not div_util.check_for_executable('fusermount'):
                raise ExecutableNotFoundException("No fusermount found. Please make sure it is contained in your PATH.")

        if remote_source is not None:
            os.makedirs(sshfs_mount_dir, exist_ok=True)

        new_folders = []

        for input_folder in folders:
            last_2_path_elements = os.path.join(os.path.split(os.path.split(input_folder)[0])[1],
                                                os.path.split(input_folder)[1])
            if remote_source is not None:
                mount_point = os.path.join(sshfs_mount_dir, last_2_path_elements)
                os.makedirs(mount_point, exist_ok=True)
                subprocess.run(["sshfs", remote_source + ":" + input_folder, mount_point])
                du = subprocess.run(["du", "-s", mount_point], stdout=subprocess.PIPE,
                                    universal_newlines=True)
                subprocess.run(["fusermount", "-uz", mount_point])
                shutil.rmtree(mount_point)
                folder_size = int(du.stdout.split()[0])
            else:
                du = subprocess.run(["du", "-s", input_folder], stdout=subprocess.PIPE,
                                    universal_newlines=True)
                folder_size = int(du.stdout.split()[0])

            # as the folder_id is generated from the copy source, we cannot call get_path_on_volume to get the foler_id
            new_folder = folder.Folder(os.path.join(self.volume_name, self.path_on_volume, last_2_path_elements),
                                       folder_size,
                                       input_folder)
            if self.debug:
                print("new folder: " + str(new_folder))

            new_folders.append(new_folder)

        if remote_source is not None:
            shutil.rmtree(sshfs_mount_dir)
        if self.debug:
            print("OSDManager: random_osd_assignment: " + str(random_osd_assignment))

        new_assignments = self.distribution.add_folders(new_folders, random_osd_assignment=random_osd_assignment,
                                                        random_seed=random_seed)
        if apply_layout:
            self.apply_osd_assignments(new_assignments)
        elif self.debug:
            print("NOT applying data layout!")

        if apply_layout:
            self.__write_configuration()

        if self.debug:
            print("osd manager after new folders have been added to data distribution:")
            print(str(self))

        if execute_copy:
            self.__copy_data(new_folders, environment, remote_source)

    def __generate_move_commands_slurm(self, osd_to_folders_map, tmp_dir=None):
        if self.debug:
            print("Using SLURM mode for moving folders...")

        if tmp_dir is None:
            tmp_dir = os.path.join(self.path_to_mount_point, '.tmp_move_folder')
        os.makedirs(tmp_dir, exist_ok=True)

        slurm_hosts = div_util.get_slurm_hosts()

        if self.debug:
            print('slurm_hosts: ', slurm_hosts)

        osd_to_host_map = div_util.get_osd_to_hostname_map(self.volume_information[1], slurm_hosts)

        if self.debug:
            print('osd_to_host_map: ', osd_to_host_map)

        command_list = []

        host_name = ""
        for key in osd_to_folders_map.keys():
            if host_name == "":
                host_name = osd_to_host_map[key]
            command = ""
            for move_folder in osd_to_folders_map[key]:
                folder_path = self.get_absolute_file_path(move_folder)
                folder_tmp_path = os.path.join(tmp_dir, os.path.split(move_folder)[1])

                # move folder to temporary location
                command += "srun -N1-1 --nodelist=" + host_name
                command += " mv " + folder_path + " " + tmp_dir + " ; "
                # copy folder back from temporary location to initial location
                command += "srun -N1-1 --nodelist=" + host_name
                command += " cp -r " + folder_tmp_path + " " + os.path.split(folder_path)[0] + " ; "
                # delete folder from temporary location
                command += "srun -N1-1 --nodelist=" + host_name
                command += " rm -r " + folder_tmp_path + " ; "

            if len(command) > 0:
                command_list.append(command)

        return command_list

    def move_folder_to_osd(self, folder_id: str, new_osd_id: str, tmp_dir=None):
        """
        moves a folder from one OSD to another OSD. you may specify a temporary folder.
        """
        folder_path = os.path.join(self.get_target_dir(folder_id),
                                   os.path.split(folder_id)[1])

        if tmp_dir is None:
            tmp_dir = os.path.join(self.path_to_mount_point, '.tmp_move_folder')

        start_time = 0
        if self.debug:
            start_time = time.time()

        if self.debug:
            print("externally moving folder " + folder_id + " to osd: " + new_osd_id)

        os.makedirs(tmp_dir, exist_ok=True)

        if not div_util.check_for_executable('xtfsutil'):
            raise ExecutableNotFoundException("No xtfsutil found. Please make sure it is contained in your PATH.")
        # step 1: add folder to new OSD, update data distribution and xtreemfs configuration
        self.distribution.assign_new_osd(folder_id, new_osd_id)
        if self.debug:
            subprocess.run(["xtfsutil",
                            "--set-pattr", "1004.filenamePrefix", "--value",
                            "add " + folder_id + " " + new_osd_id + "", self.path_to_mount_point])
        else:
            subprocess.run(["xtfsutil",
                            "--set-pattr", "1004.filenamePrefix", "--value",
                            "add " + folder_id + " " + new_osd_id + "", self.path_to_mount_point],
                           stdout=subprocess.PIPE, universal_newlines=True)

        # step 2: one by one, move files to tmp_location and then back to the folder, which means that they should now
        # be located onto the new OSD.

        for root, dirs, files in os.walk(folder_path):
            for file in files:
                current_file_path = os.path.join(root, file)
                copied_file_path = os.path.join(tmp_dir, file)
                shutil.move(current_file_path, copied_file_path)
                # os.remove(current_file_path)
                shutil.copy(copied_file_path, os.path.split(current_file_path)[0])
                os.remove(copied_file_path)

        shutil.rmtree(tmp_dir, ignore_errors=True)

        if self.debug:
            total_time = time.time() - start_time
            print("externally moved folder " + folder_id +
                  " to osd: " + new_osd_id + " in secs: " + str(round(total_time)))

    def remove_folder(self, folder_id):
        """
        removes a folder from the distribution. this does NOT delete the folder from the file system.
        """
        containing_osd = self.distribution.get_containing_osd(folder_id)
        if containing_osd is not None:
            if not div_util.check_for_executable('xtfsutil'):
                raise ExecutableNotFoundException("No xtfsutil found. Please make sure it is contained in your PATH.")

            containing_osd.remove_folder(folder_id)
            if self.debug:
                subprocess.run(["xtfsutil",
                                "--set-pattr", "1004.filenamePrefix", "--value",
                                "remove " + folder_id + "", self.path_to_mount_point])
            else:
                subprocess.run(["xtfsutil",
                                "--set-pattr", "1004.filenamePrefix", "--value",
                                "remove " + folder_id + "", self.path_to_mount_point],
                               stdout=subprocess.PIPE, universal_newlines=True)

    def update(self, arg_folders=None):
        """
        update the given (by absolute path) folders, such that the values held by self.dataDistribution
        matches their size on disk.
        if no argument is given, all folders are updated.
        """
        if arg_folders is not None:
            for folder_for_update in arg_folders:
                if not folder_for_update.startswith(self.managed_folder):
                    raise PathNotManagedException(
                        "The path :" + folder_for_update + "is not managed by this instance of the XtreemFS OSD"
                                                           "manager!")

        folder_size_updates = {}

        folders = arg_folders
        if arg_folders is None:
            folders = self.get_depth_2_subdirectories()

        for folder_for_update in folders:
            folder_id = self.get_path_on_volume(folder_for_update)
            command = ["du", "-s", folder_for_update]
            if self.debug:
                print("executing: " + str(command))
            du = subprocess.run(command, stdout=subprocess.PIPE, universal_newlines=True)
            folder_disk_size = int(du.stdout.split()[0])
            folder_size_updates[folder_id] = folder_disk_size

        for folder_for_update, size in folder_size_updates.items():
            self.distribution.update_folder(folder_for_update, size)

        self.__write_configuration()

        if self.debug:
            print(str(self))

    def apply_osd_assignments(self, assignments):
        """
        apply the given assignments to the XtreemFS volume, using xtfsutil.
        the assignments are given as a list containing tuples (tile_id, osd),
        where tile_id is given by applying path_on_volume() onto the absolute path of the folder.
        """
        if not div_util.check_for_executable('xtfsutil'):
            raise ExecutableNotFoundException("No xtfsutil found. Please make sure it is contained in your PATH.")

        if self.osd_selection_policy is not "1000,1004":
            if self.debug:
                subprocess.run(["xtfsutil", "--set-osp", "prefix", self.path_to_mount_point])
            else:
                subprocess.run(["xtfsutil", "--set-osp", "prefix", self.path_to_mount_point],
                               stdout=subprocess.PIPE, universal_newlines=True)

        for new_tile in assignments:
            if self.debug:
                subprocess.run(["xtfsutil",
                                "--set-pattr", "1004.filenamePrefix", "--value",
                                "add " + new_tile[0] + " " + new_tile[1] + "",
                                self.path_to_mount_point])
            else:
                subprocess.run(["xtfsutil",
                                "--set-pattr", "1004.filenamePrefix", "--value",
                                "add " + new_tile[0] + " " + new_tile[1] + "",
                                self.path_to_mount_point],
                               stdout=subprocess.PIPE, universal_newlines=True)

    def __copy_data(self, input_folders, environment, remote_source):
        """
        copy data onto XtreemFS volume
        """
        if self.debug:
            print('calling copy_data with: ', input_folders, environment, remote_source)
        osd_list = self.distribution.get_osd_list()
        osd_to_folders_map = {}
        for osd in osd_list:
            osd_to_folders_map[osd] = []
        for input_folder in input_folders:
            osd_for_tile = self.distribution.get_containing_osd(input_folder.id).uuid
            osd_to_folders_map[osd_for_tile].append(input_folder)

        if self.debug:
            print("osd to folders map:")
            for key, value in osd_to_folders_map.items():
                print("osd: " + key)
                print("assigned folders:")
                for input_folder in value:
                    print(str(input_folder))

        # trigger the copying!
        if environment == "SLURM":
            assert remote_source is not None
            copy_commands = self.__generate_copy_commands_slurm(osd_to_folders_map, remote_source)
        elif environment == "HU_CLUSTER":
            copy_commands = self.__generate_copy_commands_hu_cluster(osd_to_folders_map)
        else:
            copy_commands = self.__generate_copy_commands_local(osd_to_folders_map)

        for input_folder in input_folders:
            path_to_target_dir = self.get_target_dir(input_folder.id)
            os.makedirs(path_to_target_dir, exist_ok=True)

        self.__execute_commands(copy_commands)

    def __generate_copy_commands_slurm(self, osd_to_folders_map, remote_source):
        """
        generates a list of copy commands, one command for each OSD that receives new data.
        the copy commands are constructed such that they can be executed in a slurm environment (that is, within a slurm job
        allocation) at ZIB.
        each command is a copy command including all new folders for the corresponding OSD, preceded by a srun command
        to execute the copy command locally on the slurm node on which the target OSD resides.
        """
        if self.debug:
            print("Using SLURM mode for copying...")

        slurm_hosts = div_util.get_slurm_hosts()

        if self.debug:
            print('slurm_hosts: ', slurm_hosts)

        osd_to_host_map = div_util.get_osd_to_hostname_map(self.volume_information[1], slurm_hosts)

        if self.debug:
            print('osd_to_host_map: ', osd_to_host_map)

        command_list = []

        host_name = ""
        # command = ""
        for key in osd_to_folders_map.keys():
            if host_name == "":
                host_name = osd_to_host_map[key]
            command = ""
            for copy_folder in osd_to_folders_map[key]:
                command += "srun -N1-1 --nodelist=" + host_name
                command += " scp -rq " + remote_source + ":" + copy_folder.origin
                command += " " + self.get_target_dir(copy_folder.id)
                command += " ;"
            if len(osd_to_folders_map[key]) > 0:
                command_list.append(command)
        # command_list.append(command)
        return command_list

    def __generate_copy_commands_hu_cluster(self, osd_to_folders_map):
        """
        generates a list of copy commands, one command for each OSD that receives new data.
        the copy commands are constructed such that they can be executed on the GeoMultiSens cluster at HU Berlin.
        each command is a copy command including all new folders for the corresponding OSD, preceded by a ssh command
        to execute the copy command locally on the node of the target OSD.
        """
        if self.debug:
            print("Using HU_CLUSTER mode for copying...")

        if not div_util.check_for_executable('xtfsutil'):
            raise ExecutableNotFoundException("No xtfsutil found. Please make sure it is contained in your PATH.")

        xtfsutil = subprocess.run(["xtfsutil", self.path_to_mount_point],
                                  stdout=subprocess.PIPE, universal_newlines=True)
        volume_information = div_util.extract_volume_information(xtfsutil.stdout)
        osd_to_ip_address = {}
        for (osd, ip) in volume_information[1]:
            osd_to_ip_address[osd] = ip

        command_list = []

        for key in osd_to_folders_map.keys():
            ip_address = osd_to_ip_address[key]
            command = "ssh " + ip_address + " \'"
            for copy_folder in osd_to_folders_map[key]:
                command += " cp -r"
                command += " " + copy_folder.origin
                command += " " + self.get_target_dir(copy_folder.id)
                command += " ;"
            command += " \' "
            if len(osd_to_folders_map[key]) > 0:
                command_list.append(command)

        return command_list

    def __generate_copy_commands_local(self, osd_to_folders_map):
        """
        generates a list of copy commands, one command for each OSD that receives new data.
        plain old cp is used for the actual copying.
        """
        if self.debug:
            print("Using local cp for copying...")

        command_list = []

        for key in osd_to_folders_map.keys():
            command = ""
            for copy_folder in osd_to_folders_map[key]:
                command += "cp -r "
                command += copy_folder.origin
                command += " " + self.get_target_dir(copy_folder.id)
                command += " ; "

            if len(osd_to_folders_map[key]) > 0:
                command_list.append(command)

        return command_list

    def __execute_commands(self, command_list):
        """
        execute, in parallel, a given set of commands. note that the degree of parallelism will match the length of
        command_list.
        """
        if self.debug:
            print("Executing commands: ")
            for command in command_list:
                print(str(command))
            print("in total " + str(len(command_list)) + " commands.")
        processes = []
        for command in command_list:
            process = subprocess.Popen(command, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True)
            time.sleep(5)
            processes.append(process)

        for p in processes:
            p.wait()

        if self.debug:
            for terminated_process in processes:
                print(str(terminated_process.communicate()))
            print("Executing commands done.")

    def get_depth_2_subdirectories(self):
        """
        creates a list of all depth 2 subdirectories of self.managed_folder
        """
        subdirectories = []
        for depth_1_folder in os.listdir(self.managed_folder):
            depth_1_path = os.path.join(self.managed_folder, depth_1_folder)
            if os.path.isdir(depth_1_path):
                for depth_2_folder in os.listdir(depth_1_path):
                    depth_2_path = os.path.join(self.managed_folder, depth_1_folder, depth_2_folder)
                    if os.path.isdir(depth_2_path):
                        subdirectories.append(depth_2_path)

        return subdirectories

    def get_assigned_folder_ids(self):
        """
        creates a list of ids of all assigned folders (folders assigned to OSDs)
        """
        osd_list = self.distribution.get_osd_list()
        assigned_folders = []
        for osd in osd_list:
            for one_folder in self.distribution.OSDs[osd].folders:
                assigned_folders.append(one_folder)
        return assigned_folders

    def get_target_dir(self, folder_id):
        """
        gets the path to the target dir (where to copy the folder), given the folder_id
        """
        return os.path.split(self.get_absolute_file_path(folder_id))[0]

    def get_path_on_volume(self, path):
        """
        remove the leading part of the path, such that only the part onto the xtreemfs volume remains, including
        the volume itself.
        throws an exception when the path is not managed by this XtreemFS OSD manager.
        use this method to calculate the folder_id.
        """
        if not path.startswith(self.managed_folder):
            raise PathNotManagedException("Path " + path + " is not managed by this instance of the XtreemFS OSD"
                                                           "manager!")
        return os.path.join(self.volume_name, path[len(self.path_to_mount_point) + 1:])

    def get_absolute_file_path(self, folder_id):
        return os.path.join(self.path_to_mount_point, folder_id[len(self.volume_name) + 1:])

    def get_containing_folder_id(self, path_on_volume):
        """
        search for the assigned folder that is a prefix of the given path on volume
        """
        for osd in self.distribution.OSDs.values():
            for a_folder in osd.folders:
                if path_on_volume.startswith(a_folder):
                    return a_folder
        return None

    def __str__(self):
        representation = "pathToMountPoint: " + self.path_to_mount_point + " volumeName: " + self.volume_name + " pathOnVolume: " \
                         + self.path_on_volume
        representation += ("\nconfigFile: " + self.config_file + "\n")
        representation += self.distribution.description()
        return representation


class ExecutableNotFoundException(Exception):
    """raise this when an external executable can not be found"""


class NotAXtreemFSVolume(Exception):
    """raise this when a path does not point to a folder on a xtreemfs volume"""


class PathNotManagedException(Exception):
    """raise this when a path is handled, that is not managed by xOSDManager"""
