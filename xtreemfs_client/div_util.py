import subprocess
import sys
import socket
import os
import time


def get_osd_uuids(path):
    xtfsutil = subprocess.run(["xtfsutil", path],
                              stdout=subprocess.PIPE, universal_newlines=True)
    string_elements = xtfsutil.stdout.split('\n')
    osd_list = []
    for splitString in string_elements:
        if splitString.lstrip().startswith("OSD "):
            end_index = splitString.rfind(" ")
            begin_index = splitString.rfind(" ", 0, end_index) + 1
            uuid_substring = splitString[begin_index:end_index]
            osd_list.append(uuid_substring)
    return osd_list


def create_replication_policy_command(absolute_file_path):
    return command_list_to_single_string(["xtfsutil", "-r", "RONLY", absolute_file_path])


def create_create_replica_command(absolute_file_path, new_osd):
    return command_list_to_single_string(["xtfsutil", "-a" + new_osd, "--full", absolute_file_path])


def create_delete_replica_command(absolute_file_path, osd):
    return command_list_to_single_string(["xtfsutil", "-d", osd, absolute_file_path])


def command_list_to_single_string(command_list):
    single_string = ""
    for command in command_list:
        single_string = single_string + command + " "
    return single_string


def print_error(finished_process):
    print('executing process finished with error:')
    print('process: ')
    print(str(finished_process[0]))
    print('stdout:')
    print(str(finished_process[1][0]))
    print('stderr:')
    print(str(finished_process[1][1]))


def run_commands(commands, max_processes=200, print_errors=True):
    """
    execute list of commands in parallel, return list of executions returned with an error
    """
    # running_processes = set()
    running_processes_map = {}
    errored_processes = []
    num_finished = 0
    num_total = len(commands)
    for command in commands:
        started_process = subprocess.Popen(command, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
        # running_processes.add(started_process)
        start_time = time.time()
        running_processes_map[started_process] = start_time
        # if len(running_processes) >= max_processes:
        if len(running_processes_map) >= max_processes:
            os.wait()
            difference = set()
            # for running_process in running_processes:
            for running_process in running_processes_map.keys():
                if running_process.poll() is not None:
                    num_finished += 1
                    difference.add(running_process)
                    finished_process = (running_process.args,
                                        running_process.communicate(),
                                        running_process.returncode)
                    if finished_process[2] != 0:
                        errored_processes.append(finished_process)
                        if print_errors:
                            print_error(finished_process)
                            # print("progress: " + str(num_finished) + "/" + str(num_total))

            # running_processes = running_processes.difference(difference)
            for finished_process in difference:
                del running_processes_map[finished_process]

    timeout_processes = 0
    # for finished_process in running_processes:
    for still_running_process in running_processes_map:
        try:
            stdout, stderr = still_running_process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            print("process timed out: " + str(still_running_process.args))
            still_running_process.kill()
            print("process killed.")
            try:
                stdout, stderr = still_running_process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                timeout_processes += 1
                continue

        still_running_process = (still_running_process.args,
                                 (stdout, stderr),
                                 still_running_process.returncode)
        if still_running_process[2] != 0:
            errored_processes.append(still_running_process)
            if print_errors:
                print_error(still_running_process)

        num_finished += 1
        # print("progress: " + str(num_finished) + "/" + str(num_total))

    if timeout_processes > 0:
        print("number of processes with expired timeout: " + str(timeout_processes))

    return errored_processes
    # return list(map(lambda proc: (proc.args, proc.communicate(), proc.returncode), all_processes))


def print_process_list(processes):
    for process in processes:
        print("command: " + str(process[0]))
        print("stout: " + str(process[1][0]))
        print("stderr: " + str(process[1][1]))
        print("returncode: " + str(process[2]))


def extract_volume_information(string):
    """
    extract volume information from a string which  is output from xtfsutil.
    """
    string_elements = string.split('\n')
    volume_name = ""
    volume_address = ""
    osd_selection_policy = ""
    osd_section = False
    osd_list = []
    for splitString in string_elements:
        if splitString.startswith("XtreemFS URL"):
            url_splits = splitString.split("/")
            volume_name = url_splits[-1]
            volume_address = url_splits[-2]
        if splitString.startswith("OSD Selection p"):
            osd_selection_policy = splitString.split()[-1]
        if splitString.startswith("Selectable OSDs"):
            osd_section = True
        elif not splitString.startswith("   "):
            osd_section = False
        if osd_section:
            end_index = splitString.rfind(" ")
            begin_index = splitString.rfind(" ", 0, end_index) + 1
            uuid_substring = splitString[begin_index:end_index]

            end_index = splitString.rfind(":")
            begin_index = splitString.rfind("(") + 1
            ip_addr_substring = splitString[begin_index:end_index]

            osd_list.append((uuid_substring, ip_addr_substring))
    return volume_name, osd_list, osd_selection_policy, volume_address


def get_http_address(volume_address):
    splits = volume_address.split(':')
    hostname = splits[0]
    port = splits[1]
    ip_addr = socket.gethostbyname(hostname)

    http_port = port[:-4] + '0' + port[-3:]

    return 'http://' + ip_addr + ':' + http_port + '/'


def get_slurm_hosts():
    """
    find out whether we are in a slurm  or not.
    if yes, return list of hostnames.
    otherwise return empty list.
    """
    scontrol = subprocess.run(["which", "scontrol"], stdout=subprocess.PIPE,
                              universal_newlines=True)
    if not scontrol.stdout.endswith("not found"):
        slurm_hosts = subprocess.run(["scontrol", "show", "hostnames"],
                                     stdout=subprocess.PIPE, universal_newlines=True)
        hosts = slurm_hosts.stdout.split('\n')
        hosts = list(filter(None, hosts))
        return hosts
    else:
        return []


def get_osd_to_hostname_map(osds, hosts):
    """
    maps osd uuids to hostnames,
    given a list containing tuples (osdUUID, IPAddr).
    """
    # for each host look up the ip address
    osd_map = {}
    for host in hosts:
        host_output = subprocess.run(["host", host],
                                     stdout=subprocess.PIPE, universal_newlines=True)
        ip_address = host_output.stdout.split()[-1]
        for osd in osds:
            if osd[1] == ip_address:
                osd_map[osd[0]] = host
    return osd_map


def check_for_xtfsutil():
    """
    check whether xtfsutil exists
    """
    xtfsutil = subprocess.run(["xtfsutil"], stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, universal_newlines=True)

    if (not xtfsutil.stdout.startswith("Usage:")) and \
            (not xtfsutil.stderr.startswith("Usage:")):
        print("xtfsutil (the xtreemfs client utility) was not found. " +
              "Please include xtfsutil in your PATH and restart.")
        sys.exit(1)
    else:
        print("Found xtfsutil.")


def check_for_executable(executable):
    """
    check whether the given program exists in $PATH
    """
    try:
        subprocess.run([executable], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError:
        return False
    return True


def remove_leading_trailing_slashes(string):
    """
    remove all leading and trailing slashes (/)
    """
    if len(string) == 0:
        return string
    while string[0] == '/':
        string = string[1:]
        if len(string) == 0:
            return string
    while string[-1] == '/':
        string = string[:-1]
        if len(string) == 0:
            return string
    return string
