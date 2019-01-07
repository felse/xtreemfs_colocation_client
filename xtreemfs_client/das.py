import argparse

import sys

from xtreemfs_client import OSDManager
from xtreemfs_client import verify

"""
das - data add script. basically a command line wrapper for OSDManager.

please use absolute paths only!
"""

parser = argparse.ArgumentParser(description="Copy and manage files in xtreemfs volumes")

parser.add_argument("--copy", "-c", action='store_const', const=True, default=False,
                    help='Copy from the directories (comma-separated) specified by --source-folders to the'
                         ' folder specified by target_folder, which must be onto an XtreemFS volume')

parser.add_argument("--dont-execute-copy", action='store_const', const=True, default=False)

parser.add_argument("--new-folders", action='store_const', const=True, default=False,
                    help='For each subdirectory of each folder specified by  --source-folders, create'
                         ' a corresponding folder in the folder specified by target_folder and'
                         ' assign an OSD to this directory. You should call --update with the same'
                         ' --source-folders and target_folder arguments after copying data'
                         ' manually, e.g., without this tool.')

parser.add_argument("--update", "-u", action='store_const', const=True, default=False,
                    help='Update the data layout such that it matches the physical layout. This is'
                         ' necessary after copying manually, e.g., without this tool.')

parser.add_argument("--source-folders", "-s", nargs=1)
parser.add_argument("target-folder", nargs=1)

parser.add_argument("--remote-source", "-r", nargs=1,
                    help='Use a remote source to copy from. '
                         'The remote host must be reachable via ssh without password.')
parser.add_argument("--random-layout", "-x", action='store_const', const=True, default=False)
parser.add_argument("--random-osd-assignment", action='store_const', const=True, default=False)
parser.add_argument("--random-seed", nargs=1)
parser.add_argument("--environment", "-e", choices=['LOCAL', 'SLURM', 'HU_CLUSTER'], default='LOCAL')

parser.add_argument("--print", "-p", action='store_const', const=True, default=False,
                    help='Print the layout and exit. You need to specify a target folder.')

parser.add_argument("--debug", "-d", action='store_const', const=True, default=False)

parser.add_argument("--verify", "-v", action='store_const', const=True, default=False)

parser.add_argument("--create-from-existing-files", action='store_const', const=True, default=False,
                    help='creates a data distribution based on the files already present,'
                         'and changes the physical location of files to match the locations prescribed by the '
                         'distribution.')

parser.add_argument("--rebalance-existing-assignment", action='store_const', const=True, default=False,
                    help='rebalances an existing osd to folder assignment, using the lpt rebalancing method.')

parser.add_argument("--fix-internally", action='store_const', const=True, default=False,
                    help='indicate whether xtreemfs internal functions should be used to fix the physical'
                         'layout. otherwise files will be temporarily located outside xtreemfs,'
                         'increasing the chance for data loss.')

parser.add_argument("--max-files-in-progress", nargs=1)
parser.add_argument("--movement-strategy", nargs=1)

args = parser.parse_args()

if args.debug:
    print("args: ")
    print(args)

if args.verify:
    good_layout = verify.verify_gms_folder(vars(args)['target-folder'][0])
    print("good_layout: ", good_layout)
    # if not good_layout:
    #     verify.print_tree(vars(args)['target-folder'][0])
    sys.exit(0)

x_man = OSDManager.OSDManager(vars(args)['target-folder'][0], debug=args.debug)

if args.print:
    print(x_man)

if args.copy:
    if len(args.source_folders) == 0:
        print("You must specify folders to copy with --source-folders")
        sys.exit(1)

    else:
        folders = args.source_folders[0].split(',')
        x_man.copy_folders(folders, environment=args.environment, apply_layout=(not args.random_layout),
                           remote_source=args.remote_source[0], random_osd_assignment=args.random_osd_assignment,
                           random_seed=args.random_seed[0], execute_copy=(not args.dont_execute_copy))

elif args.new_folders:
    if len(args.source_folders) == 0:
        print("You must specify folders to create with --source-folders")
        sys.exit(1)

    else:
        folders = args.source_folders[0].split(',')
        x_man.create_empty_folders(folders)

elif args.update:
    x_man.update()
    print(x_man)

elif args.create_from_existing_files:
    x_man.create_distribution_from_existing_files(fix_layout_internally=args.fix_internally,
                                                  environment=args.environment,
                                                  max_files_in_progress=int(args.max_files_in_progress[0]),
                                                  movement_strategy=args.movement_strategy[0])

elif args.rebalance_existing_assignment:
    x_man.rebalance_existing_assignment(fix_layout_internally=args.fix_internally,
                                        environment=args.environment,
                                        max_files_in_progress=int(args.max_files_in_progress[0]),
                                        movement_strategy=args.movement_strategy[0])
