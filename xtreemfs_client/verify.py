import os

from xtreemfs_client import div_util


def verify_tile_folder(tile_folder, verbose):
    """
    verify a tile folder: check whether all files in its subdirectories
    (representing scenes) are located on the same OSD.
    it relies on xtfsutil, so make sure xtfsutil is included in your PATH.
    """
    osd = None
    for folders in os.walk(tile_folder):
        for filename in folders[2]:
            file_path = os.path.join(folders[0], filename)
            osds_for_file = div_util.get_osd_uuids(file_path)
            if len(osds_for_file) > 1:
                print("files in " + tile_folder + " are located on multiple OSDs!")
                return None
            osd_for_file = osds_for_file[0]
            if verbose:
                print("file: " + file_path)
                print("osd of file: " + osd_for_file)
            if osd is None:
                osd = osd_for_file
            else:
                if not osd_for_file == osd:
                    print("files in " + tile_folder + " are located on different OSDs!")
                    return None
    return osd


def verify_gms_folder(gms_folder, verbose=False):
    """
    verify a whole gms folder: gmsFolder should be structured like
    gmsFolder/utmStripes/utmTiles/scenes/files
    """
    layout_is_correct = True
    for utmStripe in os.listdir(gms_folder):
        if not os.path.isdir(gms_folder + "/" + utmStripe):
            continue
        for tile in os.listdir(gms_folder + "/" + utmStripe):
            if not os.path.isdir(gms_folder + "/" + utmStripe + "/" + tile):
                continue
            check = verify_tile_folder(gms_folder + "/" + utmStripe + "/" + tile, verbose)
            if check is None:
                return False
    return layout_is_correct


def print_tree(path):
    """
    print OSD for each file in a given folder
    """
    print("printing OSDs for all files in the following tree: " + path)
    number_of_files = 0
    osd_set = set()
    for directory in os.walk(path):
        for file in directory[2]:
            number_of_files += 1
            file_name = os.path.join(directory[0], file)
            print(file_name)
            osds_of_file = div_util.get_osd_uuids(file_name)
            for osd_of_file in osds_of_file:
                osd_set.add(osd_of_file)
            print(osds_of_file)

    print("number of files: " + str(number_of_files))
    print("OSDs: " + str(osd_set))
