import sys


class OSD(object):
    """
    representation of an Object Storage device. the OSD is identified by its uuid.
    it keeps track of the folders saved on the OSD as well as the size of the folders.
    """

    def __init__(self, uuid: str, bandwidth=1, capacity=sys.maxsize):
        if not isinstance(uuid, str):
            raise ValueError("OSD uuid must be str!")
        self.uuid = uuid
        self.bandwidth = bandwidth
        self.capacity = capacity
        self.total_folder_size = 0
        self.folders = {}

    def add_folder(self, folder_id, folder_size):
        assert self.total_folder_size + folder_size <= self.capacity

        if folder_id not in self.folders:
            self.folders[folder_id] = folder_size
        else:
            self.folders[folder_id] += folder_size
        self.total_folder_size += folder_size

    def remove_folder(self, folder):
        if folder in self.folders.keys():
            self.total_folder_size -= self.folders[folder]
            del self.folders[folder]

    def update_folder(self, folder_id, size):
        assert folder_id in self.folders.keys()
        self.remove_folder(folder_id)
        self.add_folder(folder_id, size)

    def contains_folder(self, folder_id):
        return folder_id in self.folders

    def get_smallest_folder(self):
        smallest_id = None
        smallest_size = 0
        for id, size in self.folders.items():
            if smallest_id is None or size < smallest_size:
                smallest_id, smallest_size = id, size

        return smallest_id, smallest_size

    def get_load(self):
        return self.total_folder_size

    def get_free_capacity(self):
        return self.capacity - self.total_folder_size

    def get_processing_time(self):
        return self.total_folder_size / self.bandwidth

    def get_folder_size(self, folder_id):
        assert folder_id in self.folders
        return self.folders[folder_id]

    def __str__(self):
        return "osd: '" + self.uuid \
               + "' totalFolderSize: " + str(self.total_folder_size) \
               + " processing time: " + str(self.get_processing_time()) \
               + " number of folders: " + str(len(self.folders))
