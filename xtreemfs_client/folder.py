"""
store important information about a folder
"""


class Folder(object):
    def __init__(self, folder_id, size, origin):
        self.id = folder_id
        self.size = size
        self.origin = origin

    def __str__(self):
        return "folder: '" + self.id \
               + "' size: " + str(self.size) \
               + " origin: " + str(self.origin)
