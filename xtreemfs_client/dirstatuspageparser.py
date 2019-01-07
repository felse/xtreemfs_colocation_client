from html.parser import HTMLParser


class DIRStatusPageParser(HTMLParser):
    def error(self, message):
        pass

    def __init__(self):

        super().__init__()
        self.tableLevel = 0

        # first table describes mappings, second current service states, third service configurations
        self.currentLevelTwoTable = 0

        self.currentLevelTwoData = None

        self.currentValues = {}
        self.currentKey = None

        self.dataSets = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == 'table':
            self.tableLevel += 1
            if self.tableLevel == 2:
                self.currentLevelTwoTable += 1

    def handle_endtag(self, tag):
        if tag.lower() == 'table':
            self.tableLevel -= 1

    def handle_data(self, data):
        stripped_data = data.rstrip().lstrip()
        if stripped_data.rstrip().lstrip() != '':
            if self.tableLevel == 2 and self.currentLevelTwoTable == 2:
                self.currentLevelTwoData = stripped_data
            if self.tableLevel == 3 and self.currentLevelTwoTable == 2:
                if stripped_data == "type":
                    self.currentKey = None
                    self.currentValues = {}
                if self.currentKey is None:
                    self.currentKey = stripped_data
                else:
                    self.currentValues[self.currentKey] = stripped_data
                    if self.currentKey == 'last updated':
                        self.currentValues['uuid'] = self.currentLevelTwoData
                        self.dataSets.append(self.currentValues)
                    self.currentKey = None
