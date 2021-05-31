import os
import sys

class FieldsMapping:
    '''
    The FieldsMapping object manages the mapping between user fields name and mongodb/elasticsearch fields name

    Parameters
    ----------
    collection: str
        the collection of the document


    Attributes
    ----------
    errors: list
        stores a list of errors with a json format
    mapping_from_user: dict
        a mapping from API field names to MongoDB field names
    mapping_to_user: dict
        a mapping from MongoDB field names to API field names

    '''

    def __init__(self, collection):
        ''' The constructor enable to store the conversion and loads the mapping '''

        # Initiate errors
        self.errors = []

        # Initialize mapping dictionaries
        self.mapping_from_user = {}
        self.mapping_to_user = {}

        # Load mappings
        self.loadMapping(collection)

    def loadMapping(self, collection):
        ''' Load field mappings for the collection'''

        # file location
        location = ""
        for possible_location in sys.path:
            if os.path.exists(possible_location + "/sibtmvar/files/mapping_"+collection+".txt"):
                location = possible_location
                break
                # Open mapping
        try:
            with open(location+"/sibtmvar/files/mapping_"+collection+".txt") as f:
                for line in f:
                    (key, val) = line.split()
                    # Store mapping
                    self.mapping_from_user[key] = val
                    self.mapping_to_user[val] = key

        # If file is not found
        except:
            self.errors.append({"level": "warning", "service":"mapping", "description": "Mapping file not found", "details":location+"sibtmvar/files/mapping_"+collection+".txt"})


    def convertFieldFromUserNames(self, field):
        ''' Convert fields name from user names to database names '''

        if field in self.mapping_from_user:
            return self.mapping_from_user[field]
        elif field.replace("_highlight", "") in self.mapping_from_user:
            return self.mapping_from_user[field]+"_highlight"

        # If the field name has no mapping
        return field

    def convertFieldToUserNames(self, field):
        ''' Convert fields name from user names to database names '''

        if field in self.mapping_to_user:
            return self.mapping_to_user[field]
        elif field.replace("_highlight", "") in self.mapping_to_user:
            return self.mapping_to_user[field] + "_highlight"

        # If the field name has no mapping
        return field

    def convertListFromUserNames(self, list):
        ''' Convert a list of fields names from user names to database names '''
        return [self.convertFieldFromUserNames(field) for field in list]

    def convertListToUserNames(self, list):
        ''' Convert a list of fields names from user names to database names '''
        return [self.convertFieldToUserNames(field) for field in list]
