import re

import pandas as pd

from sibtmvar.microservices import configuration as conf


class DocumentsCleaning:
    '''
    The DocumentsScoring object builds a large matrix of scores for each document

    Parameters
    ----------
    documents_df: dict
        a dictionary containing a document dataframe

    Attributes
    ----------
    documents_dataframe: dict
        a dictionary containing a document dataframe
    conf_file: Configuration
        indicate a Configuration object to use (default: None)
    errors: list
        stores a list of errors with a json format

    '''

    def __init__(self, documents_df, conf_file=None, conf_mode="prod"):
        ''' The constructor stores the documents dataframe '''

        # Initialize a variable to store errors
        self.errors = []

        # Load configuration file
        self.conf_file = conf_file
        if conf_file is None:
            self.conf_file = conf.Configuration(conf_mode)
            # Cache error handling
            self.errors += self.conf_file.errors

        # Store parameters as instance variables
        self.documents_df = documents_df

    def compute(self, query):
        '''Check if document really match the query '''

        documents_to_remove = []

        # Check if variant in the query
        for index, row in self.documents_df.iterrows():
            if (row['exact'] > 0 or ('gv' in row and row['gv'] > 0) or ('dv' in row and row['dv'] > 0)):

                # Check if highlight is present
                document = row['document']
                if not hasattr(document, "stats"):
                    document.setHighlightedEntities(query.getHlEntities())
                    document.processDocument()

                valid = False
                for snippet in document.cleaned_snippets:
                    if 'class="variant"' in snippet['text']:
                        valid = True

                # Remove document if not valid
                if valid is False:
                    documents_to_remove.append(index)
                    self.documents_df.drop(index)

        self.documents_df = self.documents_df.drop(documents_to_remove)

