import re

import pandas as pd

from sibtmvar.microservices import configuration as conf


class DocumentsFilling:
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
        ''' Fill the scores for each document '''

        # Compute relaxed score
        if 'relax' in self.conf_file.settings['settings_ranking']['strategies']:
            columns = ['dg', 'dv', 'gv']

            if not 'relax' in self.documents_df:

                # Normalize parents columns
                for column in columns:
                    if self.documents_df[column].max() != 0.0:
                        self.documents_df[column] = self.documents_df[column] / self.documents_df[column].max()

        # Compute annotation score
        if 'annot' in self.conf_file.settings['settings_ranking']['strategies']:

             # Fill annotations density per entity_type
             columns = ['drugs', 'diseases', 'genes']

             if not 'annot' in self.documents_df:
                 for column in columns:
                     self.documents_df[column] = self.documents_df.apply(lambda row: self.fillAnnotations(row, column, query), axis=1)

                     # Normalize annotations density
                     self.documents_df[column] = self.normalize(self.documents_df[column])

        # Compute demographic score
        if 'demog' in self.conf_file.settings['settings_ranking']['strategies']:

            # Fill age and gender score
            columns = ['age', 'gender']

            if not 'demog' in self.documents_df:
                for column in columns:
                    self.documents_df[column] = self.documents_df.apply(lambda row: self.fillDemographics(row, column, query), axis=1)

                    # Normalize demographic bonus
                    self.documents_df[column] = self.normalize(self.documents_df[column])



        # Decrease score of non English documents
        self.documents_df['language'] = self.documents_df.apply(lambda row: self.defineLanguage(row, query), axis=1)


    def normalize(self, serie):
        ''' Normalize a pandas serie to have the best score set at 1.0 '''

        if serie.max() != 0.0:
            serie = serie / serie.max()

        return serie



    def fill(self, parsed_document, query):
        ''' Search for details if needed'''

        # If not yet processed, fetch the document information
        if not hasattr(parsed_document, "stats"):
            parsed_document.setHighlightedEntities(query.getHlEntities())
            parsed_document.processDocument()
            self.errors += parsed_document.errors


    def fillAnnotations(self, row, annotation_type, query):
        ''' Complete the dataframe with the number of annotations per annotation type '''

        # Get the document of the row
        parsed_document = row['document']

        # If details are missing:
        self.fill(parsed_document, query)

        # If found in mongodb, return the number of annotations
        if hasattr(parsed_document, "stats"):
            # Return the number of annotations for this annotation type
            if hasattr(parsed_document.stats.details['facet_details'], annotation_type):
                return len(parsed_document.stats.details['facet_details'][annotation_type])

        # Otherwise, return 0
        else:
            return 0

    def fillDemographics(self, row, demographic_type, query):
        ''' Complete the dataframe with the bonus score per demographic type '''

        # Get the document of the row
        parsed_document = row['document']

        # If details are missing:
        self.fill(parsed_document, query)

        # If found in mongodb, return the number of annotations
        if hasattr(parsed_document, "stats"):

            if hasattr(parsed_document.stats.details['query_details'], 'query_'+demographic_type):
                demographic_value = parsed_document.stats.details['query_details']['query_'+demographic_type]

                if "same" in demographic_value:
                    return (self.conf_file.settings['settings_ranking']['match_'+demographic_type+'_bonus'])

                elif "not discussed" in demographic_value:
                    return (self.conf_file.settings['settings_ranking']['undiscussed_'+demographic_type+'_bonus'])

        # If not match, return 0
        return 0


    def defineLanguage(self, row, query):
        ''' Returns true if the article is in English, false either '''

        # Get the document of the row
        parsed_document = row['document']

        # If details are missing:
        self.fill(parsed_document, query)

        # Get the title
        if ("title" in parsed_document.requested_fields):
            title = parsed_document.requested_fields['title']

            # If the title is surrounded by [], it is not in English
            if re.match(r"^\[.*\].$", title):
                return False
            else:
                return True

        else:
            return True

