import pandas as pd
import itertools

from sibtmvar.microservices import configuration as conf

class DocumentsMerging:
    '''
    The DocumentsMerging object merges a set of TripletQuery objects and provides a dataframe with the score of each retrieved document per collection

    Parameters
    ----------
    documents_df: dict
        a dictionary containing a document dataframe per collection

    Attributes
    ----------
    documents_df: dict
        a dictionary containing a document dataframe
    conf_file: Configuration
        indicate a Configuration object to use (default: None)

    '''

    def __init__(self, documents_per_query, separator, conf_file=None, conf_mode="prod"):
        ''' Store the queries '''

        # Initialize a variable to store errors
        self.errors = []

        # Load configuration file
        self.conf_file = conf_file
        if conf_file is None:
            self.conf_file = conf.Configuration(conf_mode)
            # Cache error handling
            self.errors += self.conf_file.errors

        # Compute the merging
        self.compute(documents_per_query, separator)


    def compute(self, documents_per_query, separator):
        ''' Merge documents for each gene-variant couple and for each collection '''

        # Get list of all identifiers
        list_identifiers = self.getAllIdentifiers(documents_per_query, separator)

        # Store identifiers in a dataframe
        self.documentsAsDataframe(list_identifiers)

        # Fill the dataframe
        self.fillDocumentsDataframe(documents_per_query)

    def getAllIdentifiers(self, documents_per_query, separator):
        ''' Returns a list of identifiers that are present in all mandatory queries (if no mandatory queries, then return all the identifiers) '''

        # Initiate a list to store identifiers
        list_identifiers = []

        # For each subquery
        for documents in documents_per_query:

            # Build a list of all identifiers returned for a subquery
            ids = documents.keys()

            # Add the ids to the list
            list_identifiers.append(ids)

        # If separator is or, merge both lists
        if separator == "or":
            final_list_identifiers = list(itertools.chain.from_iterable(list_identifiers))

        else:
            final_list_identifiers = list(set.intersection(*map(set, list_identifiers)))

        return final_list_identifiers

    def documentsAsDataframe(self, list_identifiers):
        ''' Build a set of dataframes, containing as index all the possible identifiers. No additional columns '''

        # Create a dataframe
        self.documents_df = pd.DataFrame({"identifier": list_identifiers})

        # Set identifier column as the index
        self.documents_df.set_index("identifier", inplace=True)

    def fillDocumentsDataframe(self, documents_per_query):
        ''' Fill the documents dataframe with scores and document '''

        # Create the columns
        self.documents_df['document'] = pd.Series()
        scores_name = ['exact', 'dg', 'dv', 'gv']
        for score_name in scores_name:
            self.documents_df[score_name] = pd.Series()

        # For each identifier in the dataframe
        for doc_id in self.documents_df.index:

            # Build scores for the row
            scores = {}

            # For each query
            for documents in documents_per_query:

                # If the identifier is present in the results
                if doc_id in documents:

                    # Store the document in the dataframe
                    self.documents_df.at[doc_id, 'document'] = documents[doc_id]

                    # Get score for this query
                    doc_scores = documents[doc_id].elastic_scores

                    # Merge score with scores of other queries
                    for score_name, score_value in doc_scores.items():

                        if score_name in scores:
                            scores[score_name] += score_value
                        else:
                            scores[score_name] = score_value

                    # Add scores in dataframe
                    for score_name in scores_name:
                        if score_name in scores:
                            self.documents_df.at[doc_id, score_name] = scores[score_name]

