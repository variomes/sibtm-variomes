import re

import pandas as pd

from sibtmvar.microservices import configuration as conf


class DocumentsScoring:
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

            # Compute total score
            self.documents_df['relax'] = self.documents_df.apply(lambda row: self.computeTotal(row, columns, "relax"), axis=1)

        # Compute annotation score
        if 'annot' in self.conf_file.settings['settings_ranking']['strategies']:

             # Fill annotations density per entity_type
             columns = ['drugs', 'diseases', 'genes']

             if not 'annot' in self.documents_df:
                 for column in columns:
                     self.documents_df[column] = self.documents_df.apply(lambda row: self.fillAnnotations(row, column, query), axis=1)

                     # Normalize annotations density
                     self.documents_df[column] = self.normalize(self.documents_df[column])

             # Compute total score
             self.documents_df['annot'] = self.documents_df.apply(lambda row: self.computeTotal(row, columns, "annot"), axis=1)

        # Compute demographic score
        if 'demog' in self.conf_file.settings['settings_ranking']['strategies']:

            # Fill age and gender score
            columns = ['age', 'gender']

            if not 'demog' in self.documents_df:
                for column in columns:
                    self.documents_df[column] = self.documents_df.apply(lambda row: self.fillDemographics(row, column, query), axis=1)

                    # Normalize demographic bonus
                    self.documents_df[column] = self.normalize(self.documents_df[column])

            # Compute total score
            self.documents_df['demog'] = self.documents_df.apply(lambda row: self.computeTotal(row, columns, "demog"), axis=1)

        # Compute keywords score
        if 'kw' in self.conf_file.settings['settings_ranking']['strategies']:

            # Fill pos and neg score
            columns = ['pos', 'neg']

            if not 'kw' in self.documents_df:
                for column in columns:
                    self.documents_df[column] = self.documents_df.apply(lambda row: self.fillKeywords(row, column, query), axis=1)

                    # Normalize keywords count
                    self.documents_df[column] = self.normalize(self.documents_df[column])

            # Compute total score
            self.documents_df['kw'] = self.documents_df.apply(lambda row: self.computeTotal(row, columns, "kw"), axis=1)

        # Normalize strategies
        for strategy in self.conf_file.settings['settings_ranking']['strategies']:

            # If there is a score for this strategy
            if strategy in self.documents_df.columns:

                 # Normalize dataframe
                 if self.documents_df[strategy].min() < 0.0:
                     self.documents_df[strategy] = self.documents_df[strategy] - self.documents_df[
                         strategy].min()

                 self.documents_df[strategy] = self.normalize(self.documents_df[strategy])

        # Compute score of all subscores
        self.documents_df['all_score'] = self.documents_df.apply(lambda row: self.computeAllScores(row, self.documents_df.columns), axis=1)

        # Normalize final score
        self.documents_df["all_score"] = self.normalize(self.documents_df["all_score"])

        # Decrease score of non English documents
        self.documents_df['language'] = self.documents_df.apply(lambda row: self.defineLanguage(row, query), axis=1)
        min_score_all = self.documents_df['all_score'].min()
        max_score_not = self.documents_df['all_score'].where(self.documents_df['language'] == False).max()
        self.documents_df['final_score'] = self.documents_df.apply(lambda row: self.penalizeLanguage(row, min_score_all, max_score_not), axis=1)

        # Normalize final score
        self.documents_df["final_score"] = self.normalize(self.documents_df["final_score"])

        # Rank results
        self.documents_df = self.documents_df.sort_values(by=['final_score'], ascending=False)

        pd.set_option('display.max_columns', None)
        #print(self.documents_dataframe)


    def normalize(self, serie):
        ''' Normalize a pandas serie to have the best score set at 1.0 '''

        if serie.max() != 0.0:
            serie = serie / serie.max()

        return serie

    def computeTotal(self, row, columns, descriptor):
        ''' compute a strategy total score (sum some columns according to some weights)'''

        total_score = 0.0

        # For each column to sum
        for column in columns:

            # Get the score of the row
            score = row[column]

            # If not null
            if not pd.isnull(score):
                # Sum of all columns * weight of each column
                total_score += score * self.conf_file.settings['settings_ranking'][descriptor + '_' + column + '_weight']

        # Return the score
        return total_score

    def computeAllScores(self, row, columns):
        ''' compute the final score (sum of all total scores according to some weights)'''

        final_score = 0.0

        # Initialize the score with the exact score
        if not pd.isnull(row['exact']):
            final_score = row['exact']

        # For each strategy to sum
        for strategy in self.conf_file.settings['settings_ranking']['strategies']:

            # If there is a score for this strategy
            if strategy in columns:

                # Get the score for this row
                score = row[strategy]

                # If not null
                if not pd.isnull(score):
                    final_score += row[strategy] * self.conf_file.settings['settings_ranking']['strategy_' + strategy + '_weight']

        # Return the score
        return final_score

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

    def fillKeywords(self, row, keywords_type, query):
        ''' Complete the dataframe with the number of keywords per keywords type '''

        # Get the document of the row
        parsed_document = row['document']

        # If details are missing:
        self.fill(parsed_document, query)

        # Get number of tags for the entity type
        keywords_value = len(re.findall('<span class="kw_' + keywords_type + '"', str(parsed_document.requested_fields)))

        return keywords_value

    def defineLanguage(self, row, query):
        ''' Returns true if the article is in English, false either '''

        # Get the document of the row
        parsed_document = row['document']

        # If details are missing:
        self.fill(parsed_document, query)

        # Get the title
        if hasattr(parsed_document.requested_fields, "title"):
            title = parsed_document.requested_fields['title']

            # If the title is surrounded by [], it is not in English
            if re.match(r"^\[.*\].$", title):
                return False

        else:
            return True

    def penalizeLanguage(self, row, min_score_all, max_score_not):
        ''' Recalculate scores of publications not in english '''

        # For rows not in english
        if not row['language'] and max_score_not != 0:

            # Get the all score
            all_score = row['all_score']

            # Calculate a new score
            new_score = all_score * (min_score_all - (min_score_all / 2)) / max_score_not

            # Returns the new score
            return new_score

        # For rows in english, simply returns the all_score
        else:
            return row['all_score']

