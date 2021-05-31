import pandas as pd

from sibtmvar.microservices import configuration as conf
from sibtmvar.microservices import rankdoc as rd
from datetime import datetime

class RankVar:
    '''
    The RankVar ranks a set of topics according to the amount of literature

    Parameters
    ----------
    conf_mode: str
        indicate which configuration file should be used (default: prod)
    conf_file: Configuration
        indicate a Configuration object to use (default: None)

    Attributes
    ----------
    topics: list
        a list of topics with a topic number and a topic content as a query
    topics_df: dataframe
       topics listed in a dataframe with a set of scores associated
    conf_file: Configuration
        indicate a Configuration object to use (default: None)
    errors: list
        stores a list of errors with a json format

    '''
    def __init__(self, conf_file=None, conf_mode="prod"):
        ''' The constructor stores the user request'''

        # Initialize a variable to store errors
        self.errors = []

        # Load configuration file
        self.conf_file = conf_file
        if conf_file is None:
            self.conf_file = conf.Configuration(conf_mode)
            # Cache error handling
            self.errors += self.conf_file.errors

        # Initiate variablles
        self.topics = []

    def addTopic(self, topic_nb, topic_query):
        ''' add a variant to the list to rank '''
        self.topics.append([topic_nb, topic_query])

    def process(self, unique_id=None):
        ''' Execute the query to retrieve the ranked list of documents'''

        # Store topics in a dataframe
        self.topics_df = pd.DataFrame(self.topics, columns=['topic_nb', 'topic_query'])

        # Initiate a status file name
        if unique_id is not None:
            status_file = open(self.conf_file.settings['repository']['status'] + unique_id + ".txt", "a+")

        # Search for all topics
        for collection in self.conf_file.settings['settings_user']['collections']:

            # Initialize list of score for each collection
            scores_nb = []
            scores_sum = []
            rankers = []

            # For each topic
            count = 0
            for topic_nb, topic_query in self.topics:
                count += 1

                if unique_id is not None:
                    # Write status
                    now = datetime.now()
                    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
                    status_file.write(date_time + "\tSearching variant " + str(count) + "/" + str(len(self.topics)) + " in "+collection+"\n")
                    status_file.flush()

                # Compute the topic
                ranker = rd.RankDoc(topic_query, collection, conf_file=self.conf_file)
                ranker.process()

                # Store the ranker
                rankers.append(ranker)

                # Handle errors
                self.errors += ranker.errors

                # Get scores
                score_nb, score_sum = ranker.getScore()

                # Store scores
                scores_nb.append(score_nb)
                scores_sum.append(score_sum)

            # Store scores and rankdoc in the dataframe
            self.topics_df[collection+"_nb"] = scores_nb
            self.topics_df[collection+"_sum"] = scores_sum
            self.topics_df[collection+"_ranker"] = rankers

        if unique_id is not None:
            # Close the status file when over
            status_file.close()

        # Calculate a total score (all collections together)
        collections = self.conf_file.settings['settings_user']['collections']
        self.topics_df["total_score"] = self.topics_df.apply(lambda row: self.computeTotal(row, collections), axis=1)

        # Set identifier column as the index
        self.topics_df.set_index("topic_nb", inplace=True)
        pd.set_option('display.max_columns', None)

        # Rank topics by total number of documents
        self.topics_df = self.topics_df.sort_values(by=['total_score'], ascending=False)


    def computeTotal(self, row, collections):
        ''' compute a  total score (sum all documents from all collections)'''

        ids = []

        # For each column to sum
        for collection in collections:

            # Get the ranker of the collection
            ranker = row[collection+"_ranker"]

            # Get the list of ids
            ids += ranker.documents_df.index.values.tolist()

        # Return the score
        return len(list(dict.fromkeys(ids)))


