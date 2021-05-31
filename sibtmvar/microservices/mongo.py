import pymongo as mg


class Mongo():
    '''
    The Mongo object connects to the MongoDB database

    Parameters
    ----------
    url: String
        the url of the mongodb server (by default: localhost:27017)

    Attributes
    ----------
    errors: list
        a list of errors encountered by the mongodb service
    client: MongoClient
        a mongodb client
    database
        a mongodb database

    '''

    def __init__(self, url="localhost:27017"):
        ''' Create the mongodb client'''

        # Initialize a variable to store errors
        self.errors = []

        # Connect to mongodb
        self.createClient(url)

    def createClient(self, url):
        ''' Creates the mongodb client '''

        try:
            # Connect to mongodb
            self.client = mg.MongoClient('mongodb://' + url, serverSelectionTimeoutMS=2000)
            self.client.server_info()

        except:
            # If the client creation failed, log the error
            self.client = None
            self.errors.append({"level": "fatal", "service":"mongodb", "description": "MongoDB client not found", "details":url})

    def connectDb(self, database):
        ''' Connect to a specific database '''

        self.database = None

        # If the connexion to the mongodb client worked
        if self.client is not None:

            # Check if the required database exist, if not, log an error
            if not database in self.client.list_database_names():
                self.errors.append({"level": "warning", "service":"mongodb", "description": "Mongodb database not found", "details":database})

            # If it exist, connect to the database
            else:
                self.database = self.client[database]

    def query(self, collection, query):
        ''' Execute a mongodb query in a given collection '''

        # If the connection to the database worked:
        if self.database is not None:

            # Check if the collection exists
            if collection in self.database.collection_names():
                json = self.database[collection].find_one(query)
                return json

            # If the collection does not exist, store the error
            else:
                self.errors.append({"level": "warning", "service":"mongodb", "description": "Mongodb collection not found", "details":collection})

    def closeClient(self):
        ''' Stop the mongodb client '''

        # If the client worked
        if self.client is not None:

            # Close it
            self.client.close()
