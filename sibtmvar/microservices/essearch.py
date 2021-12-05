import json
import sys

from elasticsearch import Elasticsearch

from sibtmvar.microservices import configuration as conf
from sibtmvar.microservices import cache

class EsSearch:
    '''
    The EsSearch object execute an elasticsearch query on an elasticsearch search engine and returns the results

    Parameters
    ----------
    conf_mode: str
        indicate which configuration file should be used (default: prod)
    conf_file: Configuration
        indicate a Configuration object to use (default: None)

    Attributes
    ----------
    conf_file: Configuration
        indicate a Configuration object to use (default: None)
    errors: list
        stores a list of errors with a json format

    '''
    def __init__(self, conf_file=None, conf_name="prod"):
        ''' The constructor stores the configuration file '''

        # Initialize a variable to store errors
        self.errors = []

        # Initiate configuration file
        self.conf_file = conf_file
        if conf_file is None:
            self.conf_file = conf.Configuration(conf_name)
            # Cache error handling
            self.errors += self.conf_file.errors

    def executeQuery(self, query, collection):
        ''' Executes a Json query received as a parameter in a ES collection received as a parameter and returns the results as a Json object '''

        # Define the cache
        es_cache = cache.Cache("es", query, "json", conf_file=self.conf_file)
        print(query)

        json_response = None

        # Reload ES results if in cache and cache is allowed
        if es_cache.isInCache():

            # Convert the cache to json
            json_response = es_cache.loadFromCache()


        # Query ES if not present in cache
        if json_response is None:

            try:

                es = Elasticsearch([self.conf_file.settings['elasticsearch']['url']],
                                       http_auth=(self.conf_file.settings['elasticsearch']['username'],
                                                  self.conf_file.settings['elasticsearch']['password']),
                                       port=self.conf_file.settings['elasticsearch']['port'], timeout=500)
                json_response = es.search(index=self.conf_file.settings['settings_system']['es_index_'+collection], body=query,
                                              size=self.conf_file.settings['settings_user']['es_results_nb'])

                # Store in cache
                es_cache.storeToCache(json.dumps(json_response))

                # Store errors
                self.errors += es_cache.errors

            # In case of errors, log it
            except:
                self.errors.append({"level": "warning", "service": "es", "description": "Elasticsearch failed",
                                    "details": str(sys.exc_info()[0])+str(sys.exc_info()[1])})

                json_response = {}

        # Return the json response
        return json_response
