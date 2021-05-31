import hashlib
import os.path
from pathlib import Path
import time
import json

from sibtmvar.microservices import configuration as conf

class Cache:
    '''
    The Cache object manages the existence of a usable cache file and stores content in a cache file

    Parameters
    ----------
    service_type : str
        service_type describe a service that can be cached (e.g. synvar)
    query : str
        query is the full query sent to the service (e.g. a json elasticsearch query)
    file_type: str
        the extension of the cache file (default: txt)
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
    service_type: str
        service_type describe a service that can be cached (e.g. synvar)
    file_name: str
        an absolute file name for the cache file
    '''

    def __init__(self, service_type, query, file_type="txt", conf_file=None, conf_name="prod"):
        ''' The constructor stores the service_type and defines a unique file name for the query '''

        # Initiate errors
        self.errors = []

        # Initiate configuration file
        self.conf_file = conf_file
        if conf_file is None:
            self.conf_file = conf.Configuration(conf_name)
            # Cache error handling
            self.errors += self.conf_file.errors

        # Store service type in an instance variable
        self.service_type = service_type

        # Generate the file name for the cache file
        self.file_name = self.generateFileName(file_type, query)


    def generateFileName(self, file_type, query):
        ''' Define the file name for the cache file, create the repository to store the file and return the absolute file name '''

        # Transform the query in a unique hashed key
        if len(query) > 20:
            query = hashlib.sha224(query.encode(encoding='UTF-8', errors='strict')).hexdigest()

        # If special characters, remove
        invalid = '<>:"/\|?* '
        for char in invalid:
            query = query.replace(char, '')

        # Create the directory for the service if it does not exist
        repository = self.conf_file.settings['repository']['cache'] + self.service_type + "/"
        Path(repository).mkdir(parents=True, exist_ok=True)

        # Define a file name for the cache file: cache_repository/service_type/key_name.file_type
        return repository + query + "." + file_type

    def isInCache(self, time_limit=True):
        ''' Return true if the service should use the cache system and a recent cache file exist for this query, return false otherwise. '''

        # Check if the cache system is activated for the requested service (according to the activation status defined in the config file)
        if self.conf_file.settings['cache']['is_activated_'+self.service_type]:

            # Check if the user agrees to use cache (or for synvar, use it anyway)
            if self.conf_file.settings['settings_user']['cache'] or self.service_type == "synvar":

                # Check if the file exist
                if os.path.exists(self.file_name):

                    # If a time limit has been defined
                    if time_limit:

                        # Get time of last modification
                        last_modified = os.path.getmtime(self.file_name)

                        # Check if last modification is recent (according to the number of days defined in the config file)
                        if time.time() - last_modified < self.conf_file.settings['cache']['saved_days_' + self.service_type] * 86400:
                            return True

                    else:
                        return True

        # If the service is not activated, the file does not exist or the file is too old
        return False

    def loadFromCache(self):
        ''' Read a file from cache '''

        file_content = None

        # Up to 5 attemps to load the file in case it is not yet ready
        for i in range(5):

            # Reload the cache file
            try:
                with open(self.file_name, encoding="utf-8") as file:

                    # Json files
                    if ".json" in self.file_name:
                        file_content = json.load(file)

                    # Other files
                    else:
                        file_content = file.read()

                    return file_content


            # Store errors if failed to load
            except IOError:
                time.sleep(3)

        self.errors.append({"level": "warning", "service":"cache", "description": "Cache file loading failed", "details":self.file_name})

        return file_content

    def storeToCache(self, file_content):
        ''' Print the file content in the cache file '''

        # Check if the cache system is activated for the requested service (according to the activation status defined in the config file)
        if self.conf_file.settings['cache']['is_activated_' + self.service_type]:

            # Check if the user agrees to use cache (or for synvar, use it anyway)
            if self.conf_file.settings['settings_user']['cache'] or self.service_type == "synvar":

                try:

                    # Create file, with UTF-8 encoding
                    f_out = open(self.file_name, 'w', encoding="utf-8")

                    # Print in the file
                    f_out.write(file_content)

                    # Close the file
                    f_out.flush()
                    f_out.close()

                # Store errors if failed to write
                except:
                    self.errors.append({"level": "warning", "service":"cache", "description": "Cache file writing failed", "details":self.file_name})
