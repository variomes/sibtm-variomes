import os
import time
import copy
from datetime import datetime
import json

from sibtmvar.apis import apiservices as api
from sibtmvar.microservices import configuration as conf
from sibtmvar.microservices import rankvar as rv
from sibtmvar.microservices import cache
from sibtmvar.microservices import query as qu

def getStatus(request, conf_mode="prod", conf_file=None):
    ''' Retrieves a ranked set of documents, highlighted with a set of the query entites'''

    # Initialize the output variable
    output = None
    errors = []

    # Initialize the configuration
    if conf_file is None:
        conf_file = conf.Configuration(conf_mode)

    # Get the unique id
    unique_id = api.processIdParameters(request)


    # Create the cache variables
    api_cache_id = cache.Cache("rankvar", unique_id, "json", conf_file=conf_file)

    # If the result is available in cache and the user accepts to use cache (cache by id)
    if api_cache_id.isInCache(time_limit=False):
        output = "Processing is finished, results will be displayed in a few seconds."

    # If not yet finished processing (get the last line)
    elif os.path.isfile(conf_file.settings['repository']['status'] + unique_id + ".txt"):

        with open(conf_file.settings['repository']['status'] + unique_id + ".txt") as f:
            for line in f:
                pass
            last_line = line
            elements = last_line.split("\t")
            output = elements[1]

    output_json ={ "output": output}

    # Display the output for the user
    return (json.dumps(output_json, ensure_ascii=False))
