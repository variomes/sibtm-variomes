import os
import time
import copy
from datetime import datetime

from sibtmvar.apis import apiservices as api
from sibtmvar.microservices import configuration as conf
from sibtmvar.microservices import rankvar as rv
from sibtmvar.microservices import cache
from sibtmvar.microservices import query as qu

def rankVar(request, conf_mode="prod", conf_file=None):
    ''' Retrieves a ranked set of documents, highlighted with a set of the query entites'''

    # Initialize the output variable
    output = None
    errors = []

    # Initialize the configuration
    if conf_file is None:
        conf_file = conf.Configuration(conf_mode)
        # Cache error handling
        errors += conf_file.errors

    # Log the query
    ip_address = api.processIpParameters(request)
    if not ('log' in request.args and request.args['log'] == "false"):
        api.logQuery(request, "rankvar", conf_file, ip_address)

    # Settings
    conf_file = api.processSettingsParameters(conf_file, request)

    # Remove the unique id from the query (enable to search for a cached version)
    unique_id = api.processIdParameters(request)
    url = str(request.url)
    url = url.replace("&uniqueId="+unique_id, "")

    # Create the cache variables
    api_cache_url = cache.Cache("rankvar", url, "json", conf_file=conf_file)
    api_cache_id = cache.Cache("rankvar", unique_id, "json", conf_file=conf_file)

    # If the result is available in cache and the user accepts to use cache (cache by id)
    if api_cache_id.isInCache(time_limit=False):
        output = api_cache_id.loadFromCache()

    # If the result is available in cache and the user accepts to use cache (cache by query)
    elif api_cache_url.isInCache():
        output = api_cache_url.loadFromCache()

    # If not yet finished processing (reload)
    elif os.path.isfile(conf_file.settings['repository']['status'] + unique_id + ".txt"):

        # If yes, wait until the cache file is ready (query not yet finished)
        while not api_cache_id.isInCache(time_limit=False):
            time.sleep(10)

        # Reload the cache file
        output = api_cache_id.loadFromCache()

    # Store cache errors
    errors += api_cache_url.errors
    errors += api_cache_id.errors

    # If not processed (or if cache failed)
    if output is None:

        # Create the rankvar object
        rankvar = rv.RankVar(conf_file=conf_file)

        # Process the parameters
        file_name = api.processFileParameters(request)

        # Process all the parameters
        disease_txt, gen_vars_txt, gender_txt, age_txt = api.processCaseParameters(request)

        # Normalize the query
        query = qu.Query(conf_file)
        query.setDisease(disease_txt)
        query.setGender(gender_txt)
        query.setAge(age_txt)

        topics = []

        # If a file is used, then only one case in the parameters (no list of gen-var)
        if file_name != "":

            # Open the api loaded file
            file_name = conf_file.settings['repository']['api_files'] + file_name + ".txt"

            topics = []

            try:
                with open(file_name) as file:
                    for line in file:
                        gene, variant = line.strip().split("\t")
                        topics.append(gene+" ("+variant+")")

            except:
                errors.append({"level": "fatal", "service": "vcf", "description": "VCF file not found", "details": file_name})

        # In case of a text, get the list of topics from the case parameters
        else:
            for topic in gen_vars_txt.split(";"):
                topics.append(topic)

        # Initiate a status file name
        status_file = open(conf_file.settings['repository']['status'] + unique_id + ".txt", "a+")

        # Write status
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        status_file.write(date_time + "\tStart normalizing lines\n")
        status_file.flush()

        # Normalize and store each topic
        for i, gen_vars_txt in enumerate(topics):
            this_query = copy.deepcopy(query)
            this_query.setGenVars(gen_vars_txt)

            # Write status
            now = datetime.now()
            date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
            status_file.write(date_time + "\tNormalizing variant " + str(i) + "/" + str(len(topics)) + "\n")
            status_file.flush()

            rankvar.addTopic(i, this_query)

        # Process the topics
        rankvar.process(unique_id)
        errors += rankvar.errors

        # Write status
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        status_file.write(date_time + "\tPreparing json ")
        status_file.flush()

        # Initialize the json output
        output = {}
        output['unique_id'] = unique_id

        # Add settings to the output
        output['settings'] = api.returnSettingsAsJson(conf_file)

        # Add the data part
        output['data'] = []

        # Add the ranked topics
        for _, row in rankvar.topics_df.iterrows():

            topic_json = {}

            # Add the query to the output
            topic_json['query'] = row['topic_query'].getInitQuery()

            # Add the normalized query to the output
            topic_json['normalized_query'] = row['topic_query'].getNormQuery()

            # Handle query errors
            errors += query.errors

            # Add scores
            for collection in conf_file.settings['settings_user']['collections']:
                topic_json["score_"+collection] = row[collection+"_sum"]
                topic_json["count_"+collection] = row[collection+"_nb"]
            topic_json["total_score"] = row["total_score"]

            # Add the publications if not the light mode with just score
            if not 'light' in request.args:
                topic_json['publications'] = {}

                for collection in conf_file.settings['settings_user']['collections']:

                    # Get the rankdoc
                    ranker = row[collection+"_ranker"]
                    topic_json['publications'][collection] = ranker.getJson()
                    errors += ranker.errors

            # Add the topic to the json
            output['data'].append(topic_json)

        # Report errors in the json (norm, fetch)
        output['errors'] = []
        for error in errors:
            if error not in output['errors']:
                output['errors'].append(error)

    # Update the unique id (useful when the cache file was generated by another user)
    if unique_id is not None:
        output['unique_id'] = unique_id

    # Display the output for the user
    return (api.buildOutput(output, conf_file, errors, api_cache_id, api_cache_url))
