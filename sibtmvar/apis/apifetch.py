from sibtmvar.apis import apiservices as api
from sibtmvar.microservices import configuration as conf
from sibtmvar.microservices import documentparser as dp
from sibtmvar.microservices import cache
from sibtmvar.microservices import query as qu

def fetchDoc(request, conf_file=None, conf_mode="prod"):
    ''' Retrieves a set of documents, highlighted with a set of disease, gene, variants'''

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
        api.logQuery(request, "fetchdoc", conf_file, ip_address)

    # Settings
    conf_file = api.processSettingsParameters(conf_file, request)

    # If the result is available in cache and the user accepts to use cache
    api_cache = cache.Cache("fetchdoc", request.url, "json", conf_file=conf_file)
    if api_cache.isInCache():

        # Reload the cache file
        output = api_cache.loadFromCache()

        # handle errors
        errors += api_cache.errors

    # If not in cache or cache failed
    if output is None:

        # Process all the parameters
        unique_id = api.processIdParameters(request)
        disease_txt, gen_vars_txt, gender_txt, age_txt = api.processCaseParameters(request)
        pub_ids, collection = api.processFetchParameters(request)

        # Normalize the query
        query = qu.Query(conf_file=conf_file)
        query.setPubIds(pub_ids)
        query.setCollection(collection)
        query.setDisease(disease_txt)
        query.setGenVars(gen_vars_txt)
        query.setGender(gender_txt)
        query.setAge(age_txt)
        query.normalize()

        # Initialize the json output
        output = {}
        output['unique_id'] = unique_id

        # Add the query to the output
        output['query'] = query.getInitQuery()

        # Add the normalized query to the output
        output['normalized_query'] = query.getNormQuery()

        # Handle query errors
        errors += query.errors

        # Get entities to highlight
        hl_entities = query.getHlEntities()

        # Initialize the publication json part
        output['publications'] = []

        # Fetch each document
        for pub_id in pub_ids.split(";"):

            # Fetch the document and highlight entities
            document = dp.DocumentParser(pub_id, collection, conf_file=conf_file)
            document.setHighlightedEntities(hl_entities)
            document.fetchMongo()
            document.processDocument()
            document.generateJson()
            doc_json = document.getJson()

            # If found, add it to the json
            if doc_json != {}:
                output['publications'].append(doc_json)

            # Handle fetchdoc errors
            errors += document.errors

        # Report errors in the json (norm, fetch)
        output['errors'] = []
        for error in errors:
            if error not in output['errors']:
                output['errors'].append(error)

    # Display the output for the user
    return (api.buildOutput(output, conf_file, errors, api_cache))
