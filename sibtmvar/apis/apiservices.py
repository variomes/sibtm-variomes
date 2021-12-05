from datetime import datetime
import json
import uuid

import requests

def processCaseParameters(request):
    ''' Retrieves parameters specific to a set of topics '''

    # Get query parameters
    disease_txt = "none"
    if 'disease' in request.args and request.args['disease'] != "":
        disease_txt = request.args['disease'].strip()

    gen_vars_txt = "none"
    if 'genvars' in request.args and request.args['genvars'] != "":
        gen_vars_txt = request.args['genvars'].strip()

    gender_txt = "none"
    if 'gender' in request.args and request.args['gender'] != "":
        gender_txt = request.args['gender'].strip()

    age_txt = "none"
    if 'age' in request.args and request.args['age'] != "":
        age_txt = request.args['age'].strip()

    # Return the query
    return disease_txt, gen_vars_txt, gender_txt, age_txt

def processFetchParameters(request):
    ''' Retrieves parameters specific to the Fetching '''

    # Get the collection
    collection = "medline"
    if 'collection' in request.args:
        collection = request.args['collection']
    if 'collections' in request.args:
        collection = request.args['collections']

    # Get the identifiers list
    ids = ""
    if 'ids' in request.args:
        ids = request.args['ids']
    elif 'id' in request.args:
        ids = request.args['id']

    # Return publication ids list and collection name
    return ids, collection

def processIdParameters(request):
    ''' Retrieves the unique identifier or generates one '''

    # Get unique id
    unique_id = str(uuid.uuid1())
    if 'uniqueId' in request.args and request.args['uniqueId'] != "":
        unique_id = request.args['uniqueId']

    return unique_id

def processIpParameters(request):
    ''' Retrieves the IP address or returns None '''

    # Get IP address
    ip_address = None
    if 'ip' in request.args and request.args['ip'] != "":
        ip_address = request.args['ip']

    return ip_address

def processFileParameters(request):
    ''' Retrieves the file name as uploaded by the GUI '''

    # Get unique id
    file_txt = ""
    if 'file' in request.args:
        file_txt = request.args['file']

    return file_txt

def processSettingsParameters(conf_file, request):
    ''' Retrieves the settings defined by the user '''

    # Settings
    if 'minDate' in request.args and request.args['minDate'] != "":
        conf_file.settings['settings_user']['min_date'] = int(request.args['minDate'])

    if 'maxDate' in request.args and request.args['maxDate'] != "":
        conf_file.settings['settings_user']['max_date'] = int(request.args['maxDate'])

    if 'collection' in request.args and request.args['collection'] != "":
        conf_file.settings['settings_user']['collections'] = request.args['collection'].split(",")

    if 'collections' in request.args and request.args['collections'] != "":
        conf_file.settings['settings_user']['collections'] = request.args['collections'].split(",")

    if 'ret_fields' in request.args and request.args['ret_fields'] != "":
        for collection in  conf_file.settings['settings_user']['collections']:
            conf_file.settings['settings_user']['fetch_fields_'+collection] = request.args['ret_fields'].split(",")

    if 'hl_fields' in request.args and request.args['hl_fields'] != "":
        for collection in conf_file.settings['settings_user']['collections']:
            conf_file.settings['settings_user']['hl_fields_'+collection] = request.args['hl_fields'].split(",")

    if 'mustDisease' in request.args and request.args['mustDisease'] != "":
        conf_file.settings['settings_user']['mandatory_disease'] = str2bool(request.args['mustDisease'])

    if 'mustGene' in request.args and request.args['mustGene'] != "":
        conf_file.settings['settings_user']['mandatory_gene'] = str2bool(request.args['mustGene'])

    if 'mustVariant' in request.args and request.args['mustVariant'] != "":
        conf_file.settings['settings_user']['mandatory_variant'] = str2bool(request.args['mustVariant'])

    if 'expandDisease' in request.args and request.args['expandDisease'] != "":
        conf_file.settings['settings_user']['synonym_disease'] = str2bool(request.args['expandDisease'])

    if 'expandGene' in request.args and request.args['expandGene'] != "":
        conf_file.settings['settings_user']['synonym_gene'] = str2bool(request.args['expandGene'])

    if 'expandVariant' in request.args and request.args['expandVariant'] != "":
        conf_file.settings['settings_user']['synonym_variant'] = str2bool(request.args['expandVariant'])

    if 'keywordsPositive' in request.args and request.args['keywordsPositive'] != "":
        conf_file.settings['settings_user']['keywords_positive'] = request.args['keywordsPositive'].split(",")

    if 'keywordsNegative' in request.args and request.args['keywordsNegative'] != "":
        conf_file.settings['settings_user']['keywords_negative'] = request.args['keywordsNegative'].split(",")

    if 'cache' in request.args and request.args['cache'] != "":
        conf_file.settings['settings_user']['cache'] = str2bool(request.args['cache'])

    if 'nb' in request.args and request.args['nb'] != "":
        conf_file.settings['settings_user']['es_results_nb'] = int(request.args['nb'])

    return conf_file

def returnSettingsAsJson(conf_file):
    ''' Returns settings as json '''

    settings_json = {}

    # Settings
    settings_json['min_date'] = conf_file.settings['settings_user']['min_date']
    settings_json['max_date'] = conf_file.settings['settings_user']['max_date']
    settings_json['collections'] = conf_file.settings['settings_user']['collections']
    settings_json['keywords_positive'] = ';'.join(conf_file.settings['settings_user']['keywords_positive'])
    settings_json['keywords_negative'] = ';'.join(conf_file.settings['settings_user']['keywords_negative'])
    settings_json['must_disease'] = conf_file.settings['settings_user']['mandatory_disease']
    settings_json['must_gene'] = conf_file.settings['settings_user']['mandatory_gene']
    settings_json['must_variant'] = conf_file.settings['settings_user']['mandatory_variant']
    settings_json['synonym_disease'] = conf_file.settings['settings_user']['synonym_disease']
    settings_json['synonym_gene'] = conf_file.settings['settings_user']['synonym_gene']
    settings_json['synonym_variant'] = conf_file.settings['settings_user']['synonym_variant']

    return settings_json

def str2bool(v):
    # Convert a string to a boolean
    return str(v).lower() in ("yes", "true", "t", "1")

def buildOutput(output_json, conf_file, errors, cache, secondary_cache=None):
    ''' Returns a json as a string, using UTF-8 encoding '''

    # Check if fatal errors occurred
    for error in errors:
        if error['level'] == "fatal":

            # Get date
            now = datetime.now()

            error = {
                "timestamp": str(now),
                "status": 500,
                "error": "Internal Server Error",
                "message": error['description']+": "+error['details'],
            }

            # Return the error
            return json.dumps(error, ensure_ascii=False)


    # Print it in the cache file
    cache.storeToCache(json.dumps(output_json, ensure_ascii=False))

    # Add eventual errors on cache
    for error in cache.errors:
        output_json['errors'].append(error)

    # If secondary cache
    if secondary_cache is not None:
        secondary_cache.storeToCache(json.dumps(output_json, ensure_ascii=False))

    # Get date
    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

    # Print errors in files
    for error in output_json['errors']:
        f = open(conf_file.settings['repository']['errors'] + error['service']+".txt", "a")
        f.write(date_time+ "\t" + output_json['unique_id'] + "\t" + error['level'] + "\t" + error['description'] + "\t" + error['details'] + "\n")
        f.close()

    # Convert the json to string
    json_string = json.dumps(output_json, ensure_ascii=False)

    # Return the string
    return json_string

def logQuery(user_query, service, conf_file, ip_address=None):
    ''' Stores a query in the log files '''

    # Get the URL of the query
    url = user_query.url
    if ip_address is not None:
        url = url.replace("&ip=" + ip_address, "")

    # Get the time of the query
    time = datetime.now()

    # Get the ip_adress of the query
    if ip_address is None:
        if user_query.headers.getlist("X-Forwarded-For"):
            ip_address = user_query.headers.getlist("X-Forwarded-For")[0]
        else:
            ip_address = user_query.remote_addr

    # Get information (country and city) about the IP address
    try:
        response = requests.get("http://ip-api.com/json/" + ip_address)
        ip_infos = json.loads(response.text)
        if ip_infos['status'] == "success":
            country = ip_infos['country']
            city = ip_infos['city']
        else:
            country = "unknown"
            city = "unknown"
    except:
        country = "unknown"
        city = "unknown"

    # Get the log repository
    log_repository = conf_file.settings['repository']['logs']

    # Open in the file corresponding to the service
    file = open(log_repository + "API_" + service + ".txt", "a+")

    # Write all infos (time, url, ip, etc) in the log file
    file.write(str(time) + "\t" + ip_address + "\t" + city + "\t" + country + "\t" + url + "\n");

    # Close the log file
    file.close()



