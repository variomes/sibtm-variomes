"""
v.2
Author: D.Caucheteur
SVIP Project
"""

# coding: utf-8
# !/usr/local/bin/python3.8
import re
import json
from elasticsearch import Elasticsearch  # Import Elasticsearch package

#To split paragraph in sentence
def splitParagraphIntoSentences2(paragraph):
    sentenceEnders2 = re.compile(r"""
        # Split sentences on whitespace between them.
        (?:               # Group for two positive lookbehinds.
          (?<=[.!?])      # Either an end of sentence punct,
        | (?<=[.!?]['"])  # or end of sentence punct and quote.
        )                 # End group of two positive lookbehinds.
        (?<!  Mr\.   )    # Don't end sentence on "Mr."
        (?<!  Mrs\.  )    # Don't end sentence on "Mrs."
        (?<!  Ms\.  )    # Don't end sentence on "Ms."
        (?<!  Jr\.   )    # Don't end sentence on "Jr."
        (?<!  Dr\.   )    # Don't end sentence on "Dr."
        (?<!  Prof\. )    # Don't end sentence on "Prof."
        (?<!  Sr\.   )    # Don't end sentence on "Sr."
        (?<!  al\.   )    # Don't end sentence on "al."
        (?<!  \s\w\.   )
        (?<!  Fig\.   )   # Don't end sentence on "Fig."
        (?<!  e.g\.   )    # Don't end sentence on "Mr."
        \s+               # Split on whitespace between sentences.
        """,
          re.IGNORECASE | re.VERBOSE)
    sentenceList = sentenceEnders2.split(paragraph)
    return sentenceList

def rankCT(genvar, disease, gender, age, variant_must, elasticsearch_host="localhost", elasticsearch_port=9201,
               elasticsearch_index="ct_annot_data2019_nov"):

    ####################################################################
    ###################### INITIALISATION JSON ########################
    ####################################################################
    myjson = {}
    myjson2 = {}
    mylistct = {}
    mylistct["genes_variants"] =[]
    mylistct["genes"] = []
    mylistct["variants"] = []
    mylistct["diseases"] = []

    # Query retrieval
    # GENDER #
    gender_norm = gender.lower()
    if gender_norm == "all":
        gender_norm = "All"
    elif gender_norm == "male" or gender_norm == "homme" or gender_norm == "man" or gender_norm == "men" or gender_norm == "m":
        gender_norm = "Male"
    elif gender_norm == "female" or gender_norm == "femme" or gender_norm == "woman" or gender_norm == "women" or gender_norm == "f":
        gender_norm = "Female"

    # AGE #
    regex_age = '[\d+]'
    if re.match(regex_age, age) is not None:
        age_years = str(int(age))
        age_months = str(int(age) * int(12))
        age_days = (str(int(age_months) * int(30)))
    else:
        age_years = "undefined"
        age_months = "undefined"
        age_days = "undefined"

    # DISEASE # (NCIth)
    if disease == "none":
        id_disease = "undefined"
    else:
        regex = '[^c][\d+]'
        if re.match(regex, disease) is not None:
            id_disease = disease

    # Duo Gene-Variant
    #Define the operator...
    regex_duo_and = '[;]'
    regex_duo_or = '[:]'
    list_duo_norm = []
    unique = 0
    operateur = ""
    if re.search(regex_duo_and, genvar) is not None:
        operateur = "and"
        list_duo = genvar.split(";")
    elif re.search(regex_duo_or, genvar) is not None:
        operateur = "or"
        list_duo = genvar.split(":")
    else:
        unique = 1 #no operator, only one duo gene+var
        list_duo = genvar

    if unique == 0:
        var_ok = []
        for duo in list_duo:
            id_gene = duo.split("("[0])
            id_gene = id_gene[0].upper()

            m = re.search(r"\((\w+)\)", duo)
            var = m.group()
            m = re.sub('\(', '', var)
            m = re.sub("\)", '', m)
            var_ok.append(m)

            if operateur == "and":
                couple = id_gene + ";" + m
            elif operateur == "or":
                couple = id_gene + ":" + m

            list_duo_norm.append(couple)
    else:
        id_gene = (list_duo.split("(")[0])
        var = list_duo.split(id_gene)[1]
        m = re.search(r"\((\w+)\)", var)
        m = m.group()
        m = re.sub('\(', '', m)
        m = re.sub("\)", '', m)
        var_ok = [m]
        couple = id_gene + ";" + m
        list_duo_norm.append(couple)

    # >>> CONNEXION A ELASTICSEARCH
    #es = Elasticsearch([{'host': 'localhost', 'port': 9201}])
    es = Elasticsearch([{'host': elasticsearch_host, 'port': elasticsearch_port}])

    # Parameters
    boost_variant_query = 100

    # Query construction

    def buildQuery(gene_norm, id_disease, variant, age_years, age_months, age_days, gender_norm, variant_must):
        query = '{"query": '
        # Conditional bloc if variant in topic #
        if variant_must == "yes":
            query += '  { "bool":{'
            query += '      "should":['
        ## ##############
        query += '  {"bool":{'
        query += '      "must":['

        # condition: gender
        query += '          {"bool":{'
        query += '              "should": ['
        query += '                {"match": {"gender":{"query": ' + "\"" + gender_norm + "\"" + '}}},'
        query += '                {"match": {"gender":{"query": "All"}}}'
        query += '              ]'
        query += '           }'
        query += '           }'

        # condition: age (min and max)
        if age != "none":
            query += ','
            query += '          {"bool":{'
            query += '              "should": ['
            query += '                {"range": {"minimum_age_years":{"lte": ' + age_years + '}}},'
            query += '                {"range": {"minimum_age_months":{"lte": ' + age_months + '}}},'
            query += '                {"range": {"minimum_age_days":{"lte": ' + age_days + '}}},'
            query += '                {"match": {"minimum_age":{"query": "N/A"}}}'
            query += '              ]'
            query += '           }'
            query += '           }'
            #
            query += ','
            query += '          {"bool":{'
            query += '              "should": ['
            query += '                {"range": {"maximum_age_years":{"gte": ' + age_years + '}}},'
            query += '                {"range": {"maximum_age_months":{"gte": ' + age_months + '}}},'
            query += '                {"range": {"maximum_age_days":{"gte": ' + age_days + '}}},'
            query += '                {"match": {"maximum_age":{"query": "N/A"}}}'
            query += '              ]'
            query += '           }'
            query += '           }'

        # condition: gene
        query += ','
        query += '         {"bool":{'
        query += '           "should":['
        query += '              {"multi_match":{'
        query += '                "query": "' + gene_norm + '",'
        query += '                "fields": ['
        query += '                  "brief_title_annotated",'
        query += '                  "official_title_annotated",'
        query += '                  "brief_summary_annotated",'
        query += '                  "detailed_description_annotated",'
        query += '                  "condition_annotated",'
        query += '                  "criteria_annotated",'
        query += '                  "inclusion_criteria_annotated",'
        query += '                  "keywords_annotated"]'
        query += '              }'
        query += '           }'
        query += '               ]'
        query += '           }'
        query += '    }'

        # condition: disease
        if id_disease != "undefined":
            query += ','
            query += '         {"bool":{'
            query += '           "should": ['
            query += '              {"multi_match":{'
            query += '                "query": "' + id_disease + '",'
            query += '                "fields": ['
            query += '                  "brief_title_annotated",'
            query += '                  "official_title_annotated",'
            query += '                  "brief_summary_annotated",'
            query += '                  "detailed_description_annotated",'
            query += '                  "condition_annotated",'
            query += '                  "criteria_annotated",'
            query += '                  "inclusion_criteria_annotated",'
            query += '                  "keywords_annotated"]'
            query += '              }'
            query += '           }'
            query += '               ]'
            query += '           }'
            query += '    }'
        # *************************************************************************************** #
        # condition facultative: variant
        if variant_must == "yes":
            query += ','
            query += '  { "bool":{'
            query += '      "must":['
            # condition:  gender
            query += '          {"bool":{'
            query += '              "should": ['
            query += '                {"match": {"gender":{"query": ' + "\"" + gender_norm + "\"" + '}}},'
            query += '                {"match": {"gender":{"query": "All"}}}'
            query += '              ]'
            query += '           }'
            query += '           }'

            # condition: age (min and max)
            if age != "none":
                query += ','
                query += '          {"bool":{'
                query += '              "should": ['
                query += '                {"range": {"minimum_age_years":{"lte": ' + age_years + '}}},'
                query += '                {"range": {"minimum_age_months":{"lte": ' + age_months + '}}},'
                query += '                {"range": {"minimum_age_days":{"lte": ' + age_days + '}}},'
                query += '                {"match": {"minimum_age":{"query": "N/A"}}}'
                query += '              ]'
                query += '           }'
                query += '           }'
                #
                query += ','
                query += '          {"bool":{'
                query += '              "should": ['
                query += '                {"range": {"maximum_age_years":{"gte": ' + age_years + '}}},'
                query += '                {"range": {"maximum_age_months":{"gte": ' + age_months + '}}},'
                query += '                {"range": {"maximum_age_days":{"gte": ' + age_days + '}}},'
                query += '                {"match": {"maximum_age":{"query": "N/A"}}}'
                query += '              ]'
                query += '           }'
                query += '           }'

            # condition: gene
            query += ','
            query += '         {"bool":{'
            query += '           "should":['
            query += '              {"multi_match":{'
            query += '                "query": "' + gene_norm + '",'
            query += '                "fields": ['
            query += '                  "brief_title_annotated",'
            query += '                  "official_title_annotated",'
            query += '                  "brief_summary_annotated",'
            query += '                  "detailed_description_annotated",'
            query += '                  "condition_annotated",'
            query += '                  "criteria_annotated",'
            query += '                  "inclusion_criteria_annotated",'
            query += '                  "keywords_annotated"]'
            query += '              }'
            query += '           }'
            query += '               ]'
            query += '           }'
            query += '    }'

            # condition: disease
            if id_disease != "undefined":
                query += ','
                query += '         {"bool":{'
                query += '           "should": ['
                query += '              {"multi_match":{'
                query += '                "query": "' + id_disease + '",'
                query += '                "fields": ['
                query += '                  "brief_title_annotated",'
                query += '                  "official_title_annotated",'
                query += '                  "brief_summary_annotated",'
                query += '                  "detailed_description_annotated",'
                query += '                  "condition_annotated",'
                query += '                  "criteria_annotated",'
                query += '                  "inclusion_criteria_annotated",'
                query += '                  "keywords_annotated"]'
                query += '              }'
                query += '           }'
                query += '               ]'
                query += '           }'
                query += '    }'

            # condition: variant
            query += ','
            query += '         {"bool":{'
            query += '           "should": ['
            query += '              {"multi_match":{'
            query += '                "query": "' + variant + '",'
            query += '                "fields": ['
            query += '                  "brief_title",'
            query += '                  "official_title",'
            query += '                  "brief_summary",'
            query += '                  "detailed_description",'
            query += '                  "condition",'
            query += '                  "criteria",'
            query += '                  "inclusion_criteria",'
            query += '                  "keywords"]'
            query += '              }'
            query += '           }'
            query += '               ]'
            query += '           }'
            query += '    }'

            query += '         ]'
            query += ','
            query += '"boost": ' + str(boost_variant_query)
            query += '     }'
            query += '  }'
        # **************************************************************** #
        ## Conditional bloc if disease in url #
        if variant_must == "yes":
            query += ']'
            query += '}'
            query += '}'
        ######################

        query += ']'
        query += '}'
        query += '}'
        query += '}'
        return query

    #Dict. initialisation
    dico_res = {}
    dico_res_alt = {}
    dico_info = {}
    dico_infoG = {}

    # GESTION OPERATEUR couples
    for duo in list_duo_norm:
        if operateur == "or":
            decoupe = duo.split(":")
        else:
            decoupe = duo.split(";")
        gene_norm = decoupe[0]
        variant = decoupe[1]
        query = (buildQuery(gene_norm, id_disease, variant, age_years, age_months, age_days, gender_norm, variant_must))

        query_exec = es.search(index=elasticsearch_index, body=query, size=1000)

        # JSON structures construction #
        mylistct["score"] = ('test')
        myjson2["clinical_trials"] = []

        # At least 1 CT as result
        if query_exec['hits']['hits']:
            for hit in query_exec['hits']['hits']:
                NCTid = (hit["_id"])
                score = (hit["_score"])
                v = hit["_source"]
                list_passage_variant = []

                # Fields treatment to define passage variant
                if variant_must == "yes":
                    #Split les champs concernés par la recherche de variants
                    # List split_official_title
                    split_official_title = splitParagraphIntoSentences2(v['official_title'])
                    #List split_brief_summary
                    split_brief_summary = splitParagraphIntoSentences2(v['brief_summary'])
                    # List split_detailed_description
                    split_detailed_description = splitParagraphIntoSentences2(v['detailed_description'])
                    # List split_condition
                    split_condition = splitParagraphIntoSentences2(v['condition'])
                    # List split_criteria
                    split_criteria = splitParagraphIntoSentences2(v['criteria'])
                    # List split_inclusion_criteria
                    split_inclusion_criteria = splitParagraphIntoSentences2(v['inclusion_criteria'])

                    #Savoir si un ou plusieurs variants:
                    if len(var_ok) == 2:
                        my_var_1 = var_ok[0]
                        my_var_2 = var_ok[1]
                    else:
                        my_var_1 = var_ok[0]

                    try:
                        my_var_2
                    except:
                        my_var_2 = None

                    # Pour chaque passage de chaque champ, je dois regarder si j'ai le variant ou les variants de la query
                    if my_var_1 in v['brief_title']:
                        d1 = {'section':'brief_title', 'text': v['brief_title'], 'var': my_var_1}
                        list_passage_variant.append(d1)
                    if my_var_2 != None:
                        if my_var_2 in v['brief_title']:
                            d1 = {'section': 'brief_title', 'text': v['brief_title'], 'var': my_var_2}
                            list_passage_variant.append(d1)

                    for x in split_official_title:
                        if my_var_1 in x:
                            d1 = {'section': 'official_title', 'text': x, 'var': my_var_1}
                            list_passage_variant.append(d1)
                        if my_var_2 != None:
                            if my_var_2 in x:
                                d1 = {'section': 'official_title', 'text': x, 'var': my_var_2}
                                list_passage_variant.append(d1)

                    for x in split_brief_summary:
                        if my_var_1 in x:
                            d1 = {'section': 'brief_summary', 'text': x, 'var': my_var_1}
                            list_passage_variant.append(d1)
                        if my_var_2 != None:
                            if my_var_2 in x:
                                d1 = {'section': 'brief_summary', 'text': x, 'var': my_var_2}
                                list_passage_variant.append(d1)

                    for x in split_detailed_description:
                        if my_var_1 in x:
                            d1 = {'section': 'detailed_description', 'text': x, 'var': my_var_1}
                            list_passage_variant.append(d1)
                        if my_var_2 != None:
                            if my_var_2 in x:
                                d1 = {'section': 'detailed_description', 'text': x, 'var': my_var_2}
                                list_passage_variant.append(d1)

                    for x in split_condition:
                        if my_var_1 in x:
                            d1 = {'section': 'condition', 'text': x, 'var': my_var_1}
                            list_passage_variant.append(d1)
                        if my_var_2 != None:
                            if my_var_2 in x:
                                d1 = {'section': 'condition', 'text': x, 'var': my_var_2}
                                list_passage_variant.append(d1)

                    for x in split_criteria:
                        if my_var_1 in x:
                            d1 = {'section': 'criteria', 'text': x, 'var': my_var_1}
                            list_passage_variant.append(d1)
                        if my_var_2 != None:
                            if my_var_2 in x:
                                d1 = {'section': 'criteria', 'text': x, 'var': my_var_2}
                                list_passage_variant.append(d1)

                    for x in split_inclusion_criteria:
                        if my_var_1 in x:
                            d1 = {'section': 'inclusion_criteria', 'text': x, 'var': my_var_1}
                            list_passage_variant.append(d1)
                        if my_var_2 != None:
                            if my_var_2 in x:
                                d1 = {'section': 'inclusion_criteria', 'text': x, 'var': my_var_2}
                                list_passage_variant.append(d1)

                    if my_var_1 in v['keywords']:
                        d1 = {'section': 'keywords', 'text': v['keywords'], 'var': my_var_1}
                        list_passage_variant.append(d1)
                    if my_var_2 != None:
                        if my_var_2 in v['keywords']:
                            d1 = {'section': 'keywords', 'text': v['keywords'], 'var': my_var_2}
                            list_passage_variant.append(d1)

                myjson2["clinical_trials"].append({
                    'NCTid': NCTid,
                    'score': score,
                    'start_date': v['start_date'],
                    'completion_date': v['completion_date'],
                    'gender': v['gender'],
                    'minimum_age': v['minimum_age'],
                    'maximum_age': v['maximum_age'],
                    'brief_title': v['brief_title'],
                    'official_title': v['official_title'],
                    'brief_summary': v['brief_summary'],
                    'detailed_description': v['detailed_description'],
                    'condition': v['condition'],
                    'location': v['location'],
                    'criteria': v['criteria'],
                    'criteria_highlight': v['criteria'],
                    'inclusion_criteria': v['inclusion_criteria'],
                    'passage_variant': list_passage_variant,
                    'keywords': v['keywords'],
                })
                dico_infoG[NCTid] = json.dumps(myjson)
                dico_info[NCTid] = json.dumps(myjson2)
                # End of JSON structures construction #
                if unique == 0:
                    if operateur == "or":
                        if NCTid not in dico_res:
                            dico_res[NCTid] = score
                        else:
                            new_score = dico_res[NCTid] + score
                            del dico_res[NCTid]
                            dico_res[NCTid] = new_score
                    elif operateur == "and":
                        if NCTid not in dico_res_alt:
                            dico_res_alt[NCTid] = score
                        else:
                            score_res_alt = dico_res_alt[NCTid]
                            dico_res[NCTid] = score_res_alt + score
                else:
                    dico_res[NCTid] = score

            # Output construction identical no matter operator between queries
            output = dico_infoG[NCTid].rstrip("}")
            output += ("\"clinical_trials\":[")
            parcours = 0
            dico_res_sorted = (sorted(dico_res.items(), key=lambda x: x[1], reverse=True))
            for ct in dico_res_sorted:
                ct_id = ct[0]
                ct_score = ct[1]
                nb_ct = len(dico_res_sorted)
                parcours += 1
                split_info = dico_info[ct_id].split(str(ct_id) + "\",")[1]
                split_info = split_info.split("\"start_date\":")[1]
                split_info_red = split_info.rstrip("}]}")
                add = "{\"NCTid\": \"" + str(ct_id) + "\", " + "\"score\": " + str(
                    ct_score) + ", \"start_date\": " + split_info_red + "]}"
                output += add
                if parcours != nb_ct:
                    output += ","
            output += "]}"

        # No result
        else:
            NCTid = "no results"
            myjson2["clinical_trials"].append({
                'NCTid': NCTid,
            })
            dico_infoG[NCTid] = json.dumps(myjson)
            dico_info[NCTid] = json.dumps(myjson2)

            # Output construction identical no matter operator between queries
            output = dico_infoG[NCTid].rstrip("}")
            output += ("\"clinical_trials\":[")
            parcours = 0
            dico_res_sorted = (sorted(dico_res.items(), key=lambda x: x[1], reverse=True))
            for ct in dico_res_sorted:
                ct_id = ct[0]
                ct_score = ct[1]
                nb_ct = len(dico_res_sorted)
                parcours += 1
                split_info = dico_info[ct_id].split(str(ct_id) + "\",")[1]
                split_info = split_info.split("\"start_date\":")[1]
                split_info_red = split_info.rstrip("}]}")
                add = "{\"NCTid\": \"" + str(ct_id) + "\", " + "\"score\": " + str(
                    ct_score) + ", \"start_date\": " + split_info_red + "]}"
                output += add
                if parcours != nb_ct:
                    output += ","
            output += "]}"

    return (output)


#print(rankCT("nx_p15056(V600E):NX_P15056(V600K)", "C2926", "female", "20", "yes"))
#(rankCT("NX_P15056(V600K)", "none", "female", "20", "yes"))