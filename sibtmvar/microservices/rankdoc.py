import pandas as pd
import urllib.request as ur
import json
import sys
import math

from sibtmvar.microservices import configuration as conf
from sibtmvar.microservices import cache
from sibtmvar.microservices import esquerybuilder as qb
from sibtmvar.microservices import essearch as es
from sibtmvar.microservices import merging as me
from sibtmvar.microservices import cleaning as cl
from sibtmvar.microservices import documentparser as dp
from sibtmvar.microservices import scoring as sc
from sibtmvar.microservices import ct

class RankDoc:
    '''
    The RankDoc search for a list of ranked documents for a given query

    Parameters
    ----------
    query: Query
        a normalized query object
    collection: str
        a collection name (medline, pmc..)
    conf_mode: str
        indicate which configuration file should be used (default: prod)
    conf_file: Configuration
        indicate a Configuration object to use (default: None)

    Attributes
    ----------
    documents_df
       documents listed in a dataframe with a set of scores associated
    conf_file: Configuration
        indicate a Configuration object to use (default: None)
    errors: list
        stores a list of errors with a json format

    '''
    def __init__(self, query, collection, conf_file=None, conf_mode="prod"):
        ''' The constructor stores the user request'''

        # Initialize a variable to store errors
        self.errors = []

        # Load configuration file
        self.conf_file = conf_file
        if conf_file is None:
            self.conf_file = conf.Configuration(conf_mode)
            # Cache error handling
            self.errors += self.conf_file.errors

        # Store user request
        self.query = query
        self.collection = collection


    def process(self, tuning=False):
        ''' Execute the query to retrieve the ranked list of documents'''

        # Initialize variable to store documents
        documents_per_query = []

        # For clinical trial, call the CT webservice
        if self.collection == "ct":
            self.searchCt()

        # For literature
        else:

            # Get documents for each subquery
            for subquery in self.query.gen_vars_norm:
                gene, variant = subquery
                documents = self.searchLit(self.query.disease_norm, gene, variant)
                documents_per_query.append(documents)

            # Merge documents
            self.merge(documents_per_query)

            # Clean documents
            self.clean()

            # Rank documents
            if not tuning:
                self.rank()

    def searchLit(self, disease, gene, variant):
        ''' Search for document for a triplet '''

        # Add possible queries
        queries = [("exact", disease, gene, variant)]

        # Relaxed queries if any
        if not (disease is None or gene is None or variant is None or len(disease) == 0 or len(gene) == 0 or variant['query_term'] == 'none'):
            if not self.conf_file.settings['settings_user']['mandatory_disease']:
                queries.append(("gv", None, gene, variant))
            if not self.conf_file.settings['settings_user']['mandatory_gene']:
                queries.append(("dv", disease, None, variant))
            if not self.conf_file.settings['settings_user']['mandatory_variant']:
                queries.append(("dg", disease, gene, None))

        # Initialize documents
        documents = {}

        # Perform each query
        for query in queries:

            query_type, this_disease, this_gene, this_variant = query

            # Define ES query
            es_builder = qb.ESQueryBuilder(self.collection, conf_file=self.conf_file)
            query_json = es_builder.buildQuery(disease=this_disease, gene=this_gene, variant=this_variant)
            self.errors += es_builder.errors

            # Execute query
            es_search = es.EsSearch(conf_file=self.conf_file)
            output = es_search.executeQuery(query_json, self.collection)

            # In case of errors, re-query, without the highlight
            if output == {}:
                es_builder = qb.ESQueryBuilder(self.collection, conf_file=self.conf_file)
                query_json = es_builder.buildQuery(disease=this_disease, gene=this_gene, variant=this_variant, highlight=False)
                output = es_search.executeQuery(query_json, self.collection)

            # Parse query
            if 'hits' in output:

                for document_json in output['hits']['hits']:
                    doc_id = document_json["_id"]

                    #Update document id for pmc
                    if self.collection == "pmc":
                        if document_json["_source"]["pmid"] != "":
                            doc_id = document_json["_source"]["pmid"]
                        #TODO: retrieve pmid when not available in ES

                    # If document already present, get it back
                    if doc_id in documents:
                        document_parsed = documents[doc_id]
                        documents[doc_id] = document_parsed

                    # Otherwise, parse the document
                    else:
                        document_parsed = dp.DocumentParser(doc_id, self.collection, conf_file=self.conf_file)
                        document_parsed.fetchEs(document_json)

                    # Add the score and snippets
                    document_parsed.addScore(query_type, document_json["_score"], output['hits']['hits'][0]["_score"])
                    if "highlight" in document_json:
                        document_parsed.addSnippets(document_json['highlight'])

                    # Store again the document
                    documents[doc_id] = document_parsed

                    # handle errors
                    self.errors += document_parsed.errors

            # Store search errors
            self.errors += es_search.errors

        return documents

    def merge(self, documents_per_query):
        ''' Merge documents for each gene-variant couple and for each collection '''

        # Compute the merging
        merging = me.DocumentsMerging(documents_per_query, self.query.separator, conf_file=self.conf_file)

        # Get the merged dataframe
        self.documents_df = merging.documents_df
        self.errors += merging.errors
        #pd.set_option("display.max_rows", None, "display.max_columns", None)
        #print(self.init_documents_df)


    def clean(self):
        ''' Clean documents to remove unmatched documents (e.g. *) '''

        # If there is at least a document, rank the list
        if len(self.documents_df) > 0:

            # Compute the cleaning
            cleaning_function = cl.DocumentsCleaning(self.documents_df, conf_file=self.conf_file)
            cleaning_function.compute(self.query)

            # Get the dataframe with final documents
            self.documents_df = cleaning_function.documents_df


    def rank(self):

        # If there is at least a document, rank the list
        if len(self.documents_df) > 0:

            # Compute the scoring function
            scoring_function = sc.DocumentsScoring(self.documents_df, conf_file=self.conf_file)
            scoring_function.compute(self.query)
            self.errors += scoring_function.errors

            # Get the dataframe with final scores
            self.documents_df = scoring_function.documents_df

    def searchCtWS(self):
        ''' Retrieve clinical trials using CT webservice '''

        # Build query url
        url = self.conf_file.settings['url']['ct']

        # Force variant
        query = "mustV=true"

        # Add demographics
        if hasattr(self.query, "age_txt") and self.query.age_txt != "none":
            query += "&age=" + self.query.age_txt
        if hasattr(self.query, "gender_txt") and self.query.gender_txt != "none":
            query += "&gender=" + self.query.gender_txt

        # Add disease
        if hasattr(self.query, "disease_txt") and self.query.disease_txt != "none":
            # TODO: deal with more than one disease
            query += "&disease=" + self.query.disease_norm[0]['id']

        # Add genvars
        if hasattr(self.query, "gen_vars_txt") and self.query.gen_vars_txt != "none":
            gen_vars_txt = self.query.gen_vars_txt
            gen_vars_txt = gen_vars_txt.replace(" or ", " OR ")
            gen_vars_txt = gen_vars_txt.replace(" and ", " AND ")
            gen_vars_txt = gen_vars_txt.replace(" (", "(")
            query += "&genvars=" + gen_vars_txt

        # Define the cache
        ct_cache = cache.Cache("ct", query, "json", conf_file=self.conf_file)
        ct_json = {}

        # Perform query if not in cache
        if not ct_cache.isInCache():

            full_url = url + "?" + query.replace(" ", "%20")

            # Query CT service
            try:

                # Run query
                connection = ur.urlopen(full_url)

                # Retrieve answer as json
                ct_json = json.load(connection)

                # Store in cache
                ct_cache.storeToCache(json.dumps(ct_json))

            # Catch error with CT service
            except:

                self.errors.append({"level": "warning", "service": "ct", "description": "Clinical trials service failed",
                                    "details": full_url+" = "+str(sys.exc_info()[0])})

        # Reload CT results if in cache
        else:
            ct_json = ct_cache.loadFromCache()


        # Catch cache errors
        self.errors += ct_cache.errors

        # Initialize a list for storing clinical trials
        documents = []

        # Parse and store clinical trials
        if 'clinical_trials' in ct_json:

            # For each clinical trial
            for document_json in ct_json['clinical_trials']:

                # Create a document and fetch its content
                document_parsed = dp.DocumentParser(document_json["NCTid"], self.collection, conf_file=self.conf_file)
                document_parsed.addScore("exact", document_json["score"], ct_json['clinical_trials'][0]["score"])
                document_parsed.fetchMongo()

                # Store the document
                documents.append([document_parsed.doc_id, document_parsed, document_parsed.elastic_scores['exact']])

                # handle errors
                self.errors += document_parsed.errors


        # Store in a dataframe
        self.documents_df = pd.DataFrame(documents, columns=['identifier', 'document', 'final_score'])
        self.documents_df.set_index("identifier", inplace=True)
        #pd.set_option("display.max_rows", None, "display.max_columns", None)
        #print(self.init_documents_df)

    def searchCt(self):
        ''' Retrieve clinical trials using CT webservice '''

        # Add demographics
        age = "none"
        if hasattr(self.query, "age_txt") and self.query.age_txt != "none":
           age = self.query.age_txt
        gender = "all"
        if hasattr(self.query, "gender_txt") and self.query.gender_txt != "none":
           gender = self.query.gender_txt

        # Add disease
        disease = "none"
        if hasattr(self.query, "disease_txt") and self.query.disease_txt != "none":
            # TODO: deal with more than one disease
            disease = self.query.disease_norm[0]['id']

        # Add genvars
        gen_vars = []
        if hasattr(self.query, "gen_vars_txt") and self.query.gen_vars_txt != "none":
            for element in self.query.gen_vars_norm:
                genes, variant = element
                for gene in genes:
                    gen_vars.append(gene['id']+"("+variant['query_term']+")")
        if self.query.separator == "and":
            gen_var = ';'.join(gen_vars)
        else:
            gen_var = ':'.join(gen_vars)

        # Retrieve Elasticsearch CT information
        elasticsearch_host = self.conf_file.settings['elasticsearch']['url_ct']
        elasticsearch_port = self.conf_file.settings['elasticsearch']['port_ct']
        elasticsearch_index = self.conf_file.settings['settings_system']['es_index_ct']

        try:
            ct_str = ct.rankCT(gen_var, disease, gender, age, "yes", elasticsearch_host, elasticsearch_port, elasticsearch_index)
            ct_json = json.loads(ct_str)
        except:
            ct_json = {}
            self.errors.append({"level": "warning", "service": "ct", "description": "Service crashed", "details": "none"})

        # Initialize a list for storing clinical trials
        documents = []

        # Parse and store clinical trials
        if 'clinical_trials' in ct_json:

            # For each clinical trial
            for document_json in ct_json['clinical_trials']:
                # Create a document and fetch its content
                document_parsed = dp.DocumentParser(document_json["NCTid"], self.collection, conf_file=self.conf_file)
                document_parsed.addScore("exact", document_json["score"], ct_json['clinical_trials'][0]["score"])
                document_parsed.fetchMongo()

                if "passage_variant" in document_json:
                    document_parsed.addSnippetsCT(document_json['passage_variant'])

                # Store the document
                documents.append([document_parsed.doc_id, document_parsed, document_parsed.elastic_scores['exact']])

                # handle errors
                self.errors += document_parsed.errors

        # Store in a dataframe
        self.documents_df = pd.DataFrame(documents, columns=['identifier', 'document', 'final_score'])
        self.documents_df.set_index("identifier", inplace=True)
        # pd.set_option("display.max_rows", None, "display.max_columns", None)
        # print(self.init_documents_df)

    def getJson(self):
        ''' Return ranking as a json'''

        # Initialize the json
        documents_json = []

        # For each document
        rank = 1
        for _, row in self.documents_df.iterrows():

            # Get the final score
            score = row['final_score']

            # If score is null, set it to 0
            if math.isnan(score):
                score = 0.0

            # Push the final score to the document
            document = row['document']
            document.setFinalScore(score)

            # Push the rank to the document
            document.setRank(rank)
            rank += 1

            # If the document is not yet highlighted, do it
            if not hasattr(document, "stats"):
                document.setHighlightedEntities(self.query.getHlEntities())
                document.processDocument()

            # Get the document as Json with highlights
            document.generateJson()
            documents_json.append(document.getJson())

            self.errors += document.errors

        # Build json
        return documents_json

    def getScore(self):
        ''' Generate a score for the query '''

        nb_doc = 0
        sum_scores = 0

        # For each document
        for _, row in self.documents_df.iterrows():
            nb_doc += 1
            sum_scores += row['final_score']

        return (nb_doc, sum_scores)
