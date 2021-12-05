import json
import re
import sys
import os

import pandas as pd

from sibtmvar.microservices import configuration as conf
from sibtmvar.microservices import mongo as mg

class DocStats():
    '''
    The DocStats class returns a set of statistics for entities in a document

    Parameters
    ----------
    doc_id: str
        a document identifier
    collection: str
        the collection of the document
    query_entities: dict
        a set of entities corresponding to the query: [{'type': '', 'id': '', 'main_term': '', query_term': '', all_terms: [''], 'match': 'exact|partial'}] (default: None)
    doc_json : dict
        the document with highlight (default: None)
    conf_mode: str
        indicate which configuration file should be used (default: prod)
    conf_file: Configuration
        indicate a Configuration object to use (default: None)

    Attributes
    ----------
    doc_id: str
        a document identifier
    collection: str
        the collection of the document
    details: dict
        a dictionary of details (facet-based, query-based and ie-based)
    errors: list
        a list of errors encountered by the mongodb service

    '''

    def __init__(self, doc_id, collection,  conf_file=None, conf_mode="prod"):
        ''' The constructor stores the document information to process '''

        # Initialize a variable to store errors
        self.errors = []

        # Load the conf file if not provided
        self.conf_file = conf_file
        if conf_file is None:
            self.conf_file = conf.Configuration(conf_mode)
            # Cache error handling
            self.errors += self.conf_file.errors

        # Store user request
        self.doc_id = doc_id
        self.collection = collection

        # Initialize details variable
        self.details = {}

        # Get each part of details
        self.details['information_extraction'] = self.getMetadataDetails(conf_file)


    def finalizeStats(self, query_entities=None, doc_json=None, snippets_json=None):
        self.details['facet_details'] = self.getFacetsDetails(self.conf_file)
        if query_entities and doc_json is not None and snippets_json is not None:
            self.details['query_details'] = self.getQueryDetails(query_entities, doc_json, snippets_json)

    def getMetadataDetails(self, conf_file):
        ''' Add facets relative to population and clinical trials extractions '''

        # Initialize metadata json section
        metadata_json = {}
        metadata_json['populations'] = []
        metadata_json['clinical_trials'] = []

        # Connect to Mongodb
        mongo = mg.Mongo(conf_file.settings['url']['mongodb'])
        mongo.connectDb(conf_file.settings['settings_system']['client_mongodb_'+self.collection])

        # Query biomed to get document
        mongo_collection = conf_file.settings['settings_system']['mongodb_collection_metadata_' + self.collection]
        mongo_json = mongo.query(mongo_collection, {"_id": self.doc_id})

        # Close MongoDb
        mongo.closeClient()

        # Mongodb error handling
        self.errors += mongo.errors

        # If there is at least one metadata
        if mongo_json is not None:

            # Go through each metadata
            for metadata in mongo_json['metadatas']:

                # Store population metadata
                if metadata['concept_source'] == "Population":
                    metadata_json['populations'].append({'term' : metadata['concept_form']})

                # Store clinical trials metadata
                if metadata['concept_source'] == "NCTid":
                    metadata_json['clinical_trials'].append({'term': metadata['concept_form']})

        # Return metadata
        return metadata_json

    def getFacetsDetails(self, conf_file):
        ''' Add facets relative to annotations '''

        # Initialize facets json section
        facets_json = {}

        # Connect to Mongodb
        mongo = mg.Mongo(conf_file.settings['url']['mongodb'])
        mongo.connectDb(conf_file.settings['settings_system']['client_mongodb_'+self.collection])

        # Query ana to get annotations
        mongo_ana_collection = conf_file.settings['settings_system']['mongodb_collection_ana_' + self.collection]
        ana_json = mongo.query(mongo_ana_collection, {"_id": self.doc_id})

        # Query bib to get mesh terms
        mongo_bib_collection = conf_file.settings['settings_system']['mongodb_collection_bib_' + self.collection]
        bib_json = mongo.query(mongo_bib_collection, {"_id": self.doc_id})

        # Close MongoDb
        mongo.closeClient()

        # Mongodb error handling
        self.errors += mongo.errors

        # If there is at least one annotation
        if ana_json is not None:

            # Transform json to dataframe
            df = pd.DataFrame(list(ana_json['annotations']),columns = ['concept_source', 'type', 'concept_id', 'preferred_term'])
            df['concept_source'] = df['concept_source'].str.lower()

            # Add a count column to the dataframe
            count = [1 for i in df.iterrows()]
            df['sum'] = count

            # Filter to annotations of the requested annotation types
            for facet in ['disease', 'drug',  'gene']:

                # Initialize the json
                facets_json[facet+"s"] = []

                # Get concept source for the annotation type
                concept_source = conf_file.settings['terminology'][facet+"_mongo"].lower()

                # Get sub_df
                sub_df = df[df['concept_source'] == concept_source]

                # Group annotations of the same concept/terminology
                sub_df = sub_df.groupby(['concept_source', 'type', 'concept_id', 'preferred_term'])['sum'].agg('sum')
                sub_df = sub_df.sort_values(ascending=False)

                # For each row of this concept_source
                for index, count in sub_df.items():

                    # Store in the json
                    facets_json[facet+"s"].append({"id": index[2], "preferred_term": index[3],"count": count})

        # Add age and gender facets
        if bib_json is not None:

            # Get mesh terms
            if self.collection == "medline" and 'mesh_terms' in bib_json:
                meshs = bib_json['mesh_terms']

                # For each demographic facet
                for facet in ['age', 'gender']:

                    # Initialize the json
                    facets_json[facet+"_groups"] = []

                    # Get mesh ids corresponding to the demographic facet
                    valid_ids = self.loadValidIds(facet)

                    # For each mesh term, check if it corresponds to this facet
                    for mesh in meshs:
                        concept_id, preferred_term = mesh.split(":")
                        if concept_id in valid_ids:

                            # Store in the json
                            facets_json[facet+"_groups"].append({"id": concept_id, "preferred_term": preferred_term})

        # Return facets
        return facets_json

    def getQueryDetails(self, hl_entities, doc_json, snippets_json):
        ''' Add facets relative to the query '''

        # Get query details for pmc (limited to annotations) or other collections (based on hl)
        if self.collection == 'pmc':
            details_json = self.getQueryDetailsPmc(hl_entities, doc_json, snippets_json)
        else:
            details_json = self.getQueryDetailsAny(hl_entities, doc_json)

        # Return details
        return details_json

    def getQueryDetailsAny(self, hl_entities, doc_json):
        ''' Add facets relative to the query '''

        # Initialize details json section
        details_json = {}

        # Query entity types
        entity_types = ['gene', 'disease', 'variant', 'kw_pos', 'kw_neg']
        demographic_types = ['gender', 'age']

        # Count occurrences per entity type
        for entity_type in entity_types:

            # Initialize the json for this entity type count
            details_json['query_' + entity_type + '_count'] = {}

            # Initialize the total count
            count_all = 0

            # For each field
            for field in doc_json:

                # Treat only highlighted fields
                if "_highlight" in field:

                    # Get number of tags for the entity type
                    count = len(re.findall('<span class="'+entity_type+'"', doc_json[field]))

                    # Store the count
                    details_json['query_' + entity_type + '_count'][field.replace("_highlight", "")] = count

                    # Increase the total count
                    count_all += count

            # Store the total count
            details_json['query_' + entity_type + '_count']['all'] = count_all

        # Check presence per entity type
        for entity_type in entity_types:

            # Initialize the json for this entity type presence
            details_json['query_' + entity_type + '_present'] = {}

            # Get all expected entities
            concept_per_types = [hl_entity['id'] for hl_entity in hl_entities if hl_entity is not None and hl_entity['type'] == entity_type]

            present = []

            # Get json as text
            doc_str = json.dumps(doc_json)

            # Check presence for each expected entity
            for concept_id in concept_per_types:

                # Check presence of the concept id in the document
                if '<span class=\\"' + entity_type + '\\" concept_id=\\"' + concept_id + '\\">' in doc_str:
                    present.append(concept_id)

            # Store presence and absence
            details_json['query_' + entity_type + '_present']['present'] = len(set(present))
            details_json['query_' + entity_type + '_present']['absent'] = len(set(concept_per_types))-len(set(present))
            details_json['query_' + entity_type + '_present']['total'] = len(set(concept_per_types))

        # Check presence for age/gender
        for demographic_type in demographic_types:

            if demographic_type+'_groups' in self.details['facet_details']:

                # Get all expected entities
                concept_list = [hl_entity['id'] for hl_entity in hl_entities if hl_entity is not None and hl_entity['type'] == demographic_type]

                if len(concept_list) != 0:

                    # If there is no demographic information for this type in the mesh terms (retrieved from facets), indicate as not discussed
                    if len(self.details['facet_details'][demographic_type+'_groups']) == 0:
                        details_json['query_' + demographic_type] = demographic_type + " not discussed in this publication"

                    # If there is demographic information for this type in the mesh terms (retrieved from facets)
                    else:
                        # By default indicate as different
                        details_json['query_' + demographic_type] = "different " + demographic_type + " discussed in this publication"

                        # Go through each expected concept
                        for concept in concept_list:

                            # If one of the expected concept found in the demographic facets, indicate as discussed in the publication
                            if concept in str(self.details['facet_details'][demographic_type+'_groups']):
                                details_json['query_' + demographic_type] = "same " + demographic_type + " discussed in this publication"
                                break

        # Return details
        return details_json

    def getQueryDetailsPmc(self, hl_entities, doc_json, snippets_json):
        ''' Add facets relative to the query '''

        # Initialize details json section
        details_json = {}

        # Query entity types
        entity_types = ['gene', 'disease']

        # Count occurrences per entity type
        for entity_type in entity_types:

            # Get all expected entities
            concept_per_types = [hl_entity['id'] for hl_entity in hl_entities if hl_entity is not None and hl_entity['type'] == entity_type]

            # Initialize the json for this entity type count
            details_json['query_' + entity_type + '_count'] = {}

            # Initialize the total count
            count_all = 0

            if entity_type+"s" in self.details['facet_details']:
                for facet in self.details['facet_details'][entity_type+"s"]:
                    if facet['id'] in concept_per_types:
                        count_all += facet['count']

            # Store the total count
            details_json['query_' + entity_type + '_count']['all'] = count_all

        # Check presence per entity type
        for entity_type in entity_types:

                # Initialize the json for this entity type presence
                details_json['query_' + entity_type + '_present'] = {}

                # Get all expected entities
                concept_per_types = [hl_entity['id'] for hl_entity in hl_entities if hl_entity is not None and hl_entity['type'] == entity_type]

                present = []

                # Check presence for each expected entity
                for concept_id in concept_per_types:
                    concept_status = False

                    # Go through each facet of this type
                    if entity_type + "s" in self.details['facet_details']:
                        for facet in self.details['facet_details'][entity_type + "s"]:
                            if facet['id'] == concept_id:
                                concept_status = True
                                break

                    # If not found in the facet, check the highlighted parets
                    if concept_status is False:
                        # Get json as text
                        doc_str = json.dumps(doc_json) + json.dumps(snippets_json)

                        # Check presence of the concept id in the document
                        if '<span class=\\"' + entity_type + '\\" concept_id=\\"' + concept_id + '\\">' in doc_str:
                            concept_status = True

                    if concept_status:
                        present.append(concept_id)


                # Store presence and absence
                details_json['query_' + entity_type + '_present']['present'] = len(set(present))
                details_json['query_' + entity_type + '_present']['absent'] = len(set(concept_per_types)) - len(set(present))
                details_json['query_' + entity_type + '_present']['total'] = len(set(concept_per_types))

        # Return details
        return details_json

    def loadValidIds(self, facet):
        ''' Load field mappings for the facet'''

        valid_ids = []
        # file location
        location = ""
        for possible_location in sys.path:
            if os.path.exists(possible_location + "/sibtmvar/files/mapping_"+facet+".txt"):
                location = possible_location
                break

        # Open mapping
        try:
            with open(location +"/sibtmvar/files/mapping_"+facet+".txt") as f:
                next(f)
                for line in f:
                    elements = line.split(";")
                    # Store mapping
                    valid_ids.append(elements[1].strip())

        # If file is not found
        except:
            self.errors.append({"level": "warning", "service":"file", "description": "Mapping file not found", "details":location +"sibtmvar/files/mapping_"+facet+".txt"})

        return valid_ids


    def getJson(self):
        ''' Return the details dictionary '''
        return self.details