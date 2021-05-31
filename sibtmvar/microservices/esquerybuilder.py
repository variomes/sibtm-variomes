import json

from sibtmvar.microservices import configuration as conf
from sibtmvar.microservices import mapping as map

class ESQueryBuilder:
    '''
    The ESQueryBuilder class build an ElastiCsearch query for a triplet gene, variant, disease (or a duo)

    Parameters
    ----------
    collection : str
        a collection to search in (medline, pmc, ct)
    conf_mode: str
        indicate which configuration file should be used (default: prod)
    conf_file: Configuration
        indicate a Configuration object to use (default: None)

    Attributes
    ----------
    collection : str
        a collection to search in (medline, pmc, ct)
    conf_file: Configuration
        indicate a Configuration object to use (default: None)

    '''
    def __init__(self, collection, conf_file=None, conf_mode="prod"):
        ''' Store the collection and loads the configuration'''

        # Initialize a variable to store errors
        self.errors = []

        # Store user parameters
        self.collection = collection

        # Load configuration file
        self.conf_file = conf_file
        if conf_file is None:
            self.conf_file = conf.Configuration(conf_mode)
            # Cache error handling
            self.errors += self.conf_file.errors


    def buildQuery(self, disease=None, gene=None, variant=None, highlight=True):
        ''' Return the json query for a tuple disease-gene-variant '''

        full_query = {}

        # Build query parts
        query_parts = []

        # disease part if not null
        if disease is not None and len(disease) > 0:
            disease_query = self.buildAnnotatedEntity(disease, expand=self.conf_file.settings['settings_user']['synonym_disease'])
            query_parts.append(disease_query)

        # gene part if not null
        if gene is not None and len(gene) > 0:
            gene_query = self.buildAnnotatedEntity(gene, variant=variant, expand=self.conf_file.settings['settings_user']['synonym_gene'])
            query_parts.append(gene_query)

        # variant part if not null
        if variant is not None and len(variant) > 0:
            variant_query = self.buildVariantEntity(variant, gene=gene, expand=self.conf_file.settings['settings_user']['synonym_variant'])
            query_parts.append(variant_query)

        # Add date part
        date_clause = {'lte': self.conf_file.settings['settings_user']['max_date'], 'gte': self.conf_file.settings['settings_user']['min_date']}
        date_query = {"range": {"pubyear": date_clause}}
        query_parts.append(date_query)

        # Add to the full query
        full_query["query"] = {"bool": {"must": query_parts}}

        # Load mapping
        self.fields_mapping = map.FieldsMapping(self.collection)

        # Mapping error handling
        self.errors += self.fields_mapping.errors

        # Add fields to return to the full query
        fields = self.fields_mapping.convertListFromUserNames(self.conf_file.settings['settings_user']['fetch_fields_' + self.collection])
        if self.collection == "pmc":
            if not "pmid" in fields:
                fields.append("pmid")
            if not "pmcid" in fields:
                fields.append("pmcid")
        full_query["_source"] = fields

        # Add the highlight
        if highlight:
            if variant is not None and len(variant) > 0:
                full_query["highlight"] = self.buildHighlight(variant_query)

        # Return the json
        print(json.dumps(full_query))
        return json.dumps(full_query)

    def buildHighlight(self, variant_query):
        ''' Create the highlight part of the elasticsearch query to retrieve snippets of text containing evidence '''

        # Initialize the clause
        highlight_part = {}

        # Add the elements
        highlight_part['order'] = 'score'
        highlight_part['fields'] = {}
        highlight_part['fields']['*'] = {}

        # Define the size of the snippet
        highlight_part['fields']['*']['fragment_size'] = 200
        highlight_part['fields']['*']['number_of_fragments'] = 5
        highlight_part['fields']['*']['type'] = "plain"
        highlight_part['fields']['*']['fragmenter'] = "span"

        # Returns the snippets only the variants
        highlight_part['fields']['*']['highlight_query'] = variant_query

        # Return the clause
        return highlight_part

    def buildAnnotatedEntity(self, entities, variant=None, expand=True):
        ''' Return a part of the query for an annotated entity '''

        entities_part = []

        # Go through each Entity object of Entities object
        for i, entity in enumerate(entities):
            entity_parts = []

            # entity as initial term in free text
            match_query_term = self.buildMultiMatch(entity['query_term'])
            entity_parts.append(match_query_term)

            # entity in annotation if synonym expansion is requested and if the entity has been normalized
            if expand and entity['terminology'] != "none":
                match_annotation = self.buildAnnotationMatch(entity['type'], entity['id'])
                entity_parts.append(match_annotation)

            # entity as preferred term if synonym expansion is requested and if the entity has been normalized and preferred term is different from the initial term
            if expand and entity['main_term'].lower() != entity['query_term'].lower():
                match_main_term = self.buildMultiMatch(entity['main_term'])
                entity_parts.append(match_main_term)

            # entity as gene_variant merged together if synonym expansion is requested, and  if gene type and if variants has been provided
            if expand and entity['type'] == "gene" and variant is not None:
                # Check if there is only one gene
                if len(entities) == 1:
                    match_combined_term = self.buildMultiMatch(entity['query_term'] + variant['query_term'])
                    entity_parts.append(match_combined_term)

            # If the entity is composed of more than one clause, merge them together with a should clause
            if len(entity_parts) > 1:
                entities_part.append({"bool": {"should": entity_parts}})
            # If there is one clause
            elif len(entity_parts) == 1:
                entities_part.append(entity_parts[0])

        # Return the json elements

        # If there are several entities, merge them together
        if len(entities_part) > 1:
            # Returned the combined clause
            return {"bool": {"must": entities_part}}
        # If there is one entity
        else:
            # Return the single clause
            return entities_part[0]

    def buildVariantEntity(self, variant, gene=None, expand=True):
        ''' Return a part of the query for a Variants object '''

        entity_parts = []

        # entity as initial term in free text
        if variant['query_term'] != "none":
            match_init_term = self.buildMultiMatch(variant['query_term'])
            entity_parts.append(match_init_term)

        # entity as any synonym
        if expand:
            for synonym in variant['all_terms']:
                if synonym.lower() != variant['query_term'].lower():
                    match_init_term = self.buildMultiMatch(synonym)
                    entity_parts.append(match_init_term)

        # entity as gene_variant merged together if gene
        if variant['query_term'] != "none" and gene is not None:
            if len(gene) == 1 and gene[0]['query_term'] != "none":
                match_init_term = self.buildMultiMatch(gene[0]['query_term'] + variant['query_term'])
                entity_parts.append(match_init_term)

        # return the json element
        if len(entity_parts) > 1:
            return {"bool": {"should": entity_parts}}
        elif len(entity_parts) == 1:
            return entity_parts[0]


    def buildAnnotationMatch(self, entity_type, concept_id):
        ''' Return an annotation json element '''

        match_annotation = {"match": {"annotations_str": concept_id}}

        # Return the json object
        return match_annotation

    def buildMultiMatch(self, term):
        ''' Return a multi-match json element '''

        multi_match = {}
        multi_match["query"] = term
        multi_match["fields"] = self.conf_file.settings['settings_user']['search_fields_' + self.collection]
        multi_match["type"] = "phrase"

        # Return the json object
        return {"multi_match": multi_match}