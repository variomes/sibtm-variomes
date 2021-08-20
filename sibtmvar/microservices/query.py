import json
import re
import sys
import os

import pandas as pd

from sibtmvar.microservices import configuration as conf
from sibtmtermin.normalizer import normalizer
from sibtmvar.microservices import variants

class Query:
    '''
    The Query object stores and normalizes a query

    Parameters
    ----------
    conf_mode: str
        indicate which configuration file should be used (default: prod)
    conf_file: Configuration
        indicate a Configuration object to use (default: None)

    Attributes
    ----------
    pub_ids: list
        initial list of publication identifiers
    collection: String
        collection name
    disease_txt: String
        initial disease string
    gen_vars_txt: String
        initial genes and variants string
    gender_txt: String
        initial gender string
    age_txt: String
        initial age string
    disease_norm: list
        list of normalized diseases
    gen_vars_norm: list
        list of normalized genes and variants
    gender_norm: list
        list of normalized gender
    age_norm: list
        list of normalized age
    separator: String
        type of separator for genes and variants (default: and)
    conf_file: Configuration
        indicate a Configuration object to use (default: None)
    errors: list
        stores a list of errors with a json format

    '''

    def __init__(self, conf_file=None, conf_mode="prod"):
        ''' The constructor stores the configuration file '''

        # Initialize a variable to store errors
        self.errors = []

        # Initiate lists to store normalized data
        self.disease_norm = []
        self.gen_vars_norm = []
        self.gender_norm = []
        self.age_norm = []
        self.separator = "and"

        # Load configuration file
        self.conf_file = conf_file
        if conf_file is None:
            self.conf_file = conf.Configuration(conf_mode)
            # Cache error handling
            self.errors += self.conf_file.errors

    def setPubIds(self, pub_ids):
        self.pub_ids = pub_ids.split(";")

    def setCollection(self, collection):
        self.collection = collection

    def setDisease(self, disease):
        self.disease_txt = disease

        # Normalize disease
        self.disease_norm = []
        if self.disease_txt is not None and self.disease_txt != "none":
            for disease in self.disease_txt.split(";"):
                self.disease_norm.append(self.normalizeTerm(disease, 'disease'))

    def setGenVars(self, gen_vars):
        self.gen_vars_txt = gen_vars

        # Normalize gene and variants
        self.gen_vars_norm = []
        if self.gen_vars_txt is not None and self.gen_vars_txt != "none":

            # Define separator
            if " or " in self.gen_vars_txt.lower():
                self.separator ="or"

            # Replace and/or with ;
            mod_gen_vars = re.sub(r"\s+[oO][rR]\s+", ';', self.gen_vars_txt)
            mod_gen_vars = re.sub(r"\s+[aA][nN][dD]\s+", ';', mod_gen_vars)
            mod_gen_vars = re.sub(r",\s+", ';', mod_gen_vars)

            # For each gene
            for gen_var in mod_gen_vars.split(";"):

                # Define three ways to catch gene and variants
                match1 = re.search("^[A-Za-z1-9-]+\s*", gen_var)
                match2 = re.search("^none\s*", gen_var)
                match3 = re.search("\(none\)", gen_var)

                # By default, they are equal to none
                variant = "none"
                gene = "none"

                # If there is a gene
                if match1:

                    # Store the gene
                    gene = match1.group(0).strip()

                    # Extract the variant (the rest without parenthesis)
                    gen_var = re.sub(match1.group(0), '', gen_var)
                    gen_var = re.sub('[\(\)]', '', gen_var)

                    # If there is a variant, store it
                    if gen_var != "":
                        variant = gen_var

                # If the gene is marked as none
                elif match2:

                    # Extract the variant (the rest without parenthesis)
                    gen_var = re.sub(match2.group(0), '', gen_var)
                    gen_var = re.sub('[\(\)]', '', gen_var)

                    # If there is a variant, store it
                    if gen_var != "":
                        variant = gen_var

                # If the variant is marked as none
                elif match3:

                    # Extract the gene (the rest without parenthesis)
                    gen_var = re.sub(match3.group(0), '', gen_var)
                    gen_var = re.sub('[\(\)]', '', gen_var)

                    # If there is a gene, store it
                    if gen_var != "":
                        gene = gen_var

                # If there is no gene, store the expression as a variant only
                else:
                    variant = gen_var

                # In case of fusions, split the genes in a list
                genes = gene.split("-")

                # Normalize the gene and the variant
                variant_norm = variants.Variant(variant, gene, conf_file=self.conf_file)
                self.errors+= variant_norm.errors
                self.gen_vars_norm.append(([self.normalizeTerm(gene, 'gene') for gene in genes], variant_norm.asJson()))

                # Update to remove none gene or variants
                for i, gen_var in enumerate(self.gen_vars_norm):
                    genes, variant = gen_var
                    if variant['main_term'] == "none":
                        self.gen_vars_norm[i] = (genes, None)
                    if len(genes) == 1 and genes[0]['main_term'] == "none":
                        self.gen_vars_norm[i] = (None, variant)

    def setGender(self, gender):
        self.gender_txt = gender

        # Normalize gender
        self_gender_norm = []
        if self.gender_txt is not None and self.gender_txt != "none":
            for gender in self.gender_txt.split(";"):
                self.gender_norm.append(self.normalizeTerm(gender, 'gender'))


    def setAge(self, age):
        self.age_txt = age

        # file location
        location = ""
        for possible_location in sys.path:
            if os.path.exists(possible_location + "/sibtmvar/files/mapping_age.txt"):
                location = possible_location

        # Normalize age
        self.age_norm = []
        if self.age_txt is not None and self.age_txt != "none":
            for age in self.age_txt.split(";"):

                # Store the cvs as a datframe
                age_dataframe = pd.read_csv(location+"/sibtmvar/files/mapping_age.txt", sep=';')
                age_dataframe['min age'] = age_dataframe['min age'].astype(int)
                age_dataframe['max age'] = age_dataframe['max age'].astype(int)

                # Select only rows corresponding to the age to normalize
                selected_ages = age_dataframe[(age_dataframe['min age'] <= int(age)) &
                                             (age_dataframe['max age'] >= int(age))].loc[:, 'term']

                # Normalize selected age
                for selected_age in selected_ages:
                    self.age_norm.append(self.normalizeTerm(selected_age, 'age'))


    def normalizeTerm(self, term, concept_type, match="exact", normalize=True):
        ''' Retrieve the concept in the terminology and store it in the highlight entity structure '''

        element = {}

        if normalize:

            # Get the terminology associated with the entity type
            terminology = self.conf_file.settings['terminology'][concept_type+'_solr']

            # Search the concept
            try:
                norm = json.loads(normalizer.normalize(term, terminology, exactQuery=True, prodMode=False))

                # If something is found, get the best match
                if len(norm['results']) > 0:
                    element = {"type": concept_type,
                                "id": norm['results'][0]['concept_id'],
                                "query_term": term,
                                "main_term": norm['results'][0]['preferred_term'],
                                "all_terms": norm['results'][0]['synonyms'],
                               "terminology": terminology,
                                "match": match}

                    return element

            except:
                self.errors.append({"level": "warning", "service": "normalizer", "description": "Normalizer failed", "details": str(sys.exc_info()[0])})


        # If nothing is found, store a fake normalized entity (to highlight the query term)
        element = {"type": concept_type,
                        "id": term,
                        "query_term": term,
                        "main_term": term,
                        "all_terms": [],
                       "terminology": "none",
                        "match": match}

        return element

    def getInitQuery(self):
        ''' Return the query as a dict object '''

        output = {}

        if hasattr(self, "pub_ids") and self.pub_ids != "none":
            output['ids'] = self.pub_ids
        if hasattr(self, "collection") and self.collection != "none":
            output['collection'] = self.collection
        if hasattr(self, "gen_vars_txt") and self.gen_vars_txt != "none":
            output['genvars'] = self.gen_vars_txt
        if hasattr(self, "disease_txt") and self.disease_txt != "none":
            output['disease'] = self.disease_txt
        if hasattr(self, "age_txt") and self.age_txt != "none":
            output['age'] = self.age_txt
        if hasattr(self, "gender_txt") and self.gender_txt != "none":
            output['gender'] = self.gender_txt
        if hasattr(self, "pub_ids") and self.pub_ids != "none":
            output['keywords_positive'] = self.conf_file.settings['settings_user']['keywords_positive']
            output['keywords_negative'] = self.conf_file.settings['settings_user']['keywords_negative']

        return output

    def getNormQuery(self):
        ''' Return the normalized query '''

        output = {}

        # Add the disease
        if len(self.disease_norm) > 0:
            output['diseases'] = [self.conceptAsJson(disease)for disease in self.disease_norm]

        # Add the genes and variants
        if len(self.gen_vars_norm) > 0:
            output['genes'] = []
            output['variants'] = []
            for gen_var in self.gen_vars_norm:
                genes, variant = gen_var
                output['genes'] += [self.conceptAsJson(gene)for gene in genes]
                output['variants'] += [self.conceptAsJson(variant)]

        # Add the gender
        if len(self.gender_norm) > 0:
            output['gender'] = [self.conceptAsJson(gender)for gender in self.gender_norm]

        # Add the age
        if len(self.age_norm) > 0:
            output['ages'] = [self.conceptAsJson(age)for age in self.age_norm]

        return output


    def conceptAsJson(self, concept):
        ''' Transform the concept in the output normalized format '''

        # For concept where normalization failed, only return query term
        if concept['terminology'] == "none":
            concept_json = {'query_term': concept['query_term']}

        # For variants, add synonyms (needed for pmc viewer)
        elif concept['terminology'] == "variant":
            concept_json = {'concept_id': concept['id'],
                            'preferred_term': concept['main_term'],
                            'terminology': concept['terminology'],
                            'query_term': concept['query_term'],
                            'terms': concept['all_terms']}

        # For normalized concept
        else:
            concept_json = {'concept_id': concept['id'],
                            'preferred_term': concept['main_term'],
                            'terminology': concept['terminology'],
                            'query_term': concept['query_term']}

        return concept_json

    def getHlEntities(self):
        ''' Return a list of all normalized entities '''

        # Merge diseases, genders and ages
        all_entities = self.disease_norm+self.gender_norm+self.age_norm

        # Add genes and variants
        for gen_var in self.gen_vars_norm:
            genes, variant = gen_var
            all_entities += genes
            all_entities += [variant]

        #Add keywords
        for keyword in self.conf_file.settings['settings_user']['keywords_negative']:
            if keyword != "":
                all_entities += [self.normalizeTerm(keyword, "kw_neg", "partial", False)]
        for keyword in self.conf_file.settings['settings_user']['keywords_positive']:
            if keyword != "":
                all_entities += [self.normalizeTerm(keyword, "kw_pos", "partial", False)]

        return all_entities

