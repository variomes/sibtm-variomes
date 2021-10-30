import json
import re
import requests
import sys
import urllib.parse

import xml.etree.ElementTree as ET

from sibtmvar.microservices import configuration as conf
from sibtmvar.microservices import cache
from sibtmtermin.normalizer import normalizer

class Variant():
    '''
    The Variant retrieves synonyms for a given variant in a gene

    Parameters
    ----------
    variant_term: String
        a variant
    gene_term: String
        a gene name
    conf_mode: str
        indicate which configuration file should be used (default: prod)
    conf_file: Configuration
        indicate a Configuration object to use (default: None)

    Attributes
    ----------
    init_term: String
        the variant as mentionned in the query
    gene_term: String
        a gene name
    variant_type: String
        the type of the variant (SNV, CNV, other)
    concept_id: String
        a unique identifier for the variant
    preferred_term: String
        the preferred term for the variant
    synonyms: list
        a list of all variant's synonyms
    conf_file: Configuration
        indicate a Configuration object to use (default: None)
    errors: list
        a list of errors encountered by the variant service
    '''

    def __init__(self, variant_term, gene_term, conf_file=None, conf_name="prod"):
        ''' The constructor stores the initial query terms and runs the normalization '''

        # Initialize a variable to store errors
        self.errors = []

        # Initiate configuration file
        self.conf_file = conf_file
        if conf_file is None:
            self.conf_file = conf.Configuration(conf_name)
            # Cache error handling
            self.errors += self.conf_file.errors

        # Store initial term, the gene term and the variant type in instance variables
        self.gene_term = gene_term
        self.init_term = variant_term.replace("%2b", "+")
        self.variant_type = "other"

        # Initiate the concept normalization (default)
        self.concept_id = self.gene_term + "_" + self.init_term
        self.concept_id = self.concept_id.replace(" ", "-")
        self.preferred_term = self.init_term
        self.synonyms = []

        # Normalize the variant if not none
        if variant_term != "none":
            self.norm()

    def norm(self):
        '''Search for the initial term into synvar or solr to extract a list of synonyms or tries to generate them automatically. Also add a concept_id (gene + init_term) and preferred_term (same as init_term) '''

        # If the variant is available in cache
        var_cache = cache.Cache("synvar", self.gene_term + "_" + self.init_term, "xml", conf_file=self.conf_file)
        if var_cache.isInCache():

            # Define the variant type (SNV because only synvar are cached)
            self.variant_type = "SNV"

            # Load the cache file
            root = ET.fromstring(var_cache.loadFromCache())

            # Parse the cache file
            self.parseFromSynVar(root)

        # Otherwise
        else:

            # If a SNV, query SynVar
            #if self.gene_term != "none" and re.match("[a-zA-Z]{1,3}\d+[a-zA-Z*]{1,3}", self.init_term):
            content = self.loadFromSynVar()
            if content is not None:
                self.variant_type = "SNV"
                var_cache.storeToCache(content)

            # If not a SNV
            if content is None:

                # Try to normalize as a CNV, using own list
                self.loadFromSolr()

                # Otherwise automatically generate a list of expressions
                if self.variant_type == "other":
                    self.loadFromAutomaticGenerator()

        # handle errors
        self.errors += var_cache.errors

    def loadFromSynVar(self):
        ''' Queries the synvar services and parse its output or logs an error if it fails'''

        # Build url with query
        query = self.conf_file.settings['url']['synvar']
        query_term = "?ref=" + self.gene_term + "&variant=" + urllib.parse.quote(self.init_term)

        # If the service works
        try:

            # Query synvar
            url = requests.get(query + query_term)
            url_content = url.text

            # Retrieve root of the xml
            root = ET.fromstring(url_content)

            # Parse synvar
            self.parseFromSynVar(root)

            # Store synvar
            return(url_content)

        # If the service fails
        except:

            try:

                # Update query
                query_term = "?map=false&ref=" + self.gene_term + "&variant=" + urllib.parse.quote(
                    self.init_term) + "&level=transcript"

                # Query synvar
                url = requests.get(query + query_term)
                url_content = url.text

                # Retrieve root of the xml
                root = ET.fromstring(url_content)

                # Parse synvar
                self.parseFromSynVar(root)

                # Store synvar
                return(url_content)

            except:
                self.errors.append({"level": "warning", "service":"synvar", "description": "Synvar service failed", "details":self.gene_term + ": " + self.init_term })
                return None

    def loadFromSolr(self):
        ''' Queries the sibtm-terminology module'''

        try:
            # Get the terminology associated with the entity type
            norm = json.loads(normalizer.normalize(self.init_term, "cnv", exactQuery=True, prodMode=False))
            if len(norm['results']) > 0:
                self.concept_id = norm['results'][0]['concept_id']
                self.preferred_term= norm['results'][0]['preferred_term']
                self.synonyms = norm['results'][0]['synonyms']
                self.variant_type = "CNV"

        except:
            self.errors.append({"level": "warning", "service": "normalizer", "description": "Normalizer failed", "details": str(sys.exc_info()[0])})


    def loadFromAutomaticGenerator(self):
        ''' Automatically generates a set of synonyms based on regexes '''

        # Initiate the list
        synonyms = []

        # Automatic generation of substitutions based on a set of regex for most common cases (to add more cases upon needs)

        # e.g. exon9 => exon 9, exon_9 exon-9
        matchObj = re.match(r'^([a-zA-Z]+) *(\d+)$', self.init_term)
        if matchObj:
            synonyms.append(matchObj.group(1) + " " + matchObj.group(2))
            synonyms.append(matchObj.group(1) + "" + matchObj.group(2))
            synonyms.append(matchObj.group(1) + "-" + matchObj.group(2))
            synonyms.append(matchObj.group(1) + "_" + matchObj.group(2))

        # e.g. A250_Y252 => A250 Y252, A250Y252 A250-Y252
        matchObj = re.match(r'^([A-Z]*\d+)[- _]([A-Z]*\d+)$', self.init_term)
        if matchObj:
            synonyms.append(matchObj.group(1) + " " + matchObj.group(2))
            synonyms.append(matchObj.group(1) + "" + matchObj.group(2))
            synonyms.append(matchObj.group(1) + "_" + matchObj.group(2))
            synonyms.append(matchObj.group(1) + "-" + matchObj.group(2))

        # e.g. A250_Y252 => 250 252, 250-252, 250_252
        matchObj = re.match(r'^[A-Z](\d+)[- _][A-Z](\d+)$', self.init_term)
        if matchObj:
            synonyms.append(matchObj.group(1) + " " + matchObj.group(2))
            synonyms.append(matchObj.group(1) + "_" + matchObj.group(2))
            synonyms.append(matchObj.group(1) + "-" + matchObj.group(2))

        # Remove duplicates and store in the synonyms instance variable
        self.synonyms = list(dict.fromkeys(synonyms))

    def parseFromSynVar(self, root):
        ''' Parses the XML output of the synvar service '''

        # Initiate the list
        synonyms = []

        variant_list = root.find("variant-list")
        variants = variant_list.findall("variant")

        if len(variants) == 0:
            self.errors.append({"level": "warning", "service":"synvar", "description": "Variant not found at this position in the gene", "details":self.gene_term + ": " + self.init_term })

        for variant in variants:

            # Get HGVS
            hgvs_element = variant.find("hgvs")
            if hgvs_element is not None:
                hgvs = hgvs_element.text
                if hgvs is not None:
                    synonyms.append(hgvs.strip())

            # Get rsID
            rsid_element = variant.find("rsid")
            if rsid_element is not None:
                rsid = rsid_element.text
                if rsid is not None:
                    synonyms.append(rsid.strip())

            # Get cosmic
            cosmic_element = variant.find("cosmic")
            if cosmic_element is not None:
                cosmic = cosmic_element.text
                if cosmic is not None:
                    synonyms.append(cosmic.strip())

            # Get synonyms at genome level
            genome_level = variant.find("genome-level")
            if genome_level is not None:

                # Get HGVS
                hgvs_element = genome_level.find("hgvs")
                if hgvs_element is not None:
                    hgvs = hgvs_element.text
                    if hgvs is not None:
                        synonyms.append(hgvs.strip())

                # Get syntactic list
                syntactic_list = genome_level.find("syntactic-variation-list")
                if syntactic_list is not None:
                    variations = syntactic_list.findall("syntactic-variation")
                    for variation in variations:
                        if variation.text is not None:
                            synonyms.append(variation.text.strip())

            # Get isoform list
            isoform_list = variant.find("isoform-list")
            if isoform_list is not None:

                # Get each isoform
                isoforms = isoform_list.findall("isoform")
                for isoform in isoforms:

                    # Get transcript level
                    transcript_level = isoform.find("transcript-level")
                    if transcript_level is not None:

                        # Get HGVS
                        hgvs_list = transcript_level.find("hgvs-list")
                        if hgvs_list is not None:
                            hgvs_elements = hgvs_list.findall("hgvs")
                            for hgvs_element in hgvs_elements:
                                hgvs = hgvs_element.text
                                if hgvs is not None:
                                    synonyms.append(hgvs.strip())

                        # Get syntactic variations
                        syntactic_list = transcript_level.find("syntactic-variation-list")
                        if syntactic_list is not None:
                            variations = syntactic_list.findall("syntactic-variation")
                            for variation in variations:
                                if variation.text is not None:
                                    synonyms.append(variation.text.strip())

                    # Get transcript level
                    protein_level = isoform.find("protein-level")

                    if protein_level is not None:

                        # Get HGVS
                        hgvs_list = protein_level.find("hgvs-list")
                        if hgvs_list is not None:
                            hgvs_elements = hgvs_list.findall("hgvs")
                            for hgvs_element in hgvs_elements:
                                hgvs = hgvs_element.text
                                if hgvs is not None:
                                    synonyms.append(hgvs.strip())

                        # Get syntactic variations
                        syntactic_list = protein_level.find("syntactic-variation-list")
                        if syntactic_list is not None:
                            variations = syntactic_list.findall("syntactic-variation")
                            for variation in variations:
                                if variation.text is not None:
                                    synonyms.append(variation.text.strip())

        # Remove duplicates and store in the synonyms instance variable
        self.synonyms = list(dict.fromkeys(synonyms))
        self.synonyms = list(filter(None, self.synonyms))

    def asJson(self):

        output = {}

        output['type'] = 'variant'
        output['id'] = self.concept_id
        output['query_term'] = self.init_term
        output['main_term'] = self.preferred_term
        output['all_terms'] = self.synonyms
        output['terminology'] = 'variant'
        output['match'] = 'exact'

        return output
