import re

class Highlight:
    '''
    The Highlight class tag a given text with a set of entities

    Parameters
    ----------
    text : str
        a text to tag (e.g. an abstract, a title)
    entities : dict
        a set of entities to tag: [{'type': '', 'id': '', 'main_term': '', query_term': '', all_terms: [''], 'match': 'exact|partial'}]

    Attributes
    ----------
    text : str
        a text to tag (e.g. an abstract, a title)
    entities : dict
        a set of entities to tag: [{'type': '', 'id': '', 'main_term': '', query_term': '', all_terms: [''], 'match': 'exact|partial'}]
    highlighted_text: str
        the text with the highlight tags
    '''

    def __init__(self, text, entities):
        ''' The constructor stores the text to highlight and the entities to highlight '''

        # Store the parameters
        self.text = text
        self.entities = entities

        # Initiate the return variables
        self.highlighted_text = text

        # Highlight the text with given entities
        self.process()

    def process(self):
        ''' Executes the highlighting of the entities in the text '''

        # Select entities to tag and sort them by length

        # 1) Query term
        query_terms = [[entity['query_term'], entity['id'], entity['type'], entity['match'], 'query'] for entity in self.entities if 'query_term' in entity]
        query_terms.sort(key=lambda x: len(x[0]), reverse=True)

        # 2) Main term
        main_terms = [[entity['main_term'], entity['id'], entity['type'], entity['match'], 'main'] for entity in self.entities if 'main_term' in entity]
        main_terms.sort(key=lambda x: len(x[0]), reverse=True)

        # 3) All terms
        all_terms = []
        for entity in self.entities:
            if 'all_terms' in entity:
                for term in entity['all_terms']:
                    all_terms.append([term, entity['id'], entity['type'], entity['match'], 'all'])
        all_terms.sort(key=lambda x: len(x[0]), reverse=True)
        # Split all terms by sublist: terms from terminologies vs. other terms
        all_terms = [entity for entity in all_terms if entity[1] is not None] + [entity for entity in all_terms if
                                                                         entity[1] is None]

        # Merge the three lists to generate a final list
        to_tag_list = query_terms + main_terms + all_terms

        # Tag the list
        self.matchList(to_tag_list)


    def matchList(self, to_tag_list):
        ''' Replace terms from list in a text and store replaced parts in a list'''

        tagged_text = self.text
        tagged_list = []

        # For each element to match
        for to_tag_element in to_tag_list:

            # Get all its information
            term, concept_id, concept_type, match_type, source = to_tag_element

            # Escape the term (different for variant which has specific characters)
            if concept_type == "variant":
                term = re.sub('[\(\)\<\>\-]', ' ', term)
                term = re.sub('\s+', ' ', term)
                term = re.escape(term)
                term = term.replace("\ ", "[^A-Za-z0-9_]+")
            else:
                term = re.escape(term)

            # If match type is partial, allow characters after the match
            if match_type == "partial":
                matches = re.finditer(r"(?:^|(?<=[^A-Za-z0-9_]))" + term, tagged_text, flags=re.IGNORECASE)

            # Else if source is query/main and concept_type is gene, allow characters after the match
            elif (source == "query" or source == "main") and concept_type == "gene":
                matches = re.finditer(r"(?:^|(?<=[^A-Za-z0-9_]))" + term, tagged_text, flags=re.IGNORECASE)

            # Else if source is query/main and concept_type is variant, allow characters before the match
            elif (source == "query" or source == "main") and concept_type == "variant":
                matches = re.finditer(term + r"(?=[^A-Za-z0-9_]|$)", tagged_text, flags=re.IGNORECASE)

            # Else if source is all and concept_type is variant, allow characters before the match
            elif source == "all" and match_type == "partial":
                matches = re.finditer(term + r"(?=[^A-Za-z0-9_]|$)", tagged_text, flags=re.IGNORECASE)

            # Else, requires boundaries
            else:
                matches = re.finditer(r"(?:^|(?<=[^A-Za-z0-9_]))" + term + r"(?=[^A-Za-z0-9_]|$)", tagged_text,
                                      flags=re.IGNORECASE)

            # For each match
            for match in matches:

                # Get the start and end
                s = match.start()
                e = match.end()

                # Append the matched element in a list
                tagged_list.append([s, e, tagged_text[s:e], concept_id, concept_type])

                # Define the XXXX string (same length)
                newString = 'X' * (e-s)

                # Replace the text with the substitution
                tagged_text = tagged_text[:s] + newString + tagged_text[e:]

        # Take the replace list and sort it in order to process it by the last match first (in order that starting position doesn't change)
        tagged_list.sort(key=lambda x: x[0], reverse=True);

        # For each element to replace
        for tagged_element in tagged_list:

            # Get all its information
            start, end, term, concept_id, concept_type = tagged_element

            # Add the tag
            if concept_id is not None:
                self.highlighted_text = self.highlighted_text[:start] + "<span class=\"" + concept_type + "\" concept_id=\"" + concept_id + "\">" + term + "</span>" + self.highlighted_text[end:]
            else:
                self.highlighted_text = self.highlighted_text[:start] + "<span class=\"" + concept_type + "\">" + term + "</span>" + self.highlighted_text[end:]

