import configparser
import copy
import os

def init():
    ''' Create first version of configuration files '''

    config_file_name = "config-variomes-tttt"
    default_config = {
       "cache":{
          "i_saved_days_synvar":"30",
          "s_is_activated_synvar":"True",
          "i_saved_days_es":"1",
          "s_is_activated_es":"True",
          "i_saved_days_ct":"30",
          "s_is_activated_ct":"True",
          "i_saved_days_fetchLit":"30",
          "s_is_activated_fetchLit":"True",
          "i_saved_days_ranklit":"1",
          "s_is_activated_ranklit":"True",
          "i_saved_days_rankvar":"1",
          "s_is_activated_rankvar":"True"
       },
        "elasticsearch":{
            "s_url": "localhost",
            "i_port": "9200",
            "s_username":"",
            "s_password": ""
        },
       "repository":{
           "r_cache":"<path>/Caches/",
           "r_logs":"<path>/Logs/",
           "r_errors":"<path>/Errors/",
           "r_status":"<path>/Status/",
           "r_api_files": "<path>/API_Files/"
       },
       "settings_system":{
          "s_client_mongodb_medline":"SIBiLS",
          "s_client_mongodb_pmc":"SIBiLS",
          "s_client_mongodb_ct":"clinical_trials",
           "s_mongodb_collection_bib_medline":"bibmed20",
            "s_mongodb_collection_bib_pmc":"bibpmc20",
           "s_mongodb_collection_bib_ct":"ct2019",
           "s_mongodb_collection_metadata_medline":"metadatamed2020",
           "s_mongodb_collection_metadata_pmc":"metadatapmc20",
           "s_mongodb_collection_metadata_ct":"",
           "s_mongodb_collection_ana_medline":"anamed20",
           "s_mongodb_collection_ana_pmc":"anapmc20",
           "s_mongodb_collection_ana_ct":"anact2019",
           "s_es_index_medline":"med20",
           "s_es_index_pmc":"pmc20",
           "l_collections":"medline,pmc,ct"
       },
       "settings_user":{
          "l_collections":"medline",
          "i_min_date":"1900",
          "i_max_date":"2100",
          "b_mandatory_disease":"true",
          "b_mandatory_gene":"true",
          "b_mandatory_variant":"true",
          "b_synonym_gene":"true",
          "b_synonym_variant":"true",
          "b_synonym_disease":"true",
          "l_keywords_positive":"",
          "l_keywords_negative":"",
          "b_cache":"true",
          "i_es_results_nb ":"1000",
          "l_fetch_fields_medline":"abstract,authors,chemicals,comments_in,comments_on,date,publication_date,journal,keywords,meshs,publication_types,title",
          "l_fetch_fields_pmc":"abstract,title,authors,date,pmc_date,journal,publication_types,pmid,keywords",
          "l_fetch_fields_ct":"abstract,title,start_date,completion_date,gender,minimum_age,maximum_age,brief_title,official_title,brief_summary,detailed_description,condition,inclusion_criteria,keywords,details",
          "l_hl_fields_medline":"abstract,chemicals,keywords,meshs,title",
          "l_hl_fields_pmc":"abstract,keywords,title",
          "l_hl_fields_ct":"abstract,title",
          "l_search_fields_pmc":"title,abstract,keywords,full_text,figures_captions",
          "l_search_fields_medline":"title,abstract,mesh_terms,keywords"
    },
       "settings_ranking":{
          "l_strategies":"relax,annot,demog,kw",
          "f_strategy_relax_weight":"0.1",
          "f_relax_dg_weight":"0.2",
          "f_relax_gv_weight":"0.2",
          "f_relax_dv_weight":"0.2",
          "f_strategy_annot_weight":"0.1",
          "f_annot_gene_weight":"0.5",
          "f_annot_disease_weight":"0.3",
          "f_annot_drug_weight":"0.4",
          "f_strategy_demog_weight":"0.1",
          "f_demog_age_weight":"0.4",
          "f_demog_gender_weight":"0.4",
          "f_match_age_bonus":"0.5",
          "f_match_gender_bonus":"0.3",
          "f_undiscussed_age_bonus":"0.5",
          "f_undiscussed_gender_bonus":"0.3",
          "f_strategy_kw_weight":"0.005",
          "f_kw_pos_weight":"0.5",
          "f_kw_neg_weight":"-0.1"
       },
       "terminology":{
          "s_disease_mongo":"NCI Thesaurus",
          "s_gene_mongo":"nextprot",
          "s_demographics_mongo":"mesh",
          "s_drug_mongo":"drugbank",
          "s_disease_solr":"ncit",
          "s_gene_solr":"nextprot",
          "s_demographics_solr":"mesh",
          "s_drug_solr":"drugbank"
       },
       "url":{
          "s_mongodb":"localhost:27017/",
          "s_synvar":"http://goldorak.hesge.ch/synvar/generate/litterature/fromMutation",
          "s_ct":"http://candy.hesge.ch/CT/CTSVIP/rankCT.jsp"
       },
        "api": {
            'i_port': '5003',
            's_host': '0.0.0.0'
        }
    }

    create(config_file_name, default_config)

def create(config_file_name, default_config):
    ''' Create a configuration file with a given name and a default configuration '''

    # Create a configuration file to “~/.config/sibtm/config-variomes.ini”
    config_folder = os.path.join(os.path.expanduser("~"), '.config', 'sibtm')
    os.makedirs(config_folder, exist_ok=True)
    config_file_path = os.path.join(config_folder, config_file_name+".ini")
    config = configparser.ConfigParser()

    # If no configuration file found, create default configuration file
    if not os.path.exists(config_file_path) or os.stat(config_file_path).st_size == 0:
        for key in default_config.keys():
            config[key] = default_config[key]
        with open(config_file_path, 'w') as config_file:
            config.write(config_file)


init()