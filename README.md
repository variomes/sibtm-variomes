# sibtm-variomes

A python module to retrieve relevant literature for variants-related queries


dependencies
============
Required Python modules:
* elasticsearch
* numpy
* pandas
* pymongo
* pysolr
* flask

Other resources:
*Requires running SIBiLS Elasticsearch and MongoDB servers 
*Requires running sibtm-terminology
	
	
Python installation instructions
========================

* Install the solr package from the sibtm-variomes directory
	```bash
	pip install git+https://github.com/emilie19/sibtm-variomes.git
    ```
	
* Initialize the configuration file by running config.py
	```bash
	python sibtmvar/microservices/init.py
    ```   
	
* Update the configuration file for the sibtm-variomes services (~/.config/sibtm/config-variomes.ini)
	```
	[elasticsearch]
	s_url = localhost
	i_port = <port>
	s_username = <username>
	s_password = <password>
		
	[repository]
	r_cache = <path>/Caches/
	r_logs = <path>/Logs/
	r_errors = <path>/Errors/
	r_status = <path>/Status/
    r_api_files = <path>/API_Files/
	
	[url]
	s_mongodb = localhost:27017/
	s_synvar = http://goldorak.hesge.ch/synvar/generate/litterature/fromMutation
	s_ct = http://candy.hesge.ch/CT/CTSVIP/rankCT.jsp
    ```  
	
* Update the other settings if needed in the configuration file for the sibtm-variomes services (~/.config/sibtm/config-variomes.ini)

license
------------
This project is licensed under the terms of the GNU General Public License v3.0 license (gpl-3.0).

authors
------------
Emilie Pasche