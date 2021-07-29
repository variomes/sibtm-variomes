import flask
from flask import Response, request

from sibtmvar.apis import apifetch as af
from sibtmvar.apis import apiranklit as arl
from sibtmvar.apis import apirankvar as arv
from sibtmvar.microservices import configuration as conf

app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Select the prod or dev configuration files
conf_mode = "dev2"

# Load the configuration file
conf_file = conf.Configuration(conf_mode)

#APIs for variomes services

@app.route('/api/fetchDoc', methods=['GET'])
def fetchDoc():
    ''' Fetch one or several documents and return them with highlights and statistics '''
    output = af.fetchDoc(request, conf_mode=conf_mode)
    return Response(output, content_type="application/json; charset=utf-8")

@app.route('/api/rankLit', methods=['GET'])
def rankLit():
    ''' Search and rank documents for one query '''
    output = arl.rankLit(request, conf_mode=conf_mode)
    return Response(output, content_type="application/json; charset=utf-8")

@app.route('/api/rankVar', methods=['GET', 'POST'])
def rankVar():
    ''' Search and rank variants for one file or query '''
    output = arv.rankVar(request, conf_mode=conf_mode)
    return Response(output, content_type="application/json; charset=utf-8")

# Run the API
app.run(host=conf_file.settings['api']['host'],port=conf_file.settings['api']['port'])
