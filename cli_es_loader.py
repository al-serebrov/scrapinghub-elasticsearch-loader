"""Downlad items to ElasticSearch.

usage: download_items_to_es.py -j JOB_ID [-e ELASTICSEARCH_URL] [-i INDEX] [-t DOC_TYPE] [-h]

Download items from Scrapinhub cloud and upload them to ElasticSearch index.

optional arguments:
  -h, --help            show this help message and exit
  -j JOB_ID, --job_id JOB_ID                                Required Scrapy Cloud job idetentifier
  -e ELASTICSEARCH_URL, --elasticsearch ELASTICSEARCH_URL   URL of ElasticSearch instance, [default: localhost:9200]
  -i INDEX, --index index                                   Index name, defaults to job_id
  -t DOC_TYPE, --type DOC_TYPE                              Document type, [default: product]

"""
import os
from docopt import docopt
from elasticsearch import Elasticsearch
from scrapinghub import ScrapinghubClient
from es_loader.es_loader import ESPipeline

arguments = docopt(__doc__)

es = arguments.get('--elasticsearch')
job_id = arguments.get('--job_id')
index = arguments.get('--index')
doc_type = arguments.get('--type')
api_key = os.environ.get('SH_API_KEY')

es = Elasticsearch([es])
sc = ScrapinghubClient(api_key)

es_pipe = ESPipeline(
    sc=sc,
    es=es,
    job_id=job_id,
    index=index,
    doc_type=doc_type
).process_items()
