# Load items from Scrapy Cloud to ElasticSearch instance

## Installation
Install dependencies:
```bash
virtualenv venv
source venv/bin/activate
pip install .
```

Also you need to install ElasticSearch, or install docker and docker-compose to use the `docker-compose.yml` config from this project.
## Usage

### Fire up ElasticSearch
Launch it if you have local installation and make sure that it's running or use a configuration from this project and run ElasticSearch and Kibana with command:
```
docker-compose up -d
```

### Set environmental variables
In order to use this script you need you [Scrapy Cloud API key](https://app.scrapinghub.com/account/apikey), add it to environmenatal variable `SH_APIKEY`:
```bash
export SH_APIKEY="your_key"
```

### Run script
The project has a command line interface "shes" (ScrapingHub - ElasticSearch), try running it and see a help message:
```bash
$ ./shes.py -h
Download items to ElasticSearch.

usage: shes.py -j JOB_ID [-e ELASTICSEARCH_URL] [-i INDEX] [-t DOC_TYPE] [-h]

Download items from Scrapinhub cloud and upload them to ElasticSearch index.

optional arguments:
  -h, --help            show this help message and exit
  -j JOB_ID, --job_id JOB_ID                                Required Scrapy Cloud job idetentifier
  -e ELASTICSEARCH_URL, --elasticsearch ELASTICSEARCH_URL   URL of ElasticSearch instance, [default: localhost:9200]
  -i INDEX, --index index                                   Index name, defaults to job_id
  -t DOC_TYPE, --type DOC_TYPE                              Document type, [default: product]
```
