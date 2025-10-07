import json
import sys
import os
import logging
import ecs_logging
from elasticsearch import Elasticsearch
from elasticsearch.client import IngestClient, LogstashClient
from elasticsearch.helpers import bulk
from elasticsearch.helpers import BulkIndexError
from deepdiff import DeepDiff 
from pathlib import Path
import datetime

from typing import List
from time import sleep

ES_INDEX='test-elastic'
LS_INDEX='test-logstash'

# logger = logging.getLogger('output_uploader')
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
logging.StreamHandler().setFormatter(ecs_logging.StdlibFormatter())
logger = logging.getLogger('output_uploader')
logger.setLevel(logging.DEBUG)

# logger.addHandler(handler)

es = Elasticsearch(hosts=["https://127.0.0.1:9200"], verify_certs=False, basic_auth=("elastic", "changeme"))

logging.info("Waiting for Elastic to be reachable")
while not es.ping():
    sleep(1)
    logging.debug("Waiting for Elastic to be reachable")

logging.info("Elastic is reachable")

pipelines = json.loads(sys.argv[1])



for p in pipelines:
   IngestClient.put_pipeline(es, id=p, **pipelines[p])


def generate_elasticsearch_bulk(filename: str, content: List[str]):
    for c in content:
        pipeline = f"main-pipeline-{Path(filename).stem}"
        source = json.loads(c)
        
        yield {
            '_index': ES_INDEX,
            '_id': source["_testsuite"]["_id"],
            'pipeline': pipeline,
            '_source': source
        }


# We are only interested in the output source, so no fields are necessary
# Create the empty indices
es.options(ignore_status=400).indices.create(index=ES_INDEX, mappings={'dynamic': False})
es.options(ignore_status=400).indices.create(index=LS_INDEX, mappings={'dynamic': False})

# Upload the Pipelines in the central pipeline management
for root, dirs, files in os.walk("../testdata/transpile/test"):
    for filename in files:
        abs_path = os.path.join(root, filename)
        with open(abs_path, "r") as content:
            # print(content.read())
            last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(abs_path)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            logger.debug("Last modified %s" , last_modified)
            
            LogstashClient.put_pipeline(es, id=os.path.splitext(os.path.basename(filename))[0], 
            pipeline={"username": "mirko","pipeline_metadata":{}, "last_modified": last_modified, "pipeline_settings": {}, "pipeline": content.read()})


## TODO Make Dynamic
files = ["./test/useragent.ndjson", "./test/capitalize.ndjson"]

for f in files:
    with open(f, "r") as content:
        lines = [l for l in content.readlines() if l != ""]

        logger.debug(lines)
        try:
            bulk(es, generate_elasticsearch_bulk(f, lines))
        except BulkIndexError as bie:
            logger.exception(bie)
            logger.info(bie.errors)


errors = 0


sleep(10)

for f in files:
    with open(f, "r") as content:        
        ids = [json.loads(line)["_testsuite"]["_id"] for line in content.readlines()]
        for id in ids:
            es_example = es.get(index=ES_INDEX, id=id)
            ls_example = es.get(index=LS_INDEX, id=id)
            ddiff = DeepDiff(es_example["_source"], ls_example["_source"], ignore_order=True, exclude_paths=["root['@version']", "root['@timestamp']", "root['host']", "root['log']", "root['_TRANSPILER']"])
            if bool(ddiff):
                print(id)
                # print(es_example["_source"])
                # print(ls_example["_source"])
                errors += 1
                logger.error(ddiff)

logger.info(f"There is/are {errors} error(s)\n")
assert errors == 0 
print("Test Successful")
        



        



