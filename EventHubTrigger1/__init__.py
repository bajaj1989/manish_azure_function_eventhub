import logging

import azure.functions as func
import config
import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import json
import datetime
import time
import requests
import xmltodict

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
log.info("Logging Info")
log.debug("Logging Debug")

def main(events: func.EventHubEvent, doc: func.Out[func.Document]) -> str:
    for event in events:
        event_body=event.get_body().decode('utf-8')
        log.info('Python EventHub trigger processed an xml event: %s',
                    event_body)
        json_event=xmltodict.parse(event_body)
        log.info('Converted the incoming xml event to json : %s',
                    json_event)
        doc.set(func.Document.from_dict(json_event))
        log.info('Sent record to cosmos DB')
        #url='http://sgesbisapp.dfs.com:5555/Inventory/SellingLocation?Division=58&Store=451&Register=632&SKU=18299120&Indicator=R&FromSellingLocation=451'
        #response = requests.post(url)
        #log.info(response.status_code)
        return json.dumps(json_event)

