import logging

import azure.functions as func
import config
import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import json
import requests

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
log.info("Logging Info")
log.debug("Logging Debug")

def main(events: func.EventHubEvent, doc: func.Out[func.Document]):
    for event in events:
        json_event=event.get_body().decode('utf-8')
        log.info('Python EventHub trigger processed an json event: %s',
                    json_event)
        json_obj = json.loads(json_event)
        csku_id=json_obj['CSKU_ID'].lower()
        rsku_id=json_obj['RSKU_ID'].lower()
        retail_store_id=json_obj['Retail_Store_ID'].lower()
        division= json_obj['MCS_Region_ID'].lower()
        location_id=json_obj['Selling_Location_ID'].lower()
        
        partition_key=csku_id+division+location_id
        json_obj['partition_key'] = partition_key

        id=csku_id+rsku_id+division+location_id+retail_store_id
        json_obj['id'] = id

        doc.set(func.Document.from_dict(json_obj))
        log.info('Sent record to cosmos DB %s',json.dumps(json_obj))

