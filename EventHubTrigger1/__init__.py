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

def callInvisibilityApi(url):
    response=requests.get(url)
    response_json=json.loads(response.content)
    return response_json['mcs_sales_location']


def main(events: func.EventHubEvent, doc: func.Out[func.Document]) -> str:
    for event in events:
        json_event=event.get_body().decode('utf-8')
        log.info('Python EventHub trigger processed an json event: %s',
                    json_event)
        #json_event=xmltodict.parse(event_body)
        #log.info('Converted the incoming xml event to json : %s',json_event)
        #log.info('type: %s',type(json_event))
        json_obj = json.loads(json_event)
        division= json_obj['Header']['szExternalID'].lower()
        store= json_obj['Header']['lRetailStoreID'].lower()
        register= json_obj['Header']['lWorkstationNmbr'].lower()
        indicator = 'R'
        fromSellingLocation = json_obj['Header']['lTransactionTypeID'].lower()
        selling_location_url=' http://sgesbisapp.dfs.com:5555/Inventory/SellingLocation?Division={division}&Store={store}&Register={register}&SKU={sku}&Indicator={indicator}&FromSellingLocation={fromSellingLocation}'
        url='https://dfs-aass-dp-nprd-functionapp-01.azurewebsites.net/api/HttpTrigger1?code=4ggfLfpjG4jSKVn39BecqjXV6kKXW6S_b1-CnuAJkLnQAzFuCa-kAw==&name=Manish'
        mcs_sales_location=callInvisibilityApi(url)
        sales_sku_dict_list=[]
        for item in json_obj['LineItems']:
            sales_sku_dict={}
            rsku_id = item['szPOSItemID'].lower()
            csku_id = item['szCommonItemID'].lower()
            log.info(selling_location_url.format(division=division, store=store,register=register,
                            indicator=indicator,fromSellingLocation=fromSellingLocation,sku=rsku_id))
            id=csku_id+rsku_id+division+mcs_sales_location+store

            partition_key=csku_id+division+mcs_sales_location
            sales_sku_dict['id'] = id
            sales_sku_dict['partition_key'] = partition_key
            sales_sku_dict['Header'] = json_obj['Header']
            sales_sku_dict['Payments'] = json_obj['Payments']
            sales_sku_dict['Deposit'] = json_obj['Deposit']
            sales_sku_dict['LineItem'] = item
            sales_sku_dict['Header']['mcs_sales_location'] = mcs_sales_location

            log.info('output json: %s', json.dumps(sales_sku_dict))
            sales_sku_dict_list.append(func.Document.from_dict(sales_sku_dict))
        doc.set(func.Document.from_dict(sales_sku_dict))
        log.info('Sent record to cosmos DB')

        
        #url='https://api.publicapis.org/entries'
        #response = requests.get(url)
        #log.info('API response status : %s',response.status_code)
        #slog.info('API response : %s',response.json)
        return json.dumps(sales_sku_dict)

