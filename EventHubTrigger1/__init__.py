import logging

import azure.functions as func
import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import json
from datetime import datetime
import requests
import xmltodict
import psycopg2

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
log.info("Logging Info")
log.debug("Logging Debug")

def callCanonicalApi(url,sales_event_body):
    response=requests.post(url,json={"sales_event":sales_event_body})
    return response.content

def callInvisibilityApi(url):
    response=requests.get(url)
    response_json=json.loads(response.content)
    return response_json['mcs_sales_location']


def executeCosmosPostgres(query):
    try:
        conn = psycopg2.connect(
        host="c-dfs-aass-dp-nprd-cosmospostgres-01.wwvvotg4x7fdz4.postgres.cosmos.azure.com",
        database="ToDo",
        user="citus",
        password="Admin@1234")
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def main(events: func.EventHubEvent) -> str:
    for event in events:
        raw_json_event=event.get_body().decode('utf-8')
        log.info('Python EventHub trigger processed an json event: %s',
                    raw_json_event)
        canonicalUrl="https://dfs-aass-dp-nprd-functionapp-01.azurewebsites.net/api/SalesCanonicalHttpTrigger?code=Q24MxYLf8vLk0vlHW3rOxlWgYDcASnr18GSOKE_SnCbGAzFu5TLg5g=="
        json_event= callCanonicalApi(canonicalUrl,raw_json_event)
        log.info('Canonical format json: %s',json_event)
        json_obj = json.loads(json_event)
        division= json_obj['Header']['szExternalID'].lower()
        log.info('Division: %s',division)
        store= json_obj['Header']['lRetailStoreID'].lower()
        register= json_obj['Header']['lWorkstationNmbr'].lower()
        txn_nmbr=json_obj['Header']['dh_txn_nmbr']
        updated_at_str=json_obj['Header']['szStartTime']
        updated_at = datetime.strftime(datetime.strptime(updated_at_str,'%Y%m%d%H%M%S'),'%Y-%m-%d %H:%M:%S')
        indicator = 'R'
        fromSellingLocation=''
        if json_obj['Header']['lTransactionTypeID'] is not None:
            fromSellingLocation = json_obj['Header']['lTransactionTypeID'].lower()

        selling_location_url=' http://sgesbisapp.dfs.com:5555/Inventory/SellingLocation?Division={division}&Store={store}&Register={register}&SKU={sku}&Indicator={indicator}&FromSellingLocation={fromSellingLocation}'
        url='https://dfs-aass-dp-nprd-functionapp-01.azurewebsites.net/api/HttpTrigger1?code=4ggfLfpjG4jSKVn39BecqjXV6kKXW6S_b1-CnuAJkLnQAzFuCa-kAw==&name=Manish'
        
        insert_query='insert into sales_pos_events (CSKU_ID, RSKU_ID, division,Selling_Location_ID, Retail_Store_ID, is_eod_completed, updated_at, sold_quantity, dh_txn_nmbr, event_details) values '
        #sales_sku_dict_list=[]
        query_values=[]
        sales_sku_dict = {}
        if len(json_obj['LineItems']) == 0:
            query_value = '(\'\',\'\',\'' + division + '\',\'\',\'' + store + '\',false,\'' + updated_at + '\',0,\'' + txn_nmbr + '\',\'' + json.dumps(json_obj) + '\')'
            query_values.append(query_value)
        else:
            for item in json_obj['LineItems']:
                sales_sku_dict = {}
                rsku_id = item['szPOSItemID'].lower()
                csku_id = item['szCommonItemID'].lower()
                sold_qty = item['dTaQty']
                log.info(selling_location_url.format(division=division, store=store, register=register,
                                                    indicator=indicator, fromSellingLocation=fromSellingLocation,
                                                    sku=rsku_id))
                mcs_sales_location = callInvisibilityApi(url)
                sales_sku_dict['Header'] = json_obj['Header']
                sales_sku_dict['Payments'] = json_obj['Payments']
                sales_sku_dict['Deposit'] = json_obj['Deposit']
                sales_sku_dict['LineItem'] = item
                sales_sku_dict['Header']['mcs_sales_location'] = mcs_sales_location

                # log.info('output json: %s', json.dumps(sales_sku_dict))
                query_value = '(\'' + csku_id + '\',\'' + rsku_id + '\',\'' + division + '\',\'' + mcs_sales_location + '\',\'' + store + '\',false,\'' + updated_at + '\',' + sold_qty + ',\'' + txn_nmbr + '\',\'' + json.dumps(
                    sales_sku_dict) + '\')'
                query_values.append(query_value)
        # sales_sku_dict_list.append(func.Document.from_dict(sales_sku_dict))
        values= ','.join(query_values)
        final_query = insert_query+values
        log.info('insert_query: %s',final_query)
        executeCosmosPostgres(final_query)
        log.info('Sent record to cosmos DB')
        return json.dumps(sales_sku_dict)

