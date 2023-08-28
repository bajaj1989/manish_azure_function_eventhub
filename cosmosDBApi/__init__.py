import logging

import azure.functions as func
from azure.cosmos import CosmosClient, PartitionKey, errors
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    req_body = req.get_json()
    url=req_body['url']
    key=req_body['key']
    database_name=req_body['database_name']
    container_name=req_body['container_name']
    query_string=req_body['query']
    logging.info('query : %s',query_string)
    #creating cosmosdb client
    client = CosmosClient(url, {'masterKey': key}, user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)

    database = client.get_database_client(database_name)
    container = database.get_container_client(container_name)

    items = list(container.query_items(
    query=query_string
))
    logging.info('type of items : %s',type(items))
    json_obj={}
    json_obj['sku_items']=items

    return func.HttpResponse(json.dumps(json_obj))

