import logging

import azure.functions as func
import config
import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import json
import requests
import psycopg2
from datetime import datetime

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
log.info("Logging Info")
log.debug("Logging Debug")
timestamp_format='\'YYYY-MM-DD HH24:MI:SS\''
timestamp_format_milli='\'YYYY-MM-DD HH24:MI:SS.US\''

def executeCosmosPostgres(queries):
    try:
        conn = psycopg2.connect(
        host="c-dfs-aass-dp-nprd-cosmospostgres-01.wwvvotg4x7fdz4.postgres.cosmos.azure.com",
        database="ToDo",
        user="citus",
        password="Admin@1234")
        cur = conn.cursor()
        for query in queries:
            log.info('Running query: %s', query)
            cur.execute(query)
            conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        log.error('Error in SQL: %s',error)
        raise
    finally:
        if conn is not None:
            conn.close()    


def getMergeQuery(json_obj,enqueue_timestamp):
    merge_query='''merge into mcs_events E
        using (select {select_columns}) T
        on E.csku_id = T.csku_id 
        and E.rsku_id = T.rsku_id 
        and E.retail_store_id = T.retail_store_id
        and E.Selling_Location_ID = T.Selling_Location_ID 
        and E.MCS_Region_ID = T.MCS_Region_ID 
        and E.updated_at < T.updated_at
        when not MATCHED
        then Insert ({column_names}) VALUES ({alias_column_name})
        when MATCHED
        then update set {set_columns}'''
    
    select_columns_list=[]
    column_names_list=[]
    alias_column_name_list=[]
    set_columns_list=[]
    #join_keys=['CSKU_ID','RSKU_ID','MCS_Region_ID','Retail_Store_ID','Selling_Location_ID']
    integer_columns=['On_Hand_Units','Merchandise_Reserve_Units','Pick_Reserve_Units','Damage_Units','Lost_Found_Units','AFS_Units']
    for k,v in json_obj.items():
        if k in integer_columns:
            select_column = 'cast(\''+v+'\' as integer) as '+k
        else:
            select_column = '\''+v+'\' as '+k
        column=k
        alias_column='T.'+k
        set_column = k+'=T.'+k
        select_columns_list.append(select_column)
        column_names_list.append(column)
        alias_column_name_list.append(alias_column)
        set_columns_list.append(set_column)
    select_columns = ','.join(select_columns_list)
    select_columns = select_columns +',to_timestamp({updated_at},{timestamp_format}) as updated_at, to_timestamp({eod_completed_at},{timestamp_format}) as eod_completed_at,to_timestamp(\'{enqueue_timestamp}\',{timestamp_format_milli}) as enqueue_timestamp'

    column_names = ','.join(column_names_list)
    column_names=column_names+',updated_at,eod_completed_at,enqueue_timestamp'

    alias_column_name = ','.join(alias_column_name_list)
    alias_column_name  =alias_column_name+',T.updated_at,T.eod_completed_at,T.enqueue_timestamp'

    set_columns = ','.join(set_columns_list)
    set_columns = set_columns+',updated_at = T.updated_at, eod_completed_at = T.eod_completed_at,enqueue_timestamp =T.enqueue_timestamp'

    updated_at_str = datetime.strftime(datetime.strptime(json_obj['MCS_Inventory_Date']+json_obj['MCS_Inventory_Time'],'%d-%m-%Y%H:%M:%S'),'%Y-%m-%d %H:%M:%S')
    updated_at = '\''+updated_at_str+'\''
    if json_obj['MCS_Inventory_Type'].lower() in 'eod':
        select_columns = select_columns.format(timestamp_format_milli=timestamp_format_milli,timestamp_format =timestamp_format,updated_at=updated_at, eod_completed_at=updated_at, enqueue_timestamp=enqueue_timestamp)
    else:
        select_columns = select_columns.format(timestamp_format_milli=timestamp_format_milli,timestamp_format =timestamp_format,updated_at=updated_at, eod_completed_at='null',enqueue_timestamp=enqueue_timestamp)

    merge_query = merge_query.format(select_columns=select_columns,column_names=column_names,alias_column_name=alias_column_name,set_columns=set_columns)
    return merge_query

def main(events: func.EventHubEvent):
    for event in events:
        enqueue_timestamp = event.enqueued_time
        json_event=event.get_body().decode('utf-8')
        log.info('Python EventHub trigger processed an json event: %s',
                    json_event)
        json_obj = json.loads(json_event)
        queries=[]
        queries.append(getMergeQuery(json_obj,enqueue_timestamp))
        executeCosmosPostgres(queries)


