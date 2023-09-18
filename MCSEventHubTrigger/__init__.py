import logging

import azure.functions as func
import config
import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import json
import psycopg2
from datetime import datetime
import psycopg2.extras
from psycopg2.extensions import AsIs

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
log.info("Logging Info")
log.debug("Logging Debug")
timestamp_format='\'YYYY-MM-DD HH24:MI:SS\''
timestamp_format_milli='\'YYYY-MM-DD HH24:MI:SS.US\''


def main(events: func.EventHubEvent):
    log.info("processing started: %d", len(events))

    first_event = events[0]
    first_event_json = json.loads(first_event.get_body().decode('utf-8'))

    merge_query = '''merge into mcs_events E
        using (select * from temp_mcs_events) T
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

    column_names_list = []
    alias_column_name_list = []
    set_columns_list = []
    
    integer_columns = ['On_Hand_Units','Merchandise_Reserve_Units','Pick_Reserve_Units','Damage_Units','Lost_Found_Units','AFS_Units']

    for k, _ in first_event_json.items():
        
        column = k
        alias_column = 'T.' + k
        set_column = k + '=T.' + k
        column_names_list.append(column)
        alias_column_name_list.append(alias_column)
        set_columns_list.append(set_column)

    column_names = ','.join(column_names_list)
    column_names = column_names + ',updated_at,eod_completed_at,enqueue_timestamp'

    alias_column_name = ','.join(alias_column_name_list)
    alias_column_name = alias_column_name + ',T.updated_at,T.eod_completed_at,T.enqueue_timestamp'

    set_columns = ','.join(set_columns_list)
    set_columns = set_columns + ',updated_at = T.updated_at, eod_completed_at = T.eod_completed_at, enqueue_timestamp = T.enqueue_timestamp'

    #if json_obj['MCS_Inventory_Type'].lower() in 'eod':
    #    select_columns = select_columns.format(timestamp_format_milli=timestamp_format_milli,timestamp_format =timestamp_format,updated_at=updated_at, eod_completed_at=updated_at, enqueue_timestamp=enqueue_timestamp)
    #else:
    #    select_columns = select_columns.format(timestamp_format_milli=timestamp_format_milli,timestamp_format =timestamp_format,updated_at=updated_at, eod_completed_at='null',enqueue_timestamp=enqueue_timestamp)

    merge_query = merge_query.format(column_names=column_names,alias_column_name=alias_column_name,set_columns=set_columns)
    log.info("merge query: %s", merge_query)

    values=[]
    log.info("column_names: %s", column_names)

    for event in events:
       enqueued_time = event.enqueued_time
       event_json = json.loads(event.get_body().decode('utf-8'))
       column_values = []

       updated_at = datetime.strftime(datetime.strptime(event_json['MCS_Inventory_Date'] + event_json['MCS_Inventory_Time'],'%d-%m-%Y%H:%M:%S'),'%Y-%m-%d %H:%M:%S')

       for column_name in column_names.split(','):
           #log.info('processing %s', column_name)
           if column_name == 'updated_at':
               column_values.append(AsIs('to_timestamp(\'{updated_at}\',{timestamp_format})'.format(updated_at = updated_at, timestamp_format = timestamp_format)))
           elif column_name == 'eod_completed_at':
               if event_json['MCS_Inventory_Type'].lower() in 'eod':
                   column_values.append(AsIs('to_timestamp(\'{eod_completed_at}\',{timestamp_format})'.format(eod_completed_at = updated_at, timestamp_format = timestamp_format)))
               else:
                   column_values.append(AsIs('null'))
           elif column_name == 'enqueue_timestamp':
               column_values.append(AsIs('to_timestamp(\'{enqueue_timestamp}\',{timestamp_format_milli})'.format(enqueue_timestamp = enqueued_time, timestamp_format_milli = timestamp_format_milli)))
           else:
               if k in integer_columns:
                   column_value = int(event_json[column_name])
               else:
                   column_value = event_json[column_name]
               column_values.append(column_value)
       #log.info('record9: %s', str(column_values))
       values.append(column_values)
           
    sql = """
        INSERT INTO temp_mcs_events (Inventory_Sequence_ID,MCS_Region_ID,Retail_Store_ID,Selling_Location_ID,CSKU_ID,RSKU_ID,On_Hand_Units,Merchandise_Reserve_Units,Pick_Reserve_Units,Damage_Units,Lost_Found_Units,AFS_Units,MCS_Inventory_Date,MCS_Inventory_Time,MCS_Inventory_Type,updated_at,eod_completed_at,enqueue_timestamp)
        VALUES %s
        """
    
    try:
        conn = psycopg2.connect(
        host="c-dfspoc-cosmos.nuc5wioti7optz.postgres.cosmos.azure.com",
        database="citus",
        user="citus",
        password="adminTest@123")
        cur = conn.cursor()

        temp_table_query = """
                            create temp table temp_mcs_events (
                            Inventory_Sequence_ID text,
                            MCS_Region_ID text,
                            Retail_Store_ID text,
                            Selling_Location_ID text,
                            CSKU_ID text,
                            RSKU_ID text,
                            On_Hand_Units integer,
                            Merchandise_Reserve_Units integer,
                            Pick_Reserve_Units integer,
                            Damage_Units integer,
                            Lost_Found_Units integer,
                            AFS_Units integer,
                            MCS_Inventory_Date text,
                            MCS_Inventory_Time text,
                            MCS_Inventory_Type text,
                            updated_at timestamp,
                            eod_completed_at timestamp,
                            enqueue_timestamp timestamp,
                            rcrd_updated_timestamp timestamp default now()	
                            )
                        """
        cur.execute(temp_table_query)
        
        psycopg2.extras.execute_values(cur, sql, values)

        cur.execute(merge_query)

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        log.error('Error in SQL: %s',error)
        raise
    finally:
        if conn is not None:
            conn.close()

    log.info("processing completed")


