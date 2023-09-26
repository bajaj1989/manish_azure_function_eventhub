import logging
import azure.functions as func
import json
from datetime import datetime
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
from shared_code import config
import os

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
log.info("Logging Info")
log.debug("Logging Debug")

timestamp_format='\'YYYY-MM-DD HH24:MI:SS\''
timestamp_format_milli='\'YYYY-MM-DD HH24:MI:SS.US\''

conn =None

def getPostgresConn():
    try:
        conn = psycopg2.connect(
        host=config.postgresSqlConn['host'],
        database=config.postgresSqlConn['database'],
        user=config.postgresSqlConn['user'],
        password=os.environ['postgres_password'])
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        log.error(error)
        raise



def main(events: func.EventHubEvent) -> str:
    return_values = []
    all_values = []
    sales_columns = ['csku_id', 'rsku_id', 'division', 'selling_location_id', 'retail_store_id'
        , 'sold_quantity', 'dh_txn_nmbr', 'ta_type','customer_id','pax_nmbr','loyalty_member_id',
          'transaction_date', 'enqueue_timestamp', 'db_updated_at','trmnl_nbr','ta_crt_nbr','class_id','category_id',
                     'event_details']
    insert_query = '''insert into txn_line_item_tmp 
        (''' + ','.join(sales_columns) + ''') values %s '''
    acceptable_ta_type = config.acceptable_ta_type
    conn = getPostgresConn()
    for event in events:
        enqueued_time = event.enqueued_time
        raw_json_event=event.get_body().decode('utf-8')
        log.info('Python CAN TMP EventHub trigger processed an json event')
        json_obj = json.loads(raw_json_event)
        return_values.append(json_obj)
        if json_obj['Header']['szTaType'] in acceptable_ta_type and len(json_obj['LineItems']) > 0:
            values = {}
            values['division'] = json_obj['Header']['szExternalID']
            values['retail_store_id'] = json_obj['Header']['lRetailStoreID']
            values['ta_type'] = json_obj['Header']['szTaType']
            values['transaction_date'] = datetime.strftime(datetime.strptime(json_obj['Header']['szDate'], '%Y%m%d%H%M%S'),
                                                    '%Y-%m-%d %H:%M:%S')
            values['dh_txn_nmbr'] = json_obj['Header']['dh_txn_nmbr']
            values['customer_id'] = json_obj['Header']['customer_id']
            values['pax_nmbr'] = json_obj['Header']['szPaxNmbr']
            values['loyalty_member_id'] = json_obj['Header']['szLoyaltyMemberID']
            values['trmnl_nbr'] = json_obj['Header']['lWorkstationNmbr']

            for item in json_obj['LineItems']:
                column_values = []
                sales_sku_dict = {}
                values['rsku_id'] = item['szPOSItemID']
                values['csku_id'] = item['szCommonItemID']
                values['sold_quantity'] = item['dTaQty']
                values['selling_location_id'] = item['selling_location_id']
                values['ta_crt_nbr'] = item['lTaCreateNmbr']
                values['class_id'] = item['szClass']
                values['category_id'] = item['szCategory']
                sales_sku_dict['Header'] = json_obj['Header']
                sales_sku_dict['Payments'] = json_obj['Payments']
                sales_sku_dict['Deposit'] = json_obj['Deposit']
                sales_sku_dict['LineItem'] = item

                for column in sales_columns:
                    #log.info('column : %s',column)
                    if column == 'event_details':
                        column_values.append(AsIs('\''+json.dumps(sales_sku_dict)+'\''))
                    elif column == 'enqueue_timestamp':
                        column_values.append(AsIs('to_timestamp(\'{enqueue_timestamp}\',{timestamp_format_milli})'.
                                                    format(enqueue_timestamp=enqueued_time,
                                                            timestamp_format_milli=timestamp_format_milli)))
                    elif column == 'db_updated_at':
                        current_timestamp = str(datetime.now())
                        column_values.append(AsIs('to_timestamp(\'{db_updated_at}\',{timestamp_format_milli})'.
                                                    format(db_updated_at=current_timestamp,
                                timestamp_format_milli=timestamp_format_milli)))
                    elif column == 'transaction_date':
                        column_values.append(AsIs('to_timestamp(\'{updated_at}\',{timestamp_format})'.
                                                    format(updated_at=values['transaction_date'],
                                timestamp_format=timestamp_format)))

                    elif values[column] is not None and values[column].strip() != '':
                        column_values.append(values[column])
                    else:
                        column_values.append(AsIs('null'))

                all_values.append(column_values)

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, insert_query, all_values)
    conn.commit()
    log.info('Inserted records in temp DB')
    merge_query = '''merge into txn_line_item E
        using (select *, row_number() over(partition by division, dh_txn_nmbr, trmnl_nbr, transaction_date, ta_crt_nbr order by ta_crt_nbr) as line_number from txn_line_item_tmp) T

        on  T.division = E.division 
        and T.dh_txn_nmbr = E.dh_txn_nmbr 
        and T.trmnl_nbr = E.trmnl_nbr 
        and T.ta_crt_nbr = E.ta_crt_nbr 
        and T.transaction_date = E.transaction_date
        when not MATCHED
        then Insert ({column_names}) VALUES ({alias_column_name})
        when MATCHED
        then update set {set_columns}'''
    
    column_names = ','.join(sales_columns)
    column_names = column_names + ',line_number'

    alias_column_name = ',T.'.join(sales_columns)
    alias_column_name = 'T.'+alias_column_name + ',T.line_number'

    set_columns_list=[]
    for column_name in sales_columns:
        set_column = column_name + '=T.' + column_name
        set_columns_list.append(set_column)

    set_columns = ','.join(set_columns_list)
    set_columns = set_columns + ',line_number = T.line_number'

    merge_query = merge_query.format(column_names=column_names, alias_column_name=alias_column_name, set_columns=set_columns)
    log.info('Merged Query: %s',merge_query)
    cur = conn.cursor()
    cur.execute(merge_query)
    conn.commit()
    log.info('Merged records to main table')

    emptyTmpTableQuery='delete from txn_line_item_tmp'
    cur = conn.cursor()
    cur.execute(emptyTmpTableQuery)
    conn.commit()
    log.info('Truncated tmp table')

    
    conn.close()

    
    return json.dumps(return_values)



