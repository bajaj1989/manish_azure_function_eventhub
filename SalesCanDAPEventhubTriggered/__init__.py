import logging
import azure.functions as func
import json
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
from datetime import datetime
from shared_code import config
import os

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
log.info("Logging Info")
log.debug("Logging Debug")



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

def getMtdTransactionCount(conn,params):
    getCountQuery = '''select count(distinct dh_txn_nmbr) from sales_pos_events {lookupWhereClause}'''
    whereClause = []
    for k,v in params.items():
        if k == 'transaction_date':
            clause = k+' >= \''+v+'\''
        else:
            clause = k+' = \''+v+'\''
        whereClause.append(clause)
    strWhereClause = ''
    if len(whereClause) > 0:
        strWhereClause = ' and '.join(whereClause)

    if len(strWhereClause) >0:
        getCountQuery = getCountQuery.format(lookupWhereClause = 'where '+ strWhereClause)
    
    log.info('getCountQuery :: %s',getCountQuery)
    cur = conn.cursor()
    cur.execute(getCountQuery)
    countList = cur.fetchall()
    return countList[0][0]


def main(events: func.EventHubEvent,dapEventhub: func.Out[str]) -> str:
    return_values=[]
    conn = getPostgresConn()
    for event in events:
        raw_json_event=event.get_body().decode('utf-8')
        log.info('Python EventHub trigger processed an json event')
        #log.info('Python EventHub trigger processed an json event: %s',raw_json_event)
        json_obj = json.loads(raw_json_event)
        acceptable_ta_type = config.acceptable_ta_type
        division_list=config.dap_division_list
        if (( json_obj['Header']['szExternalID'] and json_obj['Header']['szExternalID'].strip())
            and (json_obj['Header']['szTaType'] and json_obj['Header']['szTaType'].strip())
            and (json_obj['Header']['szTaType'] in acceptable_ta_type) 
            and (json_obj['Header']['szExternalID'] in division_list) 
            and (len(json_obj['LineItems']) > 0)):
            salesDapJson={}
            header={}
            header['memberCardNumber'] = json_obj['Header']['szLoyaltyCardNo'] #if (json_obj['Header']['szLoyaltyCardNo'] and json_obj['Header']['szLoyaltyCardNo'].strip()) else ''
            header['passportNumber'] = json_obj['Header']['szPassportNmbr']
            header['posLocationId'] = json_obj['Header']['lRetailStoreID']
            header['division'] = json_obj['Header']['szExternalID']
            header['terminalNumber'] = json_obj['Header']['lWorkstationNmbr']
            header['transactionNumber'] = json_obj['Header']['lTaTypeNmbr']
            header['adjustedFlag'] = 'NA'
            header['type'] = json_obj['Header']['szTransactionTypeID']
            header['retailStoreID'] = json_obj['Header']['lRetailStoreID']
            header['store'] = json_obj['Header']['szRetailStoreName']
            header['transactionDate'] = json_obj['Header']['szDate']
            header['businessDate'] = json_obj['Header']['szBusinessDate']
            header['currencyCode'] = json_obj['Header']['DFS_szPayCurrSym']
            header['localNetAmount'] = json_obj['Header']['discount_summary']['net_amount']
            header['localNetAmountWithoutTax'] = json_obj['Header']['discount_summary']['net_amount']
            header['localTotalTax'] = 'NA'
            header['localSubtotal'] = 'NA'
            header['hostNetAmount'] = 'NA'
            header['hostSubtotal'] = 'NA'
            header['hostTotalTax'] = 'NA'
            header['paxNumber'] = json_obj['Header']['szPaxNmbr']

            params={}
            firstDayOfCurMonth = str(datetime.today().replace(day=1,hour=0,minute=0,second=0,microsecond=0))
            if (json_obj['Header']['szPaxNmbr'] and json_obj['Header']['szPaxNmbr'].strip()) \
            and (json_obj['Header']['szExternalID'] and json_obj['Header']['szExternalID'].strip()):
                params['division'] = json_obj['Header']['szExternalID']
                params['pax_number'] = json_obj['Header']['szPaxNmbr']
                params['transaction_date'] = firstDayOfCurMonth
                header['mtdTransactionCountByPax'] = getMtdTransactionCount(conn,params)
            else:
                header['mtdTransactionCountByPax'] = 'NA'

            params={}
            if (json_obj['Header']['szLoyaltyMemberID'] and json_obj['Header']['szLoyaltyMemberID'].strip()) \
            and (json_obj['Header']['szExternalID'] and json_obj['Header']['szExternalID'].strip()):
                params['division'] = json_obj['Header']['szExternalID']
                params['lylty_mbr_id'] = json_obj['Header']['szLoyaltyMemberID']
                params['transaction_date'] = firstDayOfCurMonth
                header['mtdTransactionCountByMemberId'] = getMtdTransactionCount(conn,params)
            else:
                header['mtdTransactionCountByMemberId'] = 'NA'

            
            header['mtdTransactionCountByMemberId'] = 'TBD'
            header['loyaltMemberId'] = json_obj['Header']['szLoyaltyMemberID']
            header['daigouMemberFlag'] = 'TBD'
            header['memberTypeFlag'] = 'TBD'
            header['memberTier'] = 'TBD'
            header['exchangeFlag'] = 'TBD'
            header['originalPosLocationId'] = json_obj['Header']['lOrgRetailStoreID']
            header['originalTerminalNumber'] = json_obj['Header']['lOrgWorkstationNmbr']
            header['originalTransactionNumber'] = json_obj['Header']['lOriginTaNmbr']
            header['originalTransactionBusinessDate'] = 'NA'
            header['originalTransactionDivision'] = 'NA'

            lineItems=[]
            for item in json_obj['LineItems']:
                lineItem={}
                lineItem['lineNumber'] = 'NA'
                lineItem['csku'] = item['szCommonItemID']
                lineItem['unitPrice'] = item['dTaPrice']
                lineItem['quantity'] = item['dTaQty']
                lineItem['extendedRetail'] = item['dTaTotal']
                lineItem['localNetAmount'] = item['dTaTotal']
                lineItem['hostNetAmount'] = 'NA'
                lineItem['localTax'] = 'TBD'
                lineItem['hostTax']= 'NA'
                lineItem['sellingLoc'] = item['selling_location_id']
                lineItem['dutyType'] = item['szDutyType']
                lineItems.append(lineItem)

            salesDapJson['Header'] = header
            salesDapJson['LineItems'] = lineItems
            return_values.append(salesDapJson)


    dapEventhub.set(json.dumps(return_values))
