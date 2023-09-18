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
import psycopg2.extras
from psycopg2.extensions import AsIs
from collections import OrderedDict
import hashlib
from lxml import etree

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
log.info("Logging Info")
log.debug("Logging Debug")

timestamp_format='\'YYYY-MM-DD HH24:MI:SS\''
timestamp_format_milli='\'YYYY-MM-DD HH24:MI:SS.US\''
custLookupColumns=['customer_id','division','loyalty_member_id','pax_nmbr','passport_nmbr']

conn =None
def segregate(raw_sales_event):
    data = json.loads(raw_sales_event)
    data_backup = json.loads(raw_sales_event)
    splits = []

    # -----------------HEADER-LOGIC--------------------#
    mylist = ['total', 'subtotal', 'guaranteeCard', 'override', 'customer', 'footer', 'taxIncluded',
            'taxExcluded', 'recallReceipt', 'reprintTa', 'storeReceipt', 'loyaltyAccountReturn', 'serialized',
            'coupon']
    notreq = []
    req_tags = ['pluNotFound']
    finaltags = ['header', 'line', 'payment']
    for i in mylist:
        if i in data.keys():
            data['header'][i] = data[i]

    # ------- Add DepositIn/Out to Header/Deposit As Array
    # data['header']['deposit'] = []
    # depositTags = ['depositIn','depositOut']
    # for i in depositTags:
    #	if i in data.keys():
    #		data['header']['deposit'].append(data.get(i,{}))

    data['Deposit'] = {}
    depositChildTags = ['depositIn', 'depositOut', 'depositReturn', 'depositPayment']
    for dep in depositChildTags:
        if dep in data.keys():
            data['Deposit'][dep] = data.get(dep, {})

    # -----------------HEADER-LOGIC-ENDS-HERE---------------#

    # ------Adding-comment-tag-as-an-array--into--associated-tags---#
    # where tags parent tags are not an array-----#
    comment_list = ['abortReceipt', 'voidReceipt', 'transactionType', 'header']

    for i in comment_list:
        if i in data.keys():
            data[i]['comment'] = []

    # ------Adding-comment-tags-for-artsale,artreturn------#
    comments_array = ['artReturn', 'artSale']
    for i in comments_array:
        if i in data.keys():
            for j in range(len(data[i])):
                data[i][j]['comment'] = []

    # ------Adding-addonDialog-tags-for-artsale,artreturn------#
    addon_array = ['artReturn', 'artSale', 'artLineVoid']
    for i in addon_array:
        if i in data.keys():
            for j in range(len(data[i])):
                data[i][j]['addonDialog'] = []

    # ------------blank list for comment at manual-discount----------------#
    man_com = []

    # ------------------------LINE-LOGIC----------------------#
    if 'comment' in data.keys():
        for i in range(len(data.get('comment', []))):
            if 'lTaRefToCreateNmbr' in data.get('comment', [{}])[i].get('hdr', {}).keys():
                refno = data.get('comment', [{}])[i].get('hdr', {}).get('lTaRefToCreateNmbr')
                refobj = data.get('comment', [{}])[i].get('hdr', {}).get('szTaRefTaObject')
                # Associating comment with referred object
                if (refobj == 'ART_SALE'):
                    for j in range(len(data.get('artSale', []))):
                        if (refno == data.get('artSale', [{}])[j].get('hdr', {}).get('lTaCreateNmbr')):
                            data['artSale'][j]['comment'].append(data.get('comment', [{}])[i])
                elif (refobj == 'MANUAL_DISCOUNT'):
                    for k in range(len(data.get('manualDiscount', []))):
                        if 'lTaRefToCreateNmbr' in data.get('manualDiscount', [{}])[k].get('hdr', {}).keys():
                            if (refno == data.get('manualDiscount', [{}])[k].get('hdr', {}).get(
                                    'lTaCreateNmbr')):
                                art_ref = data.get('manualDiscount', [{}])[k].get('hdr', {}).get(
                                    'lTaRefToCreateNmbr')
                                if 'discInfo' in data.keys():
                                    for discountinfo in range(len(data.get('discInfo', []))):
                                        if 'lTaRefToCreateNmbr' in data.get('discInfo', [{}])[discountinfo].get(
                                                'hdr', {}).keys():
                                            refnnn = data.get('discInfo', [{}])[discountinfo].get('hdr',
                                                                                                {}).get(
                                                'lTaRefToCreateNmbr')
                                            if (art_ref == refnnn):
                                                man_com.append(data.get('comment', [{}])[i])
                                                data['discInfo'][discountinfo]['comment'] = man_com
                                                man_com = []
                elif (refobj == 'VOID_RECEIPT'):
                    if (refno == data.get('voidReceipt', {}).get('hdr', {}).get('lTaCreateNmbr')):
                        data['voidReceipt']['comment'].append(data.get('comment', [{}])[i])
                elif (refobj == 'ABORT_RECEIPT'):
                    if (refno == data.get('abortReceipt', {}).get('hdr', {}).get('lTaCreateNmbr')):
                        data['abortReceipt']['comment'].append(data.get('comment', [{}])[i])
                elif (refobj == 'ART_RETURN'):
                    for n in range(len(data.get('artReturn', []))):
                        if (refno == data.get('artReturn', [{}])[n].get('hdr', {}).get('lTaCreateNmbr')):
                            data['artReturn'][n]['comment'].append(data.get('comment', [{}])[i])
                elif (refobj == 'TRANSACTION_TYPE'):
                    if (refno == data.get('transactionType', {}).get('hdr', {}).get('lTaCreateNmbr')):
                        data['transactionType']['comment'].append(data.get('comment', [{}])[i])
            elif 'lTaRefToCreateNmbr' not in data.get('comment', [{}])[i].get('hdr', {}).keys():
                # Header level comment move into header
                data['header']['comment'].append(data.get('comment', [{}])[i])

    # --------------deleting comment--------------#
    # del data['comment']

    # --SEGREGATING-ADD-ON-DIALOG--#

    if 'addonDialog' in data.keys():
        if (type(data.get('addonDialog')) == list):
            for i in range(len(data.get('addonDialog', []))):
                if 'lTaRefToCreateNmbr' in data.get('addonDialog', [{}])[i].get('hdr', {}).keys():
                    refno = data.get('addonDialog', [{}])[i].get('hdr', {}).get('lTaRefToCreateNmbr')
                    refobj = data.get('addonDialog', [{}])[i].get('hdr', {}).get('szTaRefTaObject')
                    # Associating comment with referred object
                    if (refobj == 'ART_SALE'):
                        for j in range(len(data.get('artSale', []))):
                            if (refno == data.get('artSale', [{}])[j].get('hdr', {}).get('lTaCreateNmbr')):
                                data['artSale'][j]['addonDialog'].append(data.get('addonDialog', [])[i])
                    if (refobj == 'ART_RETURN'):
                        for k in range(len(data.get('artReturn', []))):
                            if (refno == data.get('artReturn', [{}])[k].get('hdr', {}).get('lTaCreateNmbr')):
                                data['artReturn'][k]['addonDialog'].append(data.get('addonDialog', [])[i])
                    if (refobj != 'ART_SALE' and refobj != 'ART_RETURN'):
                        if 'artLineVoid' in data.keys():
                            for l in range(len(data.get('artLineVoid', []))):
                                if (refno == data.get('artLineVoid', [{}])[l].get('hdr', {}).get(
                                        'lTaCreateNmbr')):
                                    data['artLineVoid'][l]['addonDialog'].append(data.get('addonDialog', [])[i])

    # ---SEGREGATION-ENDS-HERE--#

    # ----ADDONDIALOG---LOGIC----(WHERE ADDONDIALOG IS LIST )-#
    # ----MAKING-KEY-VALUE-FROM INDIRECTLY-RELATED-TAGS--#
    # ---if--addonDialog tag is object
    addon_checks = ['artSale', 'artReturn', 'artLineVoid']
    for addon_check in addon_checks:
        if addon_check in data.keys():
            for art in range(len(data.get(addon_check, []))):
                add_temp_dict = {}
                if 'addonDialog' in data.get(addon_check, [{}])[art].keys():
                    if (len(data.get(addon_check, [{}])[art].get('addonDialog')) != 0):
                        for addlen in range(len(data.get(addon_check, [{}])[art].get('addonDialog', []))):
                            for add in data.get(addon_check, [{}])[art].get('addonDialog', [{}])[addlen].keys():
                                if 'szAddOnText' in add:
                                    add_text_val = data.get(addon_check, [{}])[art].get('addonDialog', [{}])[
                                        addlen].get(add)
                                    add_input_val = data.get(addon_check, [{}])[art].get('addonDialog', [{}])[
                                        addlen].get('szAddOnInput' + str(add[11:]))
                                    add_temp_dict.update({add_text_val: add_input_val})
                                if 'szAddOnDialog' in add:
                                    add_temp_dict.update({'szAddOnDialog': data.get(addon_check, [{}])[art].get(
                                        'addonDialog', [{}])[addlen].get('szAddOnDialog')})
                    data[addon_check][art]['addonDialog'] = add_temp_dict

    # --------MAKING K-V-ENDS-HERE---#

    # --------Add-VoidReciept,abortReciept-&-Transaction-type-into-Header---#
    pendingHeader = ['abortReceipt', 'voidReceipt', 'transactionType']

    for i in pendingHeader:
        if i in data.keys():
            data['header'][i] = data[i]

    # ----------------HEADER-LEVEL-TAX-LOGIC------------------#

    # ---------Adding-type-taxExcluded and taxIncluded-------#
    type_include_exlude = ['taxExcluded', 'taxIncluded']

    for inc_exc in type_include_exlude:
        if inc_exc in data.get('header', {}).keys():
            for inc_exc_array in range(len(data.get('header', {}).get(inc_exc, []))):
                data['header'][inc_exc][inc_exc_array]['type'] = inc_exc

    # ------------------------------------------------------#

    data['header']['TAX'] = []

    if 'taxExcluded' in data.get('header', {}).keys():
        data['header']['TAX'] = data.get('header', {}).get('taxExcluded')
        del data['header']['taxExcluded']

    if 'taxIncluded' in data.get('header', {}).keys():
        data['header']['TAX'] = data.get('header', {}).get('taxIncluded')
        del data['header']['taxIncluded']

    # ---------------taxArt logic------------------#
    if 'taxArt' in data.keys():
        for i in range(len(data.get('taxArt', []))):
            if 'lTaRefToCreateNmbr' in data.get('taxArt', [{}])[i].get('hdr', {}).keys():
                refno = data.get('taxArt', [{}])[i].get('hdr', {}).get('lTaRefToCreateNmbr')
                refobj = data.get('taxArt', [{}])[i].get('hdr', {}).get('szTaRefTaObject')
                if (refobj == 'ART_SALE'):
                    for j in range(len(data.get('artSale', []))):
                        if (refno == data.get('artSale', [{}])[j].get('hdr', {}).get('lTaCreateNmbr')):
                            data['artSale'][j]['taxArt'] = data.get('taxArt', [{}])[i]
                if (refobj == 'ART_RETURN'):
                    for j in range(len(data.get('artReturn', []))):
                        if (refno == data.get('artReturn', [{}])[j].get('hdr', {}).get('lTaCreateNmbr')):
                            data['artReturn'][j]['taxArt'] = data.get('taxArt', [{}])[i]

    # Adding line as list
    data['line'] = []

    # adding artsale, artReturn, artLineVoid to line---#
    if 'artSale' in data.keys():
        for saletype in range(len(data.get('artSale', []))):
            data['artSale'][saletype]['type'] = 'artSale'

        for sale in range(len(data.get('artSale', []))):
            data['line'].append(data.get('artSale', [{}])[sale])

    if 'artReturn' in data.keys():
        for returntype in range(len(data.get('artReturn', []))):
            data['artReturn'][returntype]['type'] = 'artReturn'

        for artreturnn in range(len(data.get('artReturn', []))):
            data['line'].append(data.get('artReturn', [{}])[artreturnn])

    if 'artLineVoid' in data.keys():
        for voidtype in range(len(data.get('artLineVoid', []))):
            data['artLineVoid'][voidtype]['type'] = 'artLineVoid'

        for artvoid in range(len(data.get('artLineVoid', []))):
            data['line'].append(data.get('artLineVoid', [{}])[artvoid])
    # ------------------------------------------------#

    # -----------LINE-LOGIC-ENDS-HERE-----------------#

    # -------------PAYMENT LOGIC-------------------#

    # ------TAGGING LOYALTY-POINTS-REDEEM-TO-MEDIA---#
    if 'loyaltyPointsRedeem' in data.keys():
        if 'lTaRefToCreateNmbr' in data.get('loyaltyPointsRedeem', {}).get('hdr', {}).keys():
            refno = data.get('loyaltyPointsRedeem', {}).get('hdr', {}).get('lTaRefToCreateNmbr')
            refobj = data.get('loyaltyPointsRedeem', {}).get('hdr', {}).get('szTaRefTaObject')
            if (refobj == 'MEDIA'):
                for j in range(len(data.get('media', []))):
                    if (refno == data.get('media', [{}])[j].get('hdr', {}).get('lTaCreateNmbr')):
                        data['media'][j]['loyaltyPointsRedeem'] = data.get('loyaltyPointsRedeem')
                    # del data['loyaltyPointsRedeem']

    # ------TAGGING--eftInfo-TO-MEDIA---(LIST)#

    if 'eftInfo' in data.keys():
        for ef in range(len(data.get('eftInfo', []))):
            if 'lTaRefToCreateNmbr' in data.get('eftInfo', [{}])[ef].get('hdr', {}).keys():
                refno = data.get('eftInfo', [{}])[ef].get('hdr', {}).get('lTaRefToCreateNmbr')
                refobj = data.get('eftInfo', [{}])[ef].get('hdr', {}).get('szTaRefTaObject')
                if (refobj == 'MEDIA'):
                    for j in range(len(data.get('media', []))):
                        if (refno == data.get('media', [{}])[j].get('hdr', {}).get('lTaCreateNmbr')):
                            data['media'][j]['eftInfo'] = data.get('eftInfo', [])[ef]

    # ---ADDING--PAYMENT-TAG----#
    data['payment'] = []

    # ----Add--type-to-media----#
    if 'media' in data.keys():
        for pay in range(len(data['media'])):
            data['media'][pay]['type'] = 'payment'

    # ----Add--type-to-mediaVoid----#
    if 'mediaVoid' in data.keys():
        for vpay in range(len(data['mediaVoid'])):
            data['mediaVoid'][vpay]['type'] = 'voidpayment'

    # ----Add--type-to-depositPayment-for-struct-type----#
    # if('depositPayment' in data.keys() and type(data.get('depositPayment','')) == dict):
    #	data['depositPayment']['type'] = 'depositPayment'

    # ----Add--type-to-depositPayment-for-List-type----#
    # if('depositPayment' in data.keys() and type(data.get('depositPayment','')) == list):
    #	for depositPayment in range(len(data['depositPayment'])):
    #		data['depositPayment'][depositPayment]['type'] = 'depositPayment'

    # -=---move-media--into--payment---#
    if 'media' in data.keys():
        for mpay in range(len(data['media'])):
            data['payment'].append(data.get('media', [{}])[mpay])

    # -=---move-mediaVoid--into--payment---#
    if 'mediaVoid' in data.keys():
        for voidpaymedia in range(len(data['mediaVoid'])):
            data['payment'].append(data.get('mediaVoid', [{}])[voidpaymedia])

    # -----Move-depositPayment--into--Payment-for-Struct--#
    # if('depositPayment' in data.keys() and type(data.get('depositPayment','')) == dict):
    #	data['payment'].append(data.get('depositPayment', {}))

    # -----Move-depositPayment--into--Payment-for-List--#
    # if('depositPayment' in data.keys() and type(data.get('depositPayment','')) == list):
    #	for depositPaymentArr in range(len(data['depositPayment'])):
    #		data['payment'].append(data.get('depositPayment', [{}])[depositPaymentArr])

    # ----------PAYMENT-LOGIC-ENDS-HERE------------#

    # ---------DISCOUNT-LOGIC-(SEGREGATED)----------#

    # -----adding-discount-key-as-list-for-each-LineItem------#
    for line in range(len(data.get('line', []))):
        data['line'][line]['Discount'] = []

    # -----adding-discount-key-as-list-for-each-Header------#
    data['header']['Discount'] = []

    if 'discInfo' in data.keys():
        for i in range(len(data.get('discInfo', []))):
            if 'lTaRefToCreateNmbr' in data.get('discInfo', [{}])[i].get('hdr', {}).keys():
                refobb = data.get('discInfo', [{}])[i].get('hdr', {}).get('szTaRefTaObject')
                refnn = data.get('discInfo', [{}])[i].get('hdr', {}).get('lTaRefToCreateNmbr')
                if (refobb == 'ART_SALE'):
                    for line in range(len(data.get('line', []))):
                        if (refnn == data.get('line', [{}])[line].get('hdr', {}).get('lTaCreateNmbr')):
                            data['line'][line]['Discount'].append(data.get('discInfo', [])[i])
                if (refobb == 'ART_RETURN'):
                    for line in range(len(data.get('line', []))):
                        if (refnn == data.get('line', [{}])[line].get('hdr', {}).get('lTaCreateNmbr')):
                            data['line'][line]['Discount'].append(data.get('discInfo', [])[i])

            else:
                for j in range(len(data.get('serialized', []))):
                    if (data.get('serialized', [{}])[j].get('hdr', {}).get('lTaRefToCreateNmbr') ==
                            data.get('discInfo', [{}])[i].get('hdr', {}).get('lTaCreateNmbr')):
                        data['header']['Discount'].append(data.get('discInfo', [])[i])

    # -----------DISCOUNT-LOGIC-ENDS-HERE-----------#

    # --------------Rename all--------------#
    if 'header' in data.keys():
        data['Header'] = data.pop('header')
    if 'line' in data.keys():
        data['LineItem'] = data.pop('line')
    if 'payment' in data.keys():
        data['Payment'] = data.pop('payment')

    # ---------------ROW-KEY-FORMATION----------------#

    ret_store = data.get('Header', {}).get('lRetailStoreID', '')
    work_station = data.get('Header', {}).get('lWorkstationNmbr', '')
    row_key = ""


    # default max length

    def row_key_func(row):
        MAX_LENGTH = 5
        temp_length = ""
        diff_length = MAX_LENGTH - len(row)
        for d in range(diff_length):
            temp_length += "0"

        row = temp_length + row
        return row


    # prefix zero on work_station if length is not max
    work_station = row_key_func(work_station)

    # check due to art_return type of file
    if 'lTaTypeNmbr' in data.get('Header', {}).keys():
        ta_type = data.get('Header', {}).get('lTaTypeNmbr')
        if (ta_type != None):
            # prefix zero on ta_type if length is not max
            ta_type = row_key_func(ta_type)
            # concatinating row key
            row_key = ret_store + work_station + ta_type

    if (len(row_key) == 0):
        if 'reprintTa' in data.get('Header', {}).keys():
            if 'lTaTypeNmbr' in data.get('Header', {}).get('reprintTa', {}).keys():
                reprint_ta_type = data.get('Header', {}).get('reprintTa', {}).get('lTaTypeNmbr')
                if (reprint_ta_type != None):
                    reprint_ta_type = row_key_func(reprint_ta_type)
                    row_key = ret_store + work_station + reprint_ta_type

    if (len(row_key) == 0):
        if 'lTaNmbr' in data.get('Header', {}).keys():
            lta_number = data.get('Header', {}).get('lTaNmbr')
            if (lta_number != None):
                lta_number = row_key_func(lta_number)
                row_key = ret_store + work_station + lta_number

    if (len(row_key) == 0):
        row_key = ret_store + work_station

    # rowkey at header level
    data['Header']['dh_txn_nmbr'] = row_key

    # rowkey at Line level
    if 'LineItem' in data.keys():
        for line in range(len(data.get('LineItem', []))):
            data['LineItem'][line]['dh_txn_nmbr'] = row_key

    # rowkey at Payment level
    if 'Payment' in data.keys():
        for pay in range(len(data.get('Payment', []))):
            data['Payment'][pay]['dh_txn_nmbr'] = row_key

    # rowkey at Header level discount
    if 'Discount' in data.get('Header', {}).keys():
        for diss in range(len(data.get('Header', {}).get('Discount', []))):
            data['Header']['Discount'][diss]['dh_txn_nmbr'] = row_key

    # rowkey at Line level discount
    if 'LineItem' in data.keys():
        for lineitem in range(len(data.get('LineItem', []))):
            if 'Discount' in data.get('LineItem', [{}])[lineitem].keys():
                for i in range(len(data.get('LineItem', [])[lineitem].get('Discount', []))):
                    data['LineItem'][lineitem]['Discount'][i]['dh_txn_nmbr'] = row_key

    # ------------ROW-KEY-FORMATION-ENDS-HERE-------------#

    # ----MOVE COMMENT FROM VOID_RECEIPT---INTO HEADER--COMMENT-----#

    if 'voidReceipt' in data.get('Header', {}).keys():
        if 'comment' in data.get('Header', {}).get('voidReceipt', {}).keys():
            for void in range(len(data.get('Header', {}).get('voidReceipt', {}).get('comment', []))):
                data.get('Header', {}).get('comment', []).append(
                    data.get('Header', {}).get('voidReceipt', {}).get('comment', [{}])[void])

    # ----MOVE COMMENT FROM ABORT_RECEIPT---INTO HEADER--COMMENT-----#

    if 'abortReceipt' in data.get('Header', {}).keys():
        if 'comment' in data.get('Header', {}).get('abortReceipt', {}).keys():
            for abort in range(len(data.get('Header', {}).get('abortReceipt', {}).get('comment', []))):
                data.get('Header', {}).get('comment', []).append(
                    data.get('Header', {}).get('abortReceipt', {}).get('comment', [{}])[abort])

    # ----MOVE COMMENT FROM TRANSACTION_TYPE---INTO HEADER--COMMENT-----#

    if 'transactionType' in data.get('Header', {}).keys():
        if 'comment' in data.get('Header', {}).get('transactionType', {}).keys():
            for tran in range(len(data.get('Header', {}).get('transactionType', {}).get('comment', []))):
                data.get('Header', {}).get('comment', []).append(
                    data.get('Header', {}).get('transactionType', {}).get('comment', [{}])[tran])

    # ---------------------SUMMARY--LOGIC-----------------#

    # VARIABLES
    total_actual_amount = 0
    total_discount = 0
    net_amount = 0

    # ---------Adding summary_discount to JSON----------#
    data['Header'].update(
        OrderedDict({'discount_summary': {'total_actual_amount': 0, 'total_discount': 0, 'net_amount': 0}}))

    # to check LineItem type before calculation.
    summary_check = ['artSale', 'artReturn']
    for items in range(len(data.get("LineItem", []))):
        if (data.get("LineItem", [{}])[items].get('type', 'default') in summary_check):
            total_actual_amount += float(data.get("LineItem", [{}])[items].get("dTaTotal", 0))
            for itemdisc in range(len(data.get("LineItem", [{}])[items].get("Discount", []))):
                total_discount += float(
                    data.get("LineItem", [{}])[items].get("Discount", [{}])[itemdisc].get("dTotalDiscount", 0))

    # Calculating NetAmount (using + sym because discount is already -)
    if (total_actual_amount < 0 and total_discount < 0):
        net_amount = total_actual_amount - total_discount
    else:
        net_amount = total_actual_amount + total_discount

    data['Header'].update(OrderedDict({'discount_summary': OrderedDict({})}))

    # ----INJESTING--INTO--JSON-------#
    data['Header']['discount_summary']['total_actual_amount'] = str(total_actual_amount)
    data['Header']['discount_summary']['total_discount'] = str(total_discount)
    data['Header']['discount_summary']['net_amount'] = str(net_amount)

    # ----SUMMARY--LOGIC--ENDS--HERE--------#

    # TYPE at Line level discount
    if 'LineItem' in data.keys():
        for lineitem in range(len(data.get('LineItem', []))):
            if 'Discount' in data.get('LineItem', [{}])[lineitem].keys():
                for i in range(len(data.get('LineItem', [{}])[lineitem].get('Discount', []))):
                    data['LineItem'][lineitem]['Discount'][i]['discountLevel'] = "Item"

    #print(json.dumps(data))
    return data


def flatten(segregated_json):
    data = segregated_json
    splits = []

    # -----FLATTEN---HEADER-----#

    final_flatten = OrderedDict()
    flat_header = OrderedDict()
    flat_header.update({'Header': {}})
    flat_header['Header'] = OrderedDict()
    try:
        header_list = [
            {"dh_txn_nmbr": data.get("Header", {}).get("dh_txn_nmbr")},
            {"szExternalID": data.get("Header", {}).get("szExternalID")},
            {"lOperatorID": data.get("Header", {}).get("lOperatorID")},
            {"lRetailStoreID": data.get("Header", {}).get("lRetailStoreID")},
            {"lWorkstationNmbr": data.get("Header", {}).get("lWorkstationNmbr")},
            {"szBusinessDate": data.get("Header", {}).get("szBusinessDate")},
            {"szDate": data.get("Header", {}).get("footer", {}).get("szDate")},
            {"szStartTime": data.get("Header", {}).get("szDate")},
            {"lTaNmbr": data.get("Header", {}).get("lTaNmbr")},
            {"szHeaderLineExpand": data.get("Header", {}).get("szHeaderLineExpand")},
            {"lTaTypeNmbr": data.get("Header", {}).get("lTaTypeNmbr")},
            {"szEmplName": data.get("Header", {}).get("szEmplName")},
            {"szTaOperationType": data.get("Header", {}).get("szTaOperationType")},
            {"szTaPosVersion": data.get("Header", {}).get("szTaPosVersion")},
            {"szTaType": data.get("Header", {}).get("szTaType")},
            {"szWorkstationID": data.get("Header", {}).get("szWorkstationID")},
            {"szRetailStoreName": data.get("Header", {}).get("szRetailStoreName")},
            {"szBarcodeComplete": data.get("Header", {}).get("footer", {}).get("szBarcodeComplete")},
            {"szUTCDate": data.get("Header", {}).get("footer", {}).get("szUTCDate")},
            {"szTransactionDutyType": data.get("Header", {}).get("transactionType", {}).get("szTransactionDutyType")},
            {"szTransactionTypeDesc": data.get("Header", {}).get("transactionType", {}).get("szTransactionTypeDesc")},
            {"szTransactionTypeID": data.get("Header", {}).get("transactionType", {}).get("szTransactionTypeID")},
            {"szDutyType": data.get("Header", {}).get("transactionType", {}).get("szDutyType")},
            {"lTransactionTypeID": data.get("Header", {}).get("transactionType", {}).get("lTransactionTypeID")},
            {"szFirstName": data.get("Header", {}).get("customer", {}).get("keyCustomer", {}).get("szFirstName")},
            {"szLastName": data.get("Header", {}).get("customer", {}).get("keyCustomer", {}).get("szLastName")},
            {"szPaxNmbrBarcode": data.get("Header", {}).get("customer", {}).get("szPaxNmbrBarcode")},
            {"lPaxType": data.get("Header", {}).get("customer", {}).get("lPaxType")},
            {"szCustomerGuaranteeCardToken": data.get("Header", {}).get("customer", {}).get(
                "szCustomerGuaranteeCardToken")},
            {"szGuaranteeCardTrxnNo": data.get("Header", {}).get("customer", {}).get("szGuaranteeCardTrxnNo")},
            {"szFlightVesselNo": data.get("Header", {}).get("customer", {}).get("szFlightVesselNo")},
            {"szDestination": data.get("Header", {}).get("customer", {}).get("szDestination")},
            {"szAddress": data.get("Header", {}).get("customer", {}).get("szAddress")},
            {"szBoardingGate": data.get("Header", {}).get("customer", {}).get("szBoardingGate")},
            {"szClass": data.get("Header", {}).get("customer", {}).get("szClass")},
            {"lCustomerTypeCode": data.get("Header", {}).get("customer", {}).get("lCustomerTypeCode")},
            {"szCustomerTypeDesc": data.get("Header", {}).get("customer", {}).get("szCustomerTypeDesc")},
            {"szCustomerTypeShopperID": data.get("Header", {}).get("customer", {}).get("szCustomerTypeShopperID")},
            {"szDepartureDate": data.get("Header", {}).get("customer", {}).get("szDepartureDate")},
            {"szDepartureTime": data.get("Header", {}).get("customer", {}).get("szDepartureTime")},
            {"szDFSNationalityCode": data.get("Header", {}).get("customer", {}).get("szDFSNationalityCode")},
            {"szDFSNationalityDesc": data.get("Header", {}).get("customer", {}).get("szDFSNationalityDesc")},
            {"szFlightCode": data.get("Header", {}).get("customer", {}).get("szFlightCode")},
            {"szGenderCode": data.get("Header", {}).get("customer", {}).get("szGenderCode")},
            {"szLoyaltyCardNo": data.get("Header", {}).get("customer", {}).get("szLoyaltyCardNo")},
            {"szLoyaltyEmailAddress": data.get("Header", {}).get("customer", {}).get("szLoyaltyEmailAddress")},
            {"szLoyaltyMemberID": data.get("Header", {}).get("customer", {}).get("szLoyaltyMemberID")},
            {"szLoyaltyMobileNo": data.get("Header", {}).get("customer", {}).get("szLoyaltyMobileNo")},
            {"szLoyaltyPointsBalance": data.get("Header", {}).get("customer", {}).get("szLoyaltyPointsBalance")},
            {"szLoyaltyPointsExpiryDate": data.get("Header", {}).get("customer", {}).get("szLoyaltyPointsExpiryDate")},
            {"szLoyaltyStatusDollar": data.get("Header", {}).get("customer", {}).get("szLoyaltyStatusDollar")},
            {"szAirlineCode": data.get("Header", {}).get("customer", {}).get("szAirlineCode")},
            {"szLoyaltyTier": data.get("Header", {}).get("customer", {}).get("szLoyaltyTier")},
            {"szMobileNo": data.get("Header", {}).get("customer", {}).get("szMobileNo")},
            {"szNationalityAlphaCode": data.get("Header", {}).get("customer", {}).get("szNationalityAlphaCode")},
            {"szPartnerProgramCode": data.get("Header", {}).get("customer", {}).get("szPartnerProgramCode")},
            {"szPartnerProgramName": data.get("Header", {}).get("customer", {}).get("szPartnerProgramName")},
            {"szPartnerProgramShopperID": data.get("Header", {}).get("customer", {}).get("szPartnerProgramShopperID")},
            {"szPassportNmbr": data.get("Header", {}).get("customer", {}).get("szPassportNmbr")},
            {"szPaxNmbr": data.get("Header", {}).get("customer", {}).get("szPaxNmbr")},
            {"szPortOfDeparture": data.get("Header", {}).get("customer", {}).get("szPortOfDeparture")},
            {"szSeatNmbr": data.get("Header", {}).get("customer", {}).get("szSeatNmbr")},
            {"szTourGroupLeaderNo": data.get("Header", {}).get("customer", {}).get("szTourGroupLeaderNo")},
            {"szTourGroupName": data.get("Header", {}).get("customer", {}).get("szTourGroupName")},
            {"szTourGroupNmbr": data.get("Header", {}).get("customer", {}).get("szTourGroupNmbr")},
            {"szTourVisitDate": data.get("Header", {}).get("customer", {}).get("szTourVisitDate")},
            {"szTravelAgentNmbr": data.get("Header", {}).get("customer", {}).get("szTravelAgentNmbr")},
            {"szTravelBy": data.get("Header", {}).get("customer", {}).get("szTravelBy")},
            {"lOriginTaNmbr": data.get("Header", {}).get("reprintTa", {}).get("lOriginTaNmbr")},
            {"lOriginWorkstationNmbr": data.get("Header", {}).get("reprintTa", {}).get("lOriginWorkstationNmbr")},
            {"szOriginDate": data.get("Header", {}).get("reprintTa", {}).get("szOriginDate")},
            {"szOriginTime": data.get("Header", {}).get("reprintTa", {}).get("szOriginTime")},
            {"DFS_szTransactionNumber": data.get("Header", {}).get("reprintTa", {}).get("DFS_szTransactionNumber")},
            {"lOrgOperatorID": data.get("Header", {}).get("reprintTa", {}).get("lOrgOperatorID")},
            {"DFS_lOrgTransactionType": data.get("Header", {}).get("reprintTa", {}).get("DFS_lOrgTransactionType")},
            {"szReprintType": data.get("Header", {}).get("reprintTa", {}).get("szReprintType")},
            {"storeReceipt_szBarcode": data.get("Header", {}).get("storeReceipt", [{}])[sreceipt].get("szBarcode") for
            sreceipt in range(len(data.get("Header", {}).get("storeReceipt", [{}])))},
            {"szGuaranteeCollectionDate": data.get("Header", {}).get("guaranteeCard", {}).get("szGuaranteeCollectionDate")},
            {"lGuaranteeEventID": data.get("Header", {}).get("guaranteeCard", {}).get("lGuaranteeEventID")},
            {"lGuaranteePOSNo": data.get("Header", {}).get("guaranteeCard", {}).get("lGuaranteePOSNo")},
            {"szGuaranteeDFSTransNo": data.get("Header", {}).get("guaranteeCard", {}).get("szGuaranteeDFSTransNo")},
            {"szGuaranteeCardToken": data.get("Header", {}).get("customer", {}).get("szGuaranteeCardToken")},
            {"szRefTransactionTypeID": data.get("Header", {}).get("subtotal", {}).get("szRefTransactionTypeID")},
            {"dTotalNetSale": data.get("Header", {}).get("subtotal", {}).get("dTotalNetSale")},
            {"dTaDiscount": data.get("Header", {}).get("subtotal", {}).get("dTaDiscount")},
            {"voidFlag": data.get("Header", {}).get("subtotal", {}).get("voidFlag")},
            {"subTotal_dTotalSale": data.get("Header", {}).get("subtotal", {}).get("dTotalSale")},
            {"dExcludedTax": data.get("Header", {}).get("subtotal", {}).get("dExcludedTax")},
            {"dIncludedTax": data.get("Header", {}).get("subtotal", {}).get("dIncludedTax")},
            {"dTotalSale": data.get("Header", {}).get("total", {}).get("dTotalSale")},
            {"dTotalNet": data.get("Header", {}).get("total", {}).get("dTotalNet")},
            {"lNmbrOfItems": data.get("Header", {}).get("total", {}).get("lNmbrOfItems")},
            {"dTotalInEuro": data.get("Header", {}).get("total", {}).get("dTotalInEuro")},
            {"DFS_dNmbrOfItems": data.get("Header", {}).get("total", {}).get("DFS_dNmbrOfItems")},
            {"DFS_szPayCurrSym": data.get("Header", {}).get("total", {}).get("DFS_szPayCurrSym")},
            {"DFS_dTotalSaleRounded": data.get("Header", {}).get("total", {}).get("DFS_dTotalSaleRounded")},
            {"dRoundingError": data.get("Header", {}).get("total", {}).get("dRoundingError")},
            {"DFS_dRoundingAdjustment": data.get("Header", {}).get("total", {}).get("DFS_dRoundingAdjustment")},
            {"dRemainder": data.get("Header", {}).get("total", {}).get("dRemainder")},
            {"PAX_PREMIER_PASS_ID": None},
            {"PAX_POSTAL_CODE": None},
            {"PAX_AutoConsumo": None},
            {"PAX_VATCODE": None},
            {"Date_Of_Enrollment": None},
            {"PAX_ADDRESS_2": None},
            {"PAX_CITY": None},
            {"CashDrawerID": None},
            {"OrigTrxTillSupervisor": None},
            {"DFS_lOrgOperatorID": None},
            {"lOrgRetailStoreID": None},
            {"DFS_lOrgTaTypeNmbr": None},
            {"lOrgWorkstationNmbr": None},
            {"lOrgTaNmbr": None},
            {"szOrgDate": None},
            {"lVoidedTaNmbr": None},
            {"lVoidedWorkstationNmbr": None},
            {"lVoidedRetailStoreID": None},
            {"szVoidedDate": None},
            {"szVoidedTime": None},
            {"szDocumentNumber": data.get("customerOrder", {}).get("szDocumentNumber")},
            OrderedDict({"discount_summary": data.get("Header", {}).get("discount_summary")}),
            {"taxes": [{"lTaCreateNmbr": data.get("Header", {}).get("TAX", [{}])[h].get("hdr", {}).get("lTaCreateNmbr"),
                        "dTotalSale": data.get("Header", {}).get("TAX", [{}])[h].get("dTotalSale"),
                        "type": data.get("Header", {}).get("TAX", [{}])[h].get("type"),
                        "dUsedTotalSale": data.get("Header", {}).get("TAX", [{}])[h].get("dUsedTotalSale"),
                        "dIncludedTaxValue": data.get("Header", {}).get("TAX", [{}])[h].get("dIncludedTaxValue"),
                        "szTaxGroupID": data.get("Header", {}).get("TAX", [{}])[h].get("tax", {}).get("szTaxGroupID"),
                        "bTaxRefundable": data.get("Header", {}).get("TAX", [{}])[h].get("tax", {}).get("bTaxRefundable"),
                        "szRateRuleExpirationTime": data.get("Header", {}).get("TAX", [{}])[h].get("tax", {}).get(
                            "szRateRuleExpirationTime"),
                        "dPercent": data.get("Header", {}).get("TAX", [{}])[h].get("tax", {}).get("dPercent"),
                        "bTaxIncluded": data.get("Header", {}).get("TAX", [{}])[h].get("tax", {}).get("bTaxIncluded"),
                        "szRateRuleExpirationDate": data.get("Header", {}).get("TAX", [{}])[h].get("tax", {}).get(
                            "szRateRuleExpirationDate"),
                        "dTaxablePercent": data.get("Header", {}).get("TAX", [{}])[h].get("tax", {}).get("dTaxablePercent"),
                        "szTaxAuthorityName": data.get("Header", {}).get("TAX", [{}])[h].get("tax", {}).get(
                            "szTaxAuthorityName"),
                        "szTaxGroupRuleName": data.get("Header", {}).get("TAX", [{}])[h].get("tax", {}).get(
                            "szTaxGroupRuleName"),
                        "szTaxAuthorityID": data.get("Header", {}).get("TAX", [{}])[h].get("tax", {}).get(
                            "szTaxAuthorityID"),
                        "dIncludedTaxValue": data.get("Header", {}).get("TAX", [{}])[h].get("dIncludedTaxValue")} for h in
                    range(len(data.get("Header", {}).get("TAX", [])))]},
            {"comments": [
                {"type": data.get("Header", {}).get("comment", [{}])[c].get("hdr", {}).get("szTaRefTaObject", "HEADER"),
                "lDomainID": data.get("Header", {}).get("comment", [{}])[c].get("lDomainID"),
                "lTaCreateNmbr": data.get("Header", {}).get("comment", [{}])[c].get("hdr", {}).get("lTaCreateNmbr"),
                "szCommentDate": data.get("Header", {}).get("comment", [{}])[c].get("szCommentDate",
                                                                                    data.get("Header", {}).get("comment",
                                                                                                                [{}])[
                                                                                        c].get("szDate")),
                "szCommentDescription": data.get("Header", {}).get("comment", [{}])[c].get("szCommentDescription",
                                                                                            data.get("Header", {}).get(
                                                                                                "comment", [{}])[c].get(
                                                                                                "szDescription")),
                "szCommentDomainAreaTypeCode": data.get("Header", {}).get("comment", [{}])[c].get(
                    "szCommentDomainAreaTypeCode",
                    data.get("Header", {}).get("comment", [{}])[c].get("szDomainAreaTypeCode")),
                "szCommentFunctionName": data.get("Header", {}).get("comment", [{}])[c].get("szCommentFunctionName",
                                                                                            data.get("Header", {}).get(
                                                                                                "comment", [{}])[c].get(
                                                                                                "szFunctionName")),
                "szCommentModulName": data.get("Header", {}).get("comment", [{}])[c].get("szModulName",
                                                                                        data.get("Header", {}).get(
                                                                                            "comment", [{}])[c].get(
                                                                                            "szCommentModulName")),
                "szCommentReasonCode": data.get("Header", {}).get("comment", [{}])[c].get("szReasonCode",
                                                                                        data.get("Header", {}).get(
                                                                                            "comment", [{}])[c].get(
                                                                                            "szCommentReasonCode"))} for
                c in range(len(data.get("Header", {}).get("comment", []))) if
                'szDescription' in data.get("Header", {}).get("comment", [{}])[c].keys()]},
            {"overrides": [
                {"lTaCreateNmbr": data.get('Header', {}).get('override', [{}])[overr].get('hdr', {}).get('lTaCreateNmbr'),
                "lTaRefToCreateNmbr": data.get('Header', {}).get('override', [{}])[overr].get('hdr', {}).get(
                    'lTaRefToCreateNmbr'),
                "szTaRefTaObject": data.get('Header', {}).get('override', [{}])[overr].get('hdr', {}).get(
                    'szTaRefTaObject'),
                "supervisorName": data.get('Header', {}).get('override', [{}])[overr].get('szEmplName'),
                "supervisorID": data.get('Header', {}).get('override', [{}])[overr].get('lEmployeeID'),
                "szModulName": data.get('Header', {}).get('override', [{}])[overr].get('szModulName'),
                "szParameterName": data.get('Header', {}).get('override', [{}])[overr].get('szParameterName')}
                for overr in range(len(data.get('Header', {}).get('override', []))) if
                'override' in data.get('Header', {}).keys()]},
            {"recallReceipts": [{"szDateRef": data.get("Header", {}).get("recallReceipt", [{}])[recall].get("szDateRef"),
                                "szType": data.get("Header", {}).get("recallReceipt", [{}])[recall].get("szType"),
                                "lTaNmbrStored": data.get("Header", {}).get("recallReceipt", [{}])[recall].get(
                                    "lTaNmbrStored"),
                                "lWorkstationNmbrStored": data.get("Header", {}).get("recallReceipt", [{}])[recall].get(
                                    "lWorkstationNmbrStored"),
                                "lRetailStoreIDStored": data.get("Header", {}).get("recallReceipt", [{}])[recall].get(
                                    "lRetailStoreIDStored"),
                                "szDateStored": data.get("Header", {}).get("recallReceipt", [{}])[recall].get(
                                    "szDateStored"),
                                "szTimeStored": data.get("Header", {}).get("recallReceipt", [{}])[recall].get(
                                    "szTimeStored"),
                                "dTotalAmount": data.get("Header", {}).get("recallReceipt", [{}])[recall].get(
                                    "dTotalAmount")} for recall in
                                range(len(data.get('Header', {}).get('recallReceipt', []))) if
                                'recallReceipt' in data.get('Header', {}).keys()]},
            {"serialized": [
                {"lTaCreateNmbr": data.get("Header", {}).get("serialized", [{}])[h].get("hdr", {}).get("lTaCreateNmbr"),
                "lTaRefToCreateNmbr": data.get("Header", {}).get("serialized", [{}])[h].get("hdr", {}).get(
                    "lTaRefToCreateNmbr", {}),
                "lPresentation": data.get("Header", {}).get("serialized", [{}])[h].get("lPresentation"),
                "szStatus": data.get("Header", {}).get("serialized", [{}])[h].get("szStatus"),
                "szTypeCode": data.get("Header", {}).get("serialized", [{}])[h].get("szTypeCode"),
                "szDateValidFrom": data.get("Header", {}).get("serialized", [{}])[h].get("szDateValidFrom"),
                "szDateValidTo": data.get("Header", {}).get("serialized", [{}])[h].get("szDateValidTo"),
                "dAmount": data.get("Header", {}).get("serialized", [{}])[h].get("dAmount"),
                "dAmountForeign": data.get("Header", {}).get("serialized", [{}])[h].get("dAmountForeign"),
                "szCurrency": data.get("Header", {}).get("serialized", [{}])[h].get("szCurrency"),
                "szDiscountID": data.get("Header", {}).get("serialized", [{}])[h].get("szDiscountID"),
                "szLoyaltyCardNo": data.get("Header", {}).get("serialized", [{}])[h].get("szLoyaltyCardNo"),
                "szCommand": data.get("Header", {}).get("serialized", [{}])[h].get("szCommand"),
                "bIsOffline": data.get("Header", {}).get("serialized", [{}])[h].get("bIsOffline"),
                "szSerialNmbr": data.get("Header", {}).get("serialized", [{}])[h].get("szSerialNmbr"),
                "szSerializeTypeCode": data.get("Header", {}).get("serialized", [{}])[h].get("szSerializeTypeCode"),
                "szBarCode": data.get("Header", {}).get("serialized", [{}])[h].get("szBarCode")} for h in
                range(len(data.get("Header", {}).get("serialized", [])))]},
            {"Discounts": [{"discountLevel": "Header",
                            "lDiscListType": data.get("Header").get("Discount", [{}])[d].get("lDiscListType"),
                            "dDiscQty": data.get("Header").get("Discount", [{}])[d].get("dDiscQty"),
                            "lTaCreateNmbr": data.get("Header").get("Discount", [{}])[d].get("hdr", {}).get(
                                "lTaCreateNmbr"),
                            "dDiscValue": data.get("Header").get("Discount", [{}])[d].get("dDiscValue"),
                            "dTotalAmount": data.get("Header").get("Discount", [{}])[d].get("dTotalAmount"),
                            "dTotalDiscount": data.get("Header").get("Discount", [{}])[d].get("dTotalDiscount"),
                            "szDiscDesc": data.get("Header").get("Discount", [{}])[d].get("szDiscDesc"),
                            "szDiscountID": data.get("Header").get("Discount", [{}])[d].get("szDiscountID"),
                            "szDiscountType": data.get("Header").get("Discount", [{}])[d].get("szDiscountType"),
                            "szDiscValueType": data.get("Header").get("Discount", [{}])[d].get("szDiscValueType"),
                            "linedsc_reason_cd": None, "comments": [{"type":
                                                                        data.get("Header").get("Discount", [{}])[d].get(
                                                                            "comment", [{}])[c].get("hdr", {}).get(
                                                                            "szTaRefTaObject"), "lTaCreateNmbr":
                                                                        data.get("Header").get("Discount", [{}])[d].get(
                                                                            "comment", [{}])[c].get("hdr", {}).get(
                                                                            "lTaCreateNmbr"), "szCommentDate":
                                                                        data.get("Header").get("Discount", [{}])[d].get(
                                                                            "comment", [{}])[c].get("szCommentDate",
                                                                                                    data.get("Header").get(
                                                                                                        "Discount", [{}])[
                                                                                                        d].get("comment",
                                                                                                                [{}])[
                                                                                                        c].get("szDate")),
                                                                    "szCommentDomainAreaTypeCode":
                                                                        data.get("Header").get("Discount", [{}])[d].get(
                                                                            "comment", [{}])[c].get(
                                                                            "szCommentDomainAreaTypeCode",
                                                                            data.get("Header").get("Discount", [{}])[
                                                                                d].get("comment", [{}])[c].get(
                                                                                "szDomainAreaTypeCode")),
                                                                    "szCommentModulName":
                                                                        data.get("Header").get("Discount", [{}])[d].get(
                                                                            "comment", [{}])[c].get("szModulName",
                                                                                                    data.get("Header").get(
                                                                                                        "Discount", [{}])[
                                                                                                        d].get("comment",
                                                                                                                [{}])[
                                                                                                        c].get(
                                                                                                        "szCommentModulName")),
                                                                    "szCommentReasonCode":
                                                                        data.get("Header").get("Discount", [{}])[d].get(
                                                                            "comment", [{}])[c].get("szReasonCode",
                                                                                                    data.get("Header").get(
                                                                                                        "Discount", [{}])[
                                                                                                        d].get("comment",
                                                                                                                [{}])[
                                                                                                        c].get(
                                                                                                        "szCommentReasonCode")),
                                                                    "szCommentDescription":
                                                                        data.get("Header").get("Discount", [{}])[d].get(
                                                                            "comment", [{}])[c].get("szCommentDescription",
                                                                                                    data.get("Header").get(
                                                                                                        "Discount", [{}])[
                                                                                                        d].get("comment",
                                                                                                                [{}])[
                                                                                                        c].get(
                                                                                                        "szDescription")),
                                                                    "szCommentFunctionName":
                                                                        data.get("Header").get("Discount", [{}])[d].get(
                                                                            "comment", [{}])[c].get(
                                                                            "szCommentFunctionName",
                                                                            data.get("Header").get("Discount", [{}])[
                                                                                d].get("comment", [{}])[c].get(
                                                                                "szFunctionName"))} for c in range(
                    len(data.get("Header").get("Discount", [{}])[d].get('comment', [])))]} for d in
                        range(len(data.get("Header").get("Discount", [])))]},
            {"Coupons": [{"szDescription": data.get("Header").get("coupon", [{}])[d].get("szDescription"),
                        "szScanCode": data.get("Header").get("coupon", [{}])[d].get("szScanCode"),
                        "lTaCreateNmbr": data.get("Header").get("coupon", [{}])[d].get("hdr", {}).get("lTaCreateNmbr"),
                        "szCouponID": data.get("Header").get("coupon", [{}])[d].get("szCouponID"),
                        "szDiscountMediaID": data.get("Header").get("coupon", [{}])[d].get("szDiscountMediaID"),
                        "dAmount": data.get("Header").get("coupon", [{}])[d].get("dAmount")} for d in
                        range(len(data.get("Header").get("coupon", [])))]},
            {"deposit": data.get("Header", {}).get("deposit", [])}
        ]
    except KeyError:
        raise

    for head in header_list:
        flat_header['Header'].update(head)

    # --------------CONDITIONAL---TAGS---FROM----HEADER-----#

    # ---This is to check guaranteeCardToken and szGuaranteeCardTrxnNo from customer and guaranteeCard---#
    conditional_tags = ['szGuaranteeCardToken', 'szGuaranteeCardTrxnNo']
    for conditionals in conditional_tags:
        if (data.get("Header", {}).get("customer", {}).get(conditionals) == None):
            if (data.get("Header", {}).get("guaranteeCard", {}).get(conditionals) != None):
                flat_header['Header'].update(
                    {conditionals: data.get("Header", {}).get("guaranteeCard", {}).get(conditionals)})

    # ---This is to check ---lTaTypeNmbr from header-and reprintTa---#
    conditional_reprints = ['lTaTypeNmbr']
    for con_reprints in conditional_reprints:
        if (data.get("Header", {}).get(con_reprints) == None):
            if (data.get("Header", {}).get("reprintTa", {}).get(con_reprints) != None):
                flat_header['Header'].update({con_reprints: data.get("Header", {}).get("reprintTa", {}).get(con_reprints)})

    # ------------CONDITIONAL---TAGS---FROM----HEADER--ENDS---HERE------#


    # adding-prior-txn-tags-from-artreturn-into-header-level--#
    for art in range(len(data.get('LineItem', []))):
        if (data.get('LineItem', [{}])[art].get('type') == 'artReturn'):
            prior_txn = [{"DFS_lOrgOperatorID": data.get("LineItem", [{}])[art].get("DFS_lOrgOperatorID")},
                        {"lOrgRetailStoreID": data.get("LineItem", [{}])[art].get("lOrgRetailStoreID")},
                        {"DFS_lOrgTaTypeNmbr": data.get("LineItem", [{}])[art].get("DFS_lOrgTaTypeNmbr")},
                        {"lOrgWorkstationNmbr": data.get("LineItem", [{}])[art].get("lOrgWorkstationNmbr")},
                        {"lOrgTaNmbr": data.get("LineItem", [{}])[art].get("lOrgTaNmbr")},
                        {"szOrgDate": data.get("LineItem", [{}])[art].get("szOrgDate")}
                        ]
            for prior in prior_txn:
                flat_header['Header'].update(prior)

    # --adding-void-receipt-tags-at-header-level--#
    if 'voidReceipt' in data.get('Header', {}).keys():
        void_list = [{"lVoidedTaNmbr": data.get("Header", {}).get("voidReceipt", {}).get("lVoidedTaNmbr")},
                    {"lVoidedWorkstationNmbr": data.get("Header", {}).get("voidReceipt", {}).get(
                        "lVoidedWorkstationNmbr")},
                    {"DFS_lOrgOperatorID": data.get("Header", {}).get("voidReceipt", {}).get("DFS_lOrgOperatorID")},
                    {"DFS_lOrgTaTypeNmbr": data.get("Header", {}).get("voidReceipt", {}).get("DFS_lOrgTaTypeNmbr")},
                    {"lVoidedRetailStoreID": data.get("Header", {}).get("voidReceipt", {}).get("lVoidedRetailStoreID")},
                    {"szVoidedDate": data.get("Header", {}).get("voidReceipt", {}).get("szVoidedDate")},
                    {"szVoidedTime": data.get("Header", {}).get("voidReceipt", {}).get("szVoidedTime")}
                    ]
        for voidr in void_list:
            flat_header['Header'].update(voidr)

    final_flatten.update(flat_header)

    # ----FLATTEN---HEADER---ENDS--HERE----#

    # ---ALPHABETICALLY SORTING--HEADER-----#

    alpha_header = OrderedDict()
    list_header = OrderedDict()
    nested_header = OrderedDict()
    for k, v in sorted(final_flatten.get('Header', {}).items()):
        if (type(v) == list):
            list_header.update({k: v})
        elif (type(v) == dict or type(v) == OrderedDict):
            nested_header.update({k: v})
        else:
            alpha_header.update({k: v})

    alpha_header.update(nested_header)
    alpha_header.update(list_header)

    final_flatten['Header'] = alpha_header

    # --------HEADER---SORTING--ENDS---HERE------#

    # ---------------FLATTEN-LINE-------------#
    flat_item = OrderedDict()
    flat_item.update({'LineItems': []})
    temp_dict = OrderedDict()
    temp_dict.update({})

    if 'LineItem' in data.keys():
        for i in range(len(data.get('LineItem', []))):
            try:
                line_list = [{'type': data.get("LineItem", [{}])[i].get("type")},
                            {'dh_txn_nmbr': data.get("LineItem", [{}])[i].get("dh_txn_nmbr")},
                            {'dTaQty': data.get("LineItem", [{}])[i].get("dTaQty")},
                            {'dTaPrice': data.get("LineItem", [{}])[i].get("dTaPrice")},
                            {'dOrgPrice': data.get("LineItem", [{}])[i].get("dOrgPrice")},
                            {'szStyleID': data.get("LineItem", [{}])[i].get("szStyleID")},
                            {'lEmployeeID': data.get("LineItem", [{}])[i].get("lEmployeeID")},
                            {'szEmplName': data.get("LineItem", [{}])[i].get("szEmplName")},
                            {'lTadiscountflag': data.get("LineItem", [{}])[i].get("lTadiscountflag")},
                            {'lTaPriceInfo': data.get("LineItem", [{}])[i].get("lTaPriceInfo")},
                            {'dTaTotal': data.get("LineItem", [{}])[i].get("dTaTotal")},
                            {'szItemLookupCode': data.get("LineItem", [{}])[i].get("szItemLookupCode")},
                            {'dTaDiscount': data.get("LineItem", [{}])[i].get("dTaDiscount")},
                            {'dTaTotalDiscounted': data.get("LineItem", [{}])[i].get("dTaTotalDiscounted")},
                            {'ObjectIdentification': data.get("LineItem", [{}])[i].get("ObjectIdentification")},
                            {'dQuantityEntry': data.get("LineItem", [{}])[i].get("dQuantityEntry")},
                            {'szPrintCodes': data.get("LineItem", [{}])[i].get("szPrintCodes")},
                            {'dOrgQty': data.get("LineItem", [{}])[i].get("dOrgQty")},
                            {'szRefTransactionTypeID': data.get("LineItem", [{}])[i].get("szRefTransactionTypeID")},
                            {'szRefTransactionDutyType': data.get("LineItem", [{}])[i].get("szRefTransactionDutyType")},
                            {'szRefTransactionTypeDesc': data.get("LineItem", [{}])[i].get("szRefTransactionTypeDesc")},
                            {'lTaCreateNmbr': data.get("LineItem", [{}])[i].get("hdr", {}).get("lTaCreateNmbr")},
                            {'lOriginalTADevice': data.get("LineItem", [{}])[i].get("lOriginalTADevice")},
                            {'szInputString': data.get("LineItem", [{}])[i].get("szInputString")},
                            {'szItemID': data.get("LineItem", [{}])[i].get("article", {}).get("szItemID")},
                            {'lMerchandiseStructureID': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "lMerchandiseStructureID")},
                            {'szMerchHierarchyLevelCode': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "szMerchHierarchyLevelCode")},
                            {'szPOSDepartmentID': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "szPOSDepartmentID")},
                            {'szPOSItemID': data.get("LineItem", [{}])[i].get("article", {}).get("szPOSItemID")},
                            {'szItemTaxGroupID': data.get("LineItem", [{}])[i].get("article", {}).get("szItemTaxGroupID")},
                            {'szClass': data.get("LineItem", [{}])[i].get("article", {}).get("szClass")},
                            {'dPRLocalPiecePriceAmount': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "dPRLocalPiecePriceAmount")},
                            {'dPackingUnitPriceAmount': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "dPackingUnitPriceAmount")},
                            {'szPieceUnitOfMeasureCode': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "szPieceUnitOfMeasureCode")},
                            {'szItemCategoryTypeCode': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "szItemCategoryTypeCode")},
                            {'szDescription': data.get("LineItem", [{}])[i].get("article", {}).get("szDescription")},
                            {'szTypeCode': data.get("LineItem", [{}])[i].get("article", {}).get("szTypeCode")},
                            {'szDutyType': data.get("LineItem", [{}])[i].get("article", {}).get("szDutyType")},
                            {'szPickedLocation': data.get("LineItem", [{}])[i].get("article", {}).get("szPickedLocation")},
                            {'szCommonItemID': data.get("LineItem", [{}])[i].get("article", {}).get("szCommonItemID")},
                            {'dPRPackingUnitPriceAmount': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "dPRPackingUnitPriceAmount")},
                            {'szPRCampaignID': data.get("LineItem", [{}])[i].get("article", {}).get("szPRCampaignID")},
                            {'szPRDateValidFrom': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "szPRDateValidFrom")},
                            {'szPRDateValidTo': data.get("LineItem", [{}])[i].get("article", {}).get("szPRDateValidTo")},
                            {'DFS_dPROrgLocalPiecePriceAmount': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "DFS_dPROrgLocalPiecePriceAmount")},
                            {'DFS_dPROriginalPriceAmount': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "DFS_dPROriginalPriceAmount")},
                            {'bApprovalCategoryOverride': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "bApprovalCategoryOverride")},
                            {'bApprovalSKUOverride': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "bApprovalSKUOverride")},
                            {'bPriceEntryRequiredFlag': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "bPriceEntryRequiredFlag")},
                            {'szSizeTypeDescription': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "szSizeTypeDescription")},
                            {'bAgeControlFlag': data.get("LineItem", [{}])[i].get("article", {}).get("bAgeControlFlag")},
                            {'lAgeControl': data.get("LineItem", [{}])[i].get("article", {}).get("lAgeControl")},
                            {'szCategory': data.get("LineItem", [{}])[i].get("article", {}).get("szCategory")},
                            {'DFS_dPROrgPackingUnitPriceAmount': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "DFS_dPROrgPackingUnitPriceAmount")},
                            {'lCategoryThreshold': data.get("LineItem", [{}])[i].get("article", {}).get(
                                "lCategoryThreshold")},
                            {'lSKUThreshold': data.get("LineItem", [{}])[i].get("article", {}).get("lSKUThreshold")},
                            {'szAddOnDialog': data.get("LineItem", [{}])[i].get("article", {}).get("szAddOnDialog")},
                            {'szColorCode': data.get("LineItem", [{}])[i].get("article", {}).get("szColorCode")},
                            {'szSizeCode': data.get("LineItem", [{}])[i].get("article", {}).get("szSizeCode")},
                            {'dUsedTotalSale': data.get("LineItem", [{}])[i].get("taxArt", {}).get("dUsedTotalSale")},
                            {'dTotalSale': data.get("LineItem", [{}])[i].get("taxArt", {}).get("dTotalSale")},
                            {'dIncludedTaxValue': data.get("LineItem", [{}])[i].get("taxArt", {}).get(
                                "dIncludedTaxValue")},
                            {'dIncludedExactTaxValue': data.get("LineItem", [{}])[i].get("taxArt", {}).get(
                                "dIncludedExactTaxValue")},
                            {'dExcludedTaxValue': data.get("LineItem", [{}])[i].get("taxArt", {}).get(
                                "dExcludedTaxValue")},
                            {'dExcludedExactTaxValue': data.get("LineItem", [{}])[i].get("taxArt", {}).get(
                                "dExcludedExactTaxValue")},
                            {'szTaxGroupID': data.get("LineItem", [{}])[i].get("taxArt", {}).get("tax", {}).get(
                                "szTaxGroupID")},
                            {'lManualSequenceNumber': data.get("LineItem", [{}])[i].get("taxArt", {}).get("tax", {}).get(
                                "lManualSequenceNumber")},
                            {'szTaxAuthorityID': data.get("LineItem", [{}])[i].get("taxArt", {}).get("tax", {}).get(
                                "szTaxAuthorityID")},
                            {'szTaxAuthorityName': data.get("LineItem", [{}])[i].get("taxArt", {}).get("tax", {}).get(
                                "szTaxAuthorityName")},
                            {'szTaxGroupRuleName': data.get("LineItem", [{}])[i].get("taxArt", {}).get("tax", {}).get(
                                "szTaxGroupRuleName")},
                            {'dPercent': data.get("LineItem", [{}])[i].get("taxArt", {}).get("tax", {}).get("dPercent")},
                            {'bTaxIncluded': data.get("LineItem", [{}])[i].get("taxArt", {}).get("tax", {}).get(
                                "bTaxIncluded")},
                            {'szRateRuleEffectiveDate': data.get("LineItem", [{}])[i].get("taxArt", {}).get("tax", {}).get(
                                "szRateRuleEffectiveDate")},
                            {'addonDialog': data.get("LineItem", [{}])[i].get("addonDialog")},
                            {"PromoterID": None},
                            {"DutyAmount": None},
                            {"InventoryLocation": None},
                            {"iGateReference": None},
                            {"TaxFlag": None},
                            {"szQRCodeRef": data.get("LineItem", [{}])[i].get("szQRCodeRef")},
                            {"comments": [{"type": data.get("LineItem", [{}])[i].get("comment", [{}])[c].get("hdr",
                                                                                                            {}).get(
                                "szTaRefTaObject"),
                                            "lTaCreateNmbr": data.get("LineItem", [{}])[i].get("comment", [{}])[c].get(
                                                "hdr", {}).get("lTaCreateNmbr"),
                                            "szCommentDate": data.get("LineItem", [{}])[i].get("comment", [{}])[c].get(
                                                "szCommentDate",
                                                data.get("LineItem", [{}])[i].get("comment", [{}])[c].get("szDate")),
                                            "szCommentDescription": data.get("LineItem", [{}])[i].get("comment", [{}])[
                                                c].get("szCommentDescription",
                                                    data.get("LineItem", [{}])[i].get("comment", [{}])[c].get(
                                                        "szDescription")), "szCommentDomainAreaTypeCode":
                                                data.get("LineItem", [{}])[i].get("comment", [{}])[c].get(
                                                    "szCommentDomainAreaTypeCode",
                                                    data.get("LineItem", [{}])[i].get("comment", [{}])[c].get(
                                                        "szDomainAreaTypeCode")),
                                            "szCommentFunctionName": data.get("LineItem", [{}])[i].get("comment", [{}])[
                                                c].get("szCommentFunctionName",
                                                    data.get("LineItem", [{}])[i].get("comment", [{}])[c].get(
                                                        "szFunctionName")),
                                            "szCommentModulName": data.get("LineItem", [{}])[i].get("comment", [{}])[c].get(
                                                "szModulName", data.get("LineItem", [{}])[i].get("comment", [{}])[c].get(
                                                    "szCommentModulName")),
                                            "szCommentReasonCode": data.get("LineItem", [{}])[i].get("comment", [{}])[
                                                c].get("szReasonCode",
                                                    data.get("LineItem", [{}])[i].get("comment", [{}])[c].get(
                                                        "szCommentReasonCode"))} for c in
                                        range(len(data.get("LineItem", [{}])[i].get("comment", [])))]},
                            {"Discounts": [
                                {"dh_txn_nmbr": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get("dh_txn_nmbr"),
                                "discountLevel": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get(
                                    "discountLevel"),
                                "lDiscListType": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get(
                                    "lDiscListType"),
                                "dDiscQty": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get("dDiscQty"),
                                "lTaCreateNmbr": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get("hdr",
                                                                                                            {}).get(
                                    "lTaCreateNmbr"),
                                "dDiscValue": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get("dDiscValue"),
                                "dTotalAmount": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get(
                                    "dTotalAmount"),
                                "dTotalDiscount": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get(
                                    "dTotalDiscount"),
                                "lTaManDiscCreateNmbr": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get(
                                    "lTaManDiscCreateNmbr"),
                                "szDiscDesc": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get("szDiscDesc"),
                                "szDiscountID": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get(
                                    "szDiscountID"),
                                "szDiscountType": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get(
                                    "szDiscountType"),
                                "szDiscValueType": data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get(
                                    "szDiscValueType"), "linedsc_reason_cd": None, "comments": [{"type":
                                                                                                    data.get("LineItem",
                                                                                                                [{}])[
                                                                                                        i].get(
                                                                                                        "Discount",
                                                                                                        [{}])[d].get(
                                                                                                        "comment", [{}])[
                                                                                                        c].get("hdr",
                                                                                                                {}).get(
                                                                                                        "szTaRefTaObject"),
                                                                                                "lTaCreateNmbr":
                                                                                                    data.get("LineItem",
                                                                                                                [{}])[
                                                                                                        i].get(
                                                                                                        "Discount",
                                                                                                        [{}])[d].get(
                                                                                                        "comment", [{}])[
                                                                                                        c].get("hdr",
                                                                                                                {}).get(
                                                                                                        "lTaCreateNmbr"),
                                                                                                "szCommentDate":
                                                                                                    data.get("LineItem",
                                                                                                                [{}])[
                                                                                                        i].get(
                                                                                                        "Discount",
                                                                                                        [{}])[d].get(
                                                                                                        "comment", [{}])[
                                                                                                        c].get(
                                                                                                        "szCommentDate",
                                                                                                        data.get(
                                                                                                            "LineItem",
                                                                                                            [{}])[i].get(
                                                                                                            "Discount",
                                                                                                            [{}])[d].get(
                                                                                                            "comment",
                                                                                                            [{}])[c].get(
                                                                                                            "szDate")),
                                                                                                "szCommentDomainAreaTypeCode":
                                                                                                    data.get("LineItem",
                                                                                                                [{}])[
                                                                                                        i].get(
                                                                                                        "Discount",
                                                                                                        [{}])[d].get(
                                                                                                        "comment", [{}])[
                                                                                                        c].get(
                                                                                                        "szCommentDomainAreaTypeCode",
                                                                                                        data.get(
                                                                                                            "LineItem",
                                                                                                            [{}])[i].get(
                                                                                                            "Discount",
                                                                                                            [{}])[d].get(
                                                                                                            "comment",
                                                                                                            [{}])[c].get(
                                                                                                            "szDomainAreaTypeCode")),
                                                                                                "szCommentModulName":
                                                                                                    data.get("LineItem",
                                                                                                                [{}])[
                                                                                                        i].get(
                                                                                                        "Discount",
                                                                                                        [{}])[d].get(
                                                                                                        "comment", [{}])[
                                                                                                        c].get(
                                                                                                        "szModulName",
                                                                                                        data.get(
                                                                                                            "LineItem",
                                                                                                            [{}])[i].get(
                                                                                                            "Discount",
                                                                                                            [{}])[d].get(
                                                                                                            "comment",
                                                                                                            [{}])[c].get(
                                                                                                            "szCommentModulName")),
                                                                                                "szCommentReasonCode":
                                                                                                    data.get("LineItem",
                                                                                                                [{}])[
                                                                                                        i].get(
                                                                                                        "Discount",
                                                                                                        [{}])[d].get(
                                                                                                        "comment", [{}])[
                                                                                                        c].get(
                                                                                                        "szReasonCode",
                                                                                                        data.get(
                                                                                                            "LineItem",
                                                                                                            [{}])[i].get(
                                                                                                            "Discount",
                                                                                                            [{}])[d].get(
                                                                                                            "comment",
                                                                                                            [{}])[c].get(
                                                                                                            "szCommentReasonCode")),
                                                                                                "szCommentDescription":
                                                                                                    data.get("LineItem",
                                                                                                                [{}])[
                                                                                                        i].get(
                                                                                                        "Discount",
                                                                                                        [{}])[d].get(
                                                                                                        "comment", [{}])[
                                                                                                        c].get(
                                                                                                        "szCommentDescription",
                                                                                                        data.get(
                                                                                                            "LineItem",
                                                                                                            [{}])[i].get(
                                                                                                            "Discount",
                                                                                                            [{}])[d].get(
                                                                                                            "comment",
                                                                                                            [{}])[c].get(
                                                                                                            "szDescription")),
                                                                                                "szCommentFunctionName":
                                                                                                    data.get("LineItem",
                                                                                                                [{}])[
                                                                                                        i].get(
                                                                                                        "Discount",
                                                                                                        [{}])[d].get(
                                                                                                        "comment", [{}])[
                                                                                                        c].get(
                                                                                                        "szCommentFunctionName",
                                                                                                        data.get(
                                                                                                            "LineItem",
                                                                                                            [{}])[i].get(
                                                                                                            "Discount",
                                                                                                            [{}])[d].get(
                                                                                                            "comment",
                                                                                                            [{}])[c].get(
                                                                                                            "szFunctionName"))}
                                                                                                for c in range(
                                        len(data.get("LineItem", [{}])[i].get("Discount", [{}])[d].get('comment', [])))]}
                                for d in range(len(data.get("LineItem", [{}])[i].get("Discount", [])))]}]
                for val in line_list:
                    temp_dict.update(val)

                flat_item['LineItems'].insert(i, temp_dict)
                # --Truncate-dictionary-for-next-loop
                temp_dict = OrderedDict()
                temp_dict.update({})
            except KeyError:
                continue

    # -------PLU-NOT-FOUND-ADDITION INTO LINE ARRAY AS SEPERATE-ELEMENT--#
    if 'pluNotFound' in data.keys():
        for pnf in range(len(data.get('pluNotFound', []))):
            temp_plu_dict = OrderedDict()
            plu_not_list = [{'type': 'pluNotFound'},
                            {'dh_txn_nmbr': data.get("Header", {}).get("dh_txn_nmbr")},
                            {'lTaCreateNmbr': data.get('pluNotFound', [{}])[pnf].get('hdr', {}).get('lTaCreateNmbr')},
                            {'szItemLookupCode': data.get('pluNotFound', [{}])[pnf].get('szItemLookupCode')},
                            {'szDescription': data.get('pluNotFound', [{}])[pnf].get('article', {}).get('szDesc')},
                            {'dPackingUnitPriceAmount': data.get('pluNotFound', [{}])[pnf].get('article', {}).get(
                                'dPackingUnitPriceAmount')},
                            {'bVoidAllowed': data.get('pluNotFound', [{}])[pnf].get('article', {}).get('bVoidAllowed')},
                            {'szPOSItemID': data.get('pluNotFound', [{}])[pnf].get('article', {}).get('szPOSItemID')},
                            {'szItemCategoryTypeCode': data.get('pluNotFound', [{}])[pnf].get('article', {}).get(
                                'szItemCategoryTypeCode')},
                            {'szPOSDepartmentID': data.get('pluNotFound', [{}])[pnf].get('article', {}).get(
                                'szPOSDepartmentID')},
                            {'szItemTaxGroupID': data.get('pluNotFound', [{}])[pnf].get('article', {}).get(
                                'szItemTaxGroupID')}
                            ]
            for plu in plu_not_list:
                temp_plu_dict.update(plu)

            flat_item['LineItems'].append(temp_plu_dict)

    final_flatten.update(flat_item)

    # --------FLATTEN--LINE---ENDS---HERE------#

    # ---ALPHABETICALLY SORTING--LINE-----#

    temp_line_sort = []
    for litem in range(len(final_flatten.get('LineItems', []))):
        alpha_line = OrderedDict()
        list_line = OrderedDict()
        nested_line = OrderedDict()
        for k, v in sorted(final_flatten.get('LineItems', [{}])[litem].items()):
            if (type(v) == list):
                list_line.update({k: v})
            elif (type(v) == dict):
                nested_line.update({k: v})
            else:
                alpha_line.update({k: v})
        alpha_line.update(nested_line)
        alpha_line.update(list_line)
        temp_line_sort.insert(litem, alpha_line)

    final_flatten['LineItems'] = temp_line_sort

    # --------LINE---SORTING--ENDS---HERE------#


    # --------FLATTEN---PAYMENT----------------#

    flat_payment = OrderedDict()
    flat_payment.update({'Payments': []})
    temp_payment = OrderedDict()
    temp_payment.update({})
    if 'Payment' in data.keys():
        for i in range(len(data.get('Payment', []))):
            try:
                payment_list = [{'type': data.get("Payment", [{}])[i].get("type")},
                                {'dh_txn_nmbr': data.get("Payment", [{}])[i].get("dh_txn_nmbr")},
                                {'bIsDepositPayment': data.get("Payment", [{}])[i].get("bIsDepositPayment")},
                                {'dPaidForeignCurr': data.get("Payment", [{}])[i].get("dPaidForeignCurr")},
                                {'szCardNmbr': data.get("Payment", [{}])[i].get("szCardNmbr")},
                                {'dReturn': data.get("Payment", [{}])[i].get("dReturn")},
                                {'szCaptureReference': data.get("Payment", [{}])[i].get("szCaptureReference")},
                                {'szUPlanCardNo': data.get("Payment", [{}])[i].get("szUPlanCardNo")},
                                {'szUPlanCouponNo': data.get("Payment", [{}])[i].get("szUPlanCouponNo")},
                                {'szCurrency': data.get("Payment", [{}])[i].get("szCurrency")},
                                {'szTerminalBatch': data.get("Payment", [{}])[i].get("szTerminalBatch")},
                                {'szExpiryDate': data.get("Payment", [{}])[i].get("szExpiryDate")},
                                {'szCardCircuit': data.get("Payment", [{}])[i].get("szCardCircuit")},
                                {'szBankNmbr': data.get("Payment", [{}])[i].get("szBankNmbr")},
                                {'szAlipayBuyerLoginID': data.get("Payment", [{}])[i].get("szAlipayBuyerLoginID")},
                                {'szAlipayTransID': data.get("Payment", [{}])[i].get("szAlipayTransID")},
                                {'szAlipayFullTransID': data.get("Payment", [{}])[i].get("szAlipayFullTransID")},
                                {'szAlipayExchangeRate': data.get("Payment", [{}])[i].get("szAlipayExchangeRate")},
                                {'szAlipayTransAmountInCNY': data.get("Payment", [{}])[i].get("szAlipayTransAmountInCNY")},
                                {'szAlipayPartnerTransID': data.get("Payment", [{}])[i].get("szAlipayPartnerTransID")},
                                {'lSeqNmbr': data.get("Payment", [{}])[i].get("lSeqNmbr")},
                                {'szApprovalCode': data.get("Payment", [{}])[i].get("szApprovalCode")},
                                {'dTaPaid': data.get("Payment", [{}])[i].get("dTaPaid")},
                                {'lTaCreateNmbr': data.get("Payment", [{}])[i].get("hdr", {}).get("lTaCreateNmbr")},
                                {'dPaidForeignCurrTotal': data.get("Payment", [{}])[i].get("dPaidForeignCurrTotal")},
                                {'ObjectIdentification': data.get("Payment", [{}])[i].get("ObjectIdentification")},
                                {'szDFSEFTProvider': data.get("Payment", [{}])[i].get("payment", {}).get(
                                    "szDFSEFTProvider")},
                                {'szPOSCountingType': data.get("Payment", [{}])[i].get("payment", {}).get(
                                    "szPOSCountingType")},
                                {'szLoyaltyCardNmbr': data.get("Payment", [{}])[i].get("loyaltyPointsRedeem", {}).get(
                                    "szLoyaltyCardNmbr")},
                                {'szReceiptNumber': data.get("Payment", [{}])[i].get("loyaltyPointsRedeem", {}).get(
                                    "szReceiptNumber")},
                                {'dPointsRedeemed': data.get("Payment", [{}])[i].get("loyaltyPointsRedeem", {}).get(
                                    "dPointsRedeemed")},
                                {'dTenderedAmt': data.get("Payment", [{}])[i].get("loyaltyPointsRedeem", {}).get(
                                    "dTenderedAmt")},
                                {'lMediaMember': data.get("Payment", [{}])[i].get("payment", {}).get("lMediaMember")},
                                {'lMediaNmbr': data.get("Payment", [{}])[i].get("payment", {}).get("lMediaNmbr")},
                                {'szPosLogExternalID': data.get("Payment", [{}])[i].get("payment", {}).get(
                                    "szPosLogExternalID")},
                                {'szSpecialUsageVoidReceipt': data.get("Payment", [{}])[i].get("payment", {}).get(
                                    "szSpecialUsageVoidReceipt")},
                                {'lPayType': data.get("Payment", [{}])[i].get("payment", {}).get("lPayType")},
                                {'szDesc': data.get("Payment", [{}])[i].get("payment", {}).get("szDesc")},
                                {'szExternalID': data.get("Payment", [{}])[i].get("payment", {}).get("szExternalID")},
                                {'dPayLocal': data.get("Payment", [{}])[i].get("payment", {}).get("dPayLocal")},
                                {'dPayForeign': data.get("Payment", [{}])[i].get("payment", {}).get("dPayForeign")},
                                {'dPayExchRate': data.get("Payment", [{}])[i].get("payment", {}).get("dPayExchRate")},
                                {'szPayDesc': data.get("Payment", [{}])[i].get("payment", {}).get("szPayDesc")},
                                {'szPayCurrSym': data.get("Payment", [{}])[i].get("payment", {}).get("szPayCurrSym")},
                                {'szPayCurrSymLC': data.get("Payment", [{}])[i].get("payment", {}).get("szPayCurrSymLC")},
                                {'DFS_szExternalID': data.get("Payment", [{}])[i].get("eftInfo", {}).get(
                                    "DFS_szExternalID")},
                                {'dfs_szTransactionID': data.get("Payment", [{}])[i].get("dfs_szTransactionID")},
                                {'szMelcoMemberName': data.get("Payment", [{}])[i].get("szMelcoMemberName")},
                                {'dMelcoPointsDeducted': data.get("Payment", [{}])[i].get("dMelcoPointsDeducted")},
                                {'szPatronCardNumber': data.get("Payment", [{}])[i].get("szPatronCardNumber")},
                                {'szMCETransactionID': data.get("Payment", [{}])[i].get("szMCETransactionID")},
                                {'szPatronNumber': data.get("Payment", [{}])[i].get("szPatronNumber")},
                                {"DFS_Disc_code": None},
                                {"AuthorizationDateTime": None}
                                ]
                for pay in payment_list:
                    temp_payment.update(pay)

                flat_payment['Payments'].insert(i, temp_payment)
                temp_payment = OrderedDict()
                temp_payment.update({})
            except KeyError:
                continue

        final_flatten.update(flat_payment)

        # ---ALPHABETICALLY SORTING--PAYMENT-----#
        temp_pay_sort = []
        for pitem in range(len(final_flatten.get('Payments', []))):
            alpha_pay = OrderedDict()
            list_pay = OrderedDict()
            nested_pay = OrderedDict()
            for k, v in sorted(final_flatten.get('Payments', [{}])[pitem].items()):
                if (type(v) == list):
                    list_pay.update({k: v})
                elif (type(v) == dict):
                    nested_pay.update({k: v})
                else:
                    alpha_pay.update({k: v})
            alpha_pay.update(nested_pay)
            alpha_pay.update(list_pay)
            temp_pay_sort.insert(pitem, alpha_pay)

        final_flatten['Payments'] = temp_pay_sort

    # --------PAYMENT---SORTING--ENDS---HERE------#

    # -----FLATTEN---DEPOSIT-------#
    flat_deposit = OrderedDict()
    flat_deposit.update({'Deposit': {}})
    flat_deposit['Deposit'] = OrderedDict()
    # final_flatten.update(flat_deposit)
    try:
        deposit_list = [
            {'depositIn': {'szDate': data.get("Deposit", {}).get("depositIn", {}).get("szDate", ""),
                        'iPresentationKey': data.get("Deposit", {}).get("depositIn", {}).get("iPresentationKey", ""),
                        'szExpirationDate': data.get("Deposit", {}).get("depositIn", {}).get("szExpirationDate", ""),
                        'dAmount': data.get("Deposit", {}).get("depositIn", {}).get("dAmount", ""),
                        'szIdentNmbr': data.get("Deposit", {}).get("depositIn", {}).get("szIdentNmbr", ""),
                        'szLastName': data.get("Deposit", {}).get("depositIn", {}).get("szLastName", ""),
                        'szFirstName': data.get("Deposit", {}).get("depositIn", {}).get("szFirstName", ""),
                        'szEmailAddress': data.get("Deposit", {}).get("depositIn", {}).get("szEmailAddress", ""),
                        'szPhoneNmbr': data.get("Deposit", {}).get("depositIn", {}).get("szPhoneNmbr", ""),
                        'szFreeField1': data.get("Deposit", {}).get("depositIn", {}).get("szFreeField1", ""),
                        'lTaNmbr': data.get("Deposit", {}).get("depositIn", {}).get("lTaNmbr", ""),
                        'lWorkStationNmbr': data.get("Deposit", {}).get("depositIn", {}).get("lWorkStationNmbr", ""),
                        'lRetailStoreID': data.get("Deposit", {}).get("depositIn", {}).get("lRetailStoreID", ""),
                        'szSignOnName': data.get("Deposit", {}).get("depositIn", {}).get("szSignOnName", ""),
                        'dTurnover': data.get("Deposit", {}).get("depositIn", {}).get("dTurnover", ""),
                        'dTurnoverTax': data.get("Deposit", {}).get("depositIn", {}).get("dTurnoverTax", ""),
                        'lNmbrOfItems': data.get("Deposit", {}).get("depositIn", {}).get("lNmbrOfItems", ""),
                        'szBarcodeComplete': data.get("Deposit", {}).get("depositIn", {}).get("szBarcodeComplete", ""),
                        'szBarcode1': data.get("Deposit", {}).get("depositIn", {}).get("szBarcode1", ""),
                        'szBarcode2': data.get("Deposit", {}).get("depositIn", {}).get("szBarcode2", ""),
                        'sziGateReference': data.get("Deposit", {}).get("depositIn", {}).get("sziGateReference", ""),
                        'szDepositPercentage': data.get("Deposit", {}).get("depositIn", {}).get("szDepositPercentage",
                                                                                                ""),
                        'szEstimatedDeliveryDate': data.get("Deposit", {}).get("depositIn", {}).get(
                            "szEstimatedDeliveryDate", "")
                        }
            }, {'depositOut': {'szDate': data.get("Deposit", {}).get("depositOut", {}).get("szDate", ""),
                                'iPresentationKey': data.get("Deposit", {}).get("depositOut", {}).get("iPresentationKey",
                                                                                                    ""),
                                'szExpirationDate': data.get("Deposit", {}).get("depositOut", {}).get("szExpirationDate",
                                                                                                    ""),
                                'dAmount': data.get("Deposit", {}).get("depositOut", {}).get("dAmount", ""),
                                'szIdentNmbr': data.get("Deposit", {}).get("depositOut", {}).get("szIdentNmbr", ""),
                                'szLastName': data.get("Deposit", {}).get("depositOut", {}).get("szLastName", ""),
                                'szFirstName': data.get("Deposit", {}).get("depositOut", {}).get("szFirstName", ""),
                                'szEmailAddress': data.get("Deposit", {}).get("depositOut", {}).get("szEmailAddress", ""),
                                'szPhoneNmbr': data.get("Deposit", {}).get("depositOut", {}).get("szPhoneNmbr", ""),
                                'szFreeField1': data.get("Deposit", {}).get("depositOut", {}).get("szFreeField1", ""),
                                'lTaNmbr': data.get("Deposit", {}).get("depositOut", {}).get("lTaNmbr", ""),
                                'lWorkStationNmbr': data.get("Deposit", {}).get("depositOut", {}).get("lWorkStationNmbr",
                                                                                                    ""),
                                'lRetailStoreID': data.get("Deposit", {}).get("depositOut", {}).get("lRetailStoreID", ""),
                                'szSignOnName': data.get("Deposit", {}).get("depositOut", {}).get("szSignOnName", ""),
                                'dTurnover': data.get("Deposit", {}).get("depositOut", {}).get("dTurnover", ""),
                                'dTurnoverTax': data.get("Deposit", {}).get("depositOut", {}).get("dTurnoverTax", ""),
                                'lNmbrOfItems': data.get("Deposit", {}).get("depositOut", {}).get("lNmbrOfItems", ""),
                                'szBarcodeComplete': data.get("Deposit", {}).get("depositOut", {}).get("szBarcodeComplete",
                                                                                                    ""),
                                'szBarcode1': data.get("Deposit", {}).get("depositOut", {}).get("szBarcode1", ""),
                                'szBarcode2': data.get("Deposit", {}).get("depositOut", {}).get("szBarcode2", ""),
                                'sziGateReference': data.get("Deposit", {}).get("depositOut", {}).get("sziGateReference",
                                                                                                    ""),
                                'szDepositPercentage': data.get("Deposit", {}).get("depositOut", {}).get(
                                    "szDepositPercentage", ""),
                                'szEstimatedDeliveryDate': data.get("Deposit", {}).get("depositOut", {}).get(
                                    "szEstimatedDeliveryDate", "")
                                }
                }, {'depositReturn': {'szDate': data.get("Deposit", {}).get("depositReturn", {}).get("szDate", ""),
                                    'szDateRef': data.get("Deposit", {}).get("depositReturn", {}).get("szDateRef", ""),
                                    'lRetailStoreIDRef': data.get("Deposit", {}).get("depositReturn", {}).get(
                                        "lRetailStoreIDRef", ""),
                                    'lWorkstationNmbrRef': data.get("Deposit", {}).get("depositReturn", {}).get(
                                        "lWorkstationNmbrRef", ""),
                                    'lTaNmbrRef': data.get("Deposit", {}).get("depositReturn", {}).get("lTaNmbrRef", ""),
                                    'dTurnover': data.get("Deposit", {}).get("depositReturn", {}).get("dTurnover", ""),
                                    'lNmbrOfItems': data.get("Deposit", {}).get("depositReturn", {}).get("lNmbrOfItems",
                                                                                                            ""),
                                    'dAmount': data.get("Deposit", {}).get("depositReturn", {}).get("dAmount", ""),
                                    'szIdentNmbr': data.get("Deposit", {}).get("depositReturn", {}).get("szIdentNmbr",
                                                                                                        ""),
                                    'sziGateReference': data.get("Deposit", {}).get("depositReturn", {}).get(
                                        "sziGateReference", ""),
                                    'szFirstName': data.get("Deposit", {}).get("depositReturn", {}).get("szFirstName",
                                                                                                        ""),
                                    'szLastName': data.get("Deposit", {}).get("depositReturn", {}).get("szLastName", ""),
                                    'szEmailAddress': data.get("Deposit", {}).get("depositReturn", {}).get(
                                        "szEmailAddress", ""),
                                    'szPhoneNmbr': data.get("Deposit", {}).get("depositReturn", {}).get("szPhoneNmbr",
                                                                                                        ""),
                                    'szExpirationDate': data.get("Deposit", {}).get("depositReturn", {}).get(
                                        "szExpirationDate", ""),
                                    'lTaNmbr': data.get("Deposit", {}).get("depositReturn", {}).get("lTaNmbr", ""),
                                    'lWorkStationNmbr': data.get("Deposit", {}).get("depositReturn", {}).get(
                                        "lWorkStationNmbr", ""),
                                    'lRetailStoreID': data.get("Deposit", {}).get("depositReturn", {}).get(
                                        "lRetailStoreID", ""),
                                    'dOrgTotal': data.get("Deposit", {}).get("depositReturn", {}).get("dOrgTotal", "")
                                    }
                    }, {'depositPayment': {
                'DFS_lPaymentType': data.get("Deposit", {}).get("depositPayment", {}).get("DFS_lPaymentType", ""),
                'szDate': data.get("Deposit", {}).get("depositPayment", {}).get("szDate", ""),
                'bCountAsPayment': data.get("Deposit", {}).get("depositPayment", {}).get("bCountAsPayment", ""),
                'dTaQty': data.get("Deposit", {}).get("depositPayment", {}).get("dTaQty", ""),
                'dPaidForeignCurr': data.get("Deposit", {}).get("depositPayment", {}).get("dPaidForeignCurr", ""),
                'dPaidForeignCurrTotal': data.get("Deposit", {}).get("depositPayment", {}).get("dPaidForeignCurrTotal", ""),
                'dTaPaid': data.get("Deposit", {}).get("depositPayment", {}).get("dTaPaid", ""),
                'dTaPaidTotal': data.get("Deposit", {}).get("depositPayment", {}).get("dTaPaidTotal", ""),
                'DFS_szTransactionID': data.get("Deposit", {}).get("depositPayment", {}).get("DFS_szTransactionID", ""),
                'bIsFirstMedia': data.get("Deposit", {}).get("depositPayment", {}).get("bIsFirstMedia", ""),
                'lPremierTFPaymentMethod': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "lPremierTFPaymentMethod", ""),
                'lMediaNmbr': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("lMediaNmbr", ""),
                'lMediaMember': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("lMediaMember",
                                                                                                        ""),
                'lRetailStoreID': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("lRetailStoreID",
                                                                                                        ""),
                'bDFSRoundingAuto': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "bDFSRoundingAuto", ""),
                'lDFSRoundingMediaMember': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "lDFSRoundingMediaMember", ""),
                'szDesc': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("szDesc", ""),
                'dAcceptHALO': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("dAcceptHALO", ""),
                'bPayOverpaid': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("bPayOverpaid",
                                                                                                        ""),
                'szSerializeTypeCode': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "szSerializeTypeCode", ""),
                'bChangeAllowed': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("bChangeAllowed",
                                                                                                        ""),
                'bChangeAuto': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("bChangeAuto", ""),
                'lChangeMediaMember': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "lChangeMediaMember", ""),
                'bReturnAllowed': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("bReturnAllowed",
                                                                                                        ""),
                'szOpenCashDrawerTypeCode': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "szOpenCashDrawerTypeCode", ""),
                'bPayInputRequired': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "bPayInputRequired", ""),
                'bVoidAllowed': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("bVoidAllowed",
                                                                                                        ""),
                'bTrainingAllowed': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "bTrainingAllowed", ""),
                'bPayInAllowed': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("bPayInAllowed",
                                                                                                        ""),
                'bPayOutAllowed': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("bPayOutAllowed",
                                                                                                        ""),
                'bPickUpAllowed': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("bPickUpAllowed",
                                                                                                        ""),
                'bDebitAllowed': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("bDebitAllowed",
                                                                                                        ""),
                'bReceiptVoidAllowed': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "bReceiptVoidAllowed", ""),
                'szPOSCountingType': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "szPOSCountingType", ""),
                'bPayAllowed': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("bPayAllowed", ""),
                'lValidationEnd': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("lValidationEnd",
                                                                                                        ""),
                'szSpecialUsageVoidReceipt': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "szSpecialUsageVoidReceipt", ""),
                'bReturnInputRequired': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "bReturnInputRequired", ""),
                'bDeclareZeroOnPOS': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "bDeclareZeroOnPOS", ""),
                'bChangeAllowedInCR': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "bChangeAllowedInCR", ""),
                'szPosLogExternalID': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "szPosLogExternalID", ""),
                'dRAcceptHALO': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("dRAcceptHALO",
                                                                                                        ""),
                'bIsLoyalTRealtimeEligible': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get(
                    "bIsLoyalTRealtimeEligible", ""),
                'lPayType': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("lPayType", ""),
                'szPayDesc': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("szPayDesc", ""),
                'lPaySDOC': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("lPaySDOC", ""),
                'lPayDecNmbr': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("lPayDecNmbr", ""),
                'szPayCurrSym': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("szPayCurrSym",
                                                                                                        ""),
                'szISO4217Code': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("szISO4217Code",
                                                                                                        ""),
                'dPayExchRate': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("dPayExchRate",
                                                                                                        ""),
                'dPayLocal': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("dPayLocal", ""),
                'dPayForeign': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("dPayForeign", ""),
                'szPrintCode': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("szPrintCode", ""),
                'szPayCurrSymLC': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("szPayCurrSymLC",
                                                                                                        ""),
                'szPrintCodeLC': data.get("Deposit", {}).get("depositPayment", {}).get("payment", {}).get("szPrintCodeLC",
                                                                                                        "")
                }
                        }
        ]
    except KeyError:
        raise

    for dep in deposit_list:
        flat_deposit['Deposit'].update(dep)

    final_flatten.update(flat_deposit)

    return json.dumps(final_flatten) 

def callInvisibilityApi(url):
    try:
        response=requests.get(url)
        log.info(response.status_code)
        table = etree.HTML(response.content).find("body/table")
        rows = iter(table)
        headers = [col.text for col in next(rows)]
        mcs_location = headers[1]
        log.info('mcs_location from API: %s',mcs_location)
        return mcs_location
    except Exception as e:
        log.error(e)
        raise


def getPostgresConn():
    try:
        conn = psycopg2.connect(
        host="c-dfs-aass-dp-nprd-cosmospostgres-01.wwvvotg4x7fdz4.postgres.cosmos.azure.com",
        database="ToDo",
        user="citus",
        password="Admin@1234")
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        log.error(error)


def enrichCustomerId(params,conn):
    log.info('Inside Enrich customer id')
    americaDivision ={'11':'114147', '41':'114147', '47':'114147'} #11- HAWAI, 41- North America, 47- MIAMI
    if params['division'] in americaDivision.keys():
        params['division'] = americaDivision[params['division']]
    getCustLookupQuery = '''select distinct customer_id from customer_lookup {lookupWhereClause}'''
    getCustIdQuery = '''select distinct customer_id from customer_lookup {strWhereClause}'''
    
    
    orWhereClause=[]
    andWhereClause = []
    andLookupWhereClause = []
    custId=''
    custIdHash=''
    for k,v in params.items():
        clause = k+' = \''+v+'\''
        custIdHash = custIdHash+v
        if k == 'division':
            andWhereClause.append(clause)
        else:
            orWhereClause.append(clause)
        andLookupWhereClause.append(clause)

    strWhereClause=''
    lookupWhereClause=''
    if len(andWhereClause) > 0:
        strWhereClause = strWhereClause + ' and '.join(andWhereClause)
    if len(orWhereClause) > 0:
        strWhereClause = strWhereClause +' and '+ ' or '.join(orWhereClause)
    if len(andLookupWhereClause) > 0:
        lookupWhereClause = lookupWhereClause + ' and '.join(andLookupWhereClause)
    if len(strWhereClause) >0:
        getCustIdQuery = getCustIdQuery.format(strWhereClause = 'where '+ strWhereClause)
    if len(lookupWhereClause) >0:
        getCustLookupQuery = getCustLookupQuery.format(lookupWhereClause = 'where '+ lookupWhereClause)
    log.info('getCustIdQuery :: %s',getCustIdQuery)
    log.info('getCustLookupQuery :: %s',getCustLookupQuery)

    cur = conn.cursor()
    cur.execute(getCustIdQuery)
    customerIdList = cur.fetchall()
    column_values=[]

    if len(customerIdList) == 0: #new cust Id generate
        custId = hashlib.sha256(custIdHash.encode('UTF-8'))
        custId = custId.hexdigest()
        for column in custLookupColumns:
            if column == 'customer_id':
                column_values.append(custId)
            elif column in params.keys():
                column_values.append(params[column])
            else:
                column_values.append(AsIs('null'))
    else:
        custId = customerIdList[0]
        cur.execute(getCustLookupQuery)
        customerIdLookupList = cur.fetchall()
        if len(customerIdLookupList) == 0:
            for column in custLookupColumns:
                if column == 'customer_id':
                    column_values.append(custId)
                elif column in params.keys():
                    column_values.append(params[column])
                else:
                    column_values.append(AsIs('null'))
    
    return column_values,custId


def main(events: func.EventHubEvent) -> str:
    return_values = []
    all_values = []
    all_customer_lookup_values = []
    sales_columns = ['csku_id', 'rsku_id', 'division', 'selling_location_id', 'retail_store_id'
        , 'sold_quantity', 'dh_txn_nmbr', 'ta_type', 'src_updated_at', 'enqueue_timestamp', 'db_updated_at',
                     'event_details']
    insert_query = '''insert into sales_pos_events 
        (''' + ','.join(sales_columns) + ''') values %s '''
    acceptable_ta_type = ['SA', 'RR', 'RT', 'VR', 'Sales', 'ReturnWithReceipt', 'Return', 'ManualInvoiceReturn',
                          'ManualInvoiceSale']
    non_ta_columns = ['division', 'retail_store_id', 'src_updated_at', 'enqueue_timestamp', 'db_updated_at', 'dh_txn_nmbr', 'ta_type', 'event_details']
    insertCustQuery = '''insert into customer_lookup 
        (''' + ','.join(custLookupColumns) + ''') values %s '''
    conn = getPostgresConn()
    for event in events:
        enqueued_time = event.enqueued_time
        raw_json_event=event.get_body().decode('utf-8')
        #log.info('Python EventHub trigger processed an json event: %s',raw_json_event)
        
        # convert into canonical starts
        segregated_json = segregate(raw_json_event)
        flattened_json = flatten(segregated_json)
        #log.info('Canonical format json: %s', flattened_json)
        # convert into canonical end
        json_obj = json.loads(flattened_json)

        column_values = []

        values = {}
        salesDapDict={}
        values['division'] = json_obj['Header']['szExternalID']
        values['retail_store_id'] = json_obj['Header']['lRetailStoreID']
        values['ta_type'] = json_obj['Header']['szTaType']
        values['src_updated_at'] = datetime.strftime(datetime.strptime(json_obj['Header']['szDate'], '%Y%m%d%H%M%S'),
                                                 '%Y-%m-%d %H:%M:%S')
        values['dh_txn_nmbr'] = json_obj['Header']['dh_txn_nmbr']

        # customer lookup starts
        cust_lookup_column_values = []
        custId = ''
        custLookupParams = {}
        custLookupParams['division'] = values['division']
        custLookupParams['loyalty_member_id'] = json_obj['Header']['szLoyaltyMemberID']
        custLookupParams['pax_nmbr'] = json_obj['Header']['szPaxNmbr']
        custLookupParams['passport_nmbr'] = json_obj['Header']['szPassportNmbr']

        if not (not (custLookupParams['division'] and custLookupParams['division'].strip()) or \
                (not (custLookupParams['loyalty_member_id'] and custLookupParams['loyalty_member_id'].strip()) and \
                 not (custLookupParams['pax_nmbr'] and custLookupParams['pax_nmbr'].strip()) and \
                 not (custLookupParams['passport_nmbr'] and custLookupParams['passport_nmbr'].strip()))):
            
            params = {}
            for k, v in custLookupParams.items():
                if (v and v.strip()):
                    params[k] = v
            cust_lookup_column_values, custId = enrichCustomerId(params,conn)
        # customer lookup ends

        json_obj['Header']['customer_id'] = custId
        if values['ta_type'] not in acceptable_ta_type:
            for column in sales_columns:
                if column not in non_ta_columns:
                    column_values.append(AsIs('null'))
                elif column == 'event_details':
                    column_values.append(json.dumps(json_obj))
                elif column == 'enqueue_timestamp':
                    column_values.append(AsIs('to_timestamp(\'{enqueue_timestamp}\',{timestamp_format_milli})'.
                                              format(enqueue_timestamp=enqueued_time,
                                                     timestamp_format_milli=timestamp_format_milli)))
                elif column == 'db_updated_at':
                    current_timestamp =str(datetime.now())
                    #print(current_timestamp)
                    column_values.append(AsIs(
                        'to_timestamp(\'{db_updated_at}\',{timestamp_format_milli})'.format(db_updated_at=current_timestamp,
                                                                                   timestamp_format_milli=timestamp_format_milli)))
                elif column == 'src_updated_at':
                    column_values.append(AsIs(
                        'to_timestamp(\'{updated_at}\',{timestamp_format})'.format(updated_at=values['src_updated_at'],
                                                                                   timestamp_format=timestamp_format)))

                else:
                    column_values.append(values[column])
            return_values.append(flattened_json)
            all_values.append(column_values)

        else:
            values['register'] = json_obj['Header']['lWorkstationNmbr']
            values['indicator'] = 'R'
            values['fromSellingLocation'] = json_obj['Header']['lTransactionTypeID']

            selling_location_url = 'http://10.176.92.70:5555/Inventory/SellingLocation?Division={division}&Store={store}&Register={register}&SKU={sku}&Indicator={indicator}&FromSellingLocation={fromSellingLocation}'
            #test_url = 'https://dfs-aass-dp-nprd-functionapp-01.azurewebsites.net/api/HttpTrigger1?code=4ggfLfpjG4jSKVn39BecqjXV6kKXW6S_b1-CnuAJkLnQAzFuCa-kAw==&name=Manish'

            sales_sku_dict_list = []
            if len(json_obj['LineItems']) > 0:
                for item in json_obj['LineItems']:
                    sales_sku_dict = {}
                    column_values = []
                    values['rsku_id'] = item['szPOSItemID']
                    values['csku_id'] = item['szCommonItemID']
                    values['sold_quantity'] = item['dTaQty']
                    selling_location_url = selling_location_url.format(division=values['division'], store=values['retail_store_id'],
                                                         register=values['register'],
                                                         indicator=values['indicator'],
                                                         fromSellingLocation=values['fromSellingLocation'],
                                                         sku=values['rsku_id'])
                    log.info('mcs selling locatiom url %s',selling_location_url)
                    values['selling_location_id'] = callInvisibilityApi(selling_location_url)

                    sales_sku_dict['Header'] = json_obj['Header']
                    sales_sku_dict['Payments'] = json_obj['Payments']
                    sales_sku_dict['Deposit'] = json_obj['Deposit']
                    sales_sku_dict['LineItem'] = item
                    sales_sku_dict['Header']['mcs_sales_location'] = values['selling_location_id']

                    for column in sales_columns:
                        if column == 'event_details':
                            column_values.append(AsIs('\''+json.dumps(sales_sku_dict)+'\''))
                        elif column == 'enqueue_timestamp':
                            column_values.append(AsIs('to_timestamp(\'{enqueue_timestamp}\',{timestamp_format_milli})'.
                                                      format(enqueue_timestamp=enqueued_time,
                                                             timestamp_format_milli=timestamp_format_milli)))
                        elif column == 'db_updated_at':
                            current_timestamp = str(datetime.now())
                            #print(current_timestamp)
                            column_values.append(AsIs(
                                'to_timestamp(\'{db_updated_at}\',{timestamp_format_milli})'.format(
                                    db_updated_at=current_timestamp,
                                    timestamp_format_milli=timestamp_format_milli)))
                        elif column == 'src_updated_at':
                            column_values.append(AsIs(
                                'to_timestamp(\'{updated_at}\',{timestamp_format})'.format(
                                    updated_at=values['src_updated_at'],
                                    timestamp_format=timestamp_format)))

                        elif values[column] is not None and values[column].strip() != '':
                            column_values.append(values[column])
                        else:
                            column_values.append(AsIs('null'))

                    # log.info('output json: %s', json.dumps(sales_sku_dict))
                    sales_sku_dict_list.append(sales_sku_dict)
                    all_values.append(column_values)
                return_values = sales_sku_dict_list

        if len(cust_lookup_column_values) > 0:
            all_customer_lookup_values.append(cust_lookup_column_values)

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, insert_query, all_values)
    if len(all_customer_lookup_values) > 0:
        psycopg2.extras.execute_values(cur, insertCustQuery, all_customer_lookup_values)
    conn.commit()
    conn.close()

    log.info('Sent record to cosmos DB')
    return json.dumps(return_values)



