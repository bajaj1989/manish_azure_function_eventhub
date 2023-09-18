import logging

import azure.functions as func
import json
import requests
from lxml import etree

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
log.info("Logging Info")
log.debug("Logging Debug")

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

def main(req: func.HttpRequest) -> str:
    logging.info('Python HTTP trigger function processed a request.')
    selling_location_url='http://10.176.92.70:5555/Inventory/SellingLocation?Division=58&Store=451&Register=632&SKU=18299120&Indicator=R&FromSellingLocation=451'

    response = callInvisibilityApi(selling_location_url)
    return response

