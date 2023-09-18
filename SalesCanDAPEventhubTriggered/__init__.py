import logging

import azure.functions as func
import json

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
log.info("Logging Info")
log.debug("Logging Debug")

def main(events: func.EventHubEvent) -> str:
    for event in events:
        raw_json_event=event.get_body().decode('utf-8')
        log.info('Python EventHub trigger processed an json event: %s',raw_json_event)
        #json_obj = json.loads(raw_json_event)
        #if len(json_obj['l'])