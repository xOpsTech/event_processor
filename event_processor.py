import logging
import logs
import hashlib
import json
import redis
from esClient import EsClient

LIVE_ALERT_INDEX = 'live_alert_index'
HISTORICAL_ALERT_INDEX = 'historical_alert_index'
ALERT_TYPE = 'alert'

es_client = EsClient(host='104.196.60.89')
redis_db = redis.client.StrictRedis(host='146.148.51.45', port=6379)


def event_hash_string(event_obj):
    hash_string = '%s%s%s' % (
        event_obj['producer'], event_obj['stateTriggerId'], event_obj['locationCode']
    )
    return hash_string


def event_id_string(event_obj):
    hash_string = '%s%s%s%s' % (
        event_obj['producer'], event_obj['stateTriggerId'], event_obj['locationCode'], event_obj['raisedTimestamp']
    )
    return hash_string


def get_hash(hash_string):
    return hashlib.md5(hash_string).hexdigest()


while True:
    msg = redis_db.blpop(keys='alerts')

    if not msg:
        continue

    received_alert = json.loads(msg[1])
    event_id = get_hash(event_hash_string(received_alert))
    stored_alert = es_client.get_alert_by_event_id(index=LIVE_ALERT_INDEX, doc_type=ALERT_TYPE, doc_id=event_id)

    is_reset = received_alert.get('isReset')

    if is_reset is False:
        # a new problem alert
        if stored_alert is None:
            unique_id = get_hash(event_id_string(received_alert))
            received_alert['id'] = unique_id
            received_alert['eventId'] = event_id
            es_client.create_index_data(index=LIVE_ALERT_INDEX, doc_type=ALERT_TYPE, id=event_id, body=received_alert)
        else:
            # existing problem alert
            es_client.update_event(index=LIVE_ALERT_INDEX, doc_type=ALERT_TYPE, body=received_alert,
                                   alert_from_db=stored_alert)

    elif is_reset is True and stored_alert is not None:
        # recovery alert for already stored problem alert
        stored_alert['status'] = "recovered"
        stored_alert['resetTimestamp'] = received_alert['raisedTimestamp']
        stored_alert['isReset'] = True
        result = es_client.create_index_data(index=HISTORICAL_ALERT_INDEX, doc_type=ALERT_TYPE, id=stored_alert['id'],
                                             body=stored_alert)

        if result:
            es_client.delete_index_data(index=LIVE_ALERT_INDEX, doc_type=ALERT_TYPE, id=event_id)
    else:
        # recovery alert without a problem alert in db
        print "process: ignoring recovery alert | reason: no problem alert is found in db"
