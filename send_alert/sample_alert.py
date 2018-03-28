import hashlib
from datetime import datetime, timedelta
import pytz
from es_writer import EsWriter
from redis_client import RedisClient

utc = pytz.utc
fmt = '%Y-%m-%dT%H:%M:%S%z'
omp_time = datetime.strftime(utc.localize(datetime.utcnow() - timedelta(hours=8)), fmt)

# message_writer = EsWriter(host='35.184.66.182')
message_writer = RedisClient(host='146.148.51.45', port=6379)

IP, PORT = '127.0.0.1', 19091
LIVE_ALERT_INDEX = 'live_alert_index'
ALERT_TYPE = 'alert'


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


def get_event(triggerId):
    alert = {
        "storedTimestamp": omp_time,
        "assignedToName": "",
        "domain": "IPM",
        "orgId": "qdjj0vp",
        "extraData": {},
        "incidentNumber": "",
        "geolocLon": "",
        "sourceEventsCount": 1,
        "eventType": "monza_events",
        "platforms": [""],
        "relatedDatesRaised": [],
        "detailsURL": "NA",
        "locationLabel": "",
        "message": "[Monza] PROBLEM: Used disk space is more than 85% on volume D:\\ Label:New Volume  Serial Number 12ds23",
        "relatedStatesIds": [""],
        "isReset": False,
        "objectType": "alertState",
        "category": "Server",
        "monitoredCIID": "",
        "producer": "collector.monza.events",
        "objectId": "",
        "title": "Test alert %s" % triggerId,
        "trigger": "Test alert %s" % triggerId,
        "comments": "Used disk space on D:\\ Label:New Volume  Serial Number 12ds23: 93.63 GB\r\nTotal disk space on D:\\ Label:New Volume  Serial Number 12ds23: 100 GB",
        "status": "new",
        "version": 1,
        "location": "",
        "KBArticle": "",
        "toolUUID": "",
        "testedLocation": "",
        "closedTimestamp": None,
        "relatedEventsIds": [""],
        "description": "",
        "workDuration": "",
        "locationCoordinates": [51.2, -0.12],
        "dateRaised": omp_time.split('T')[0],
        "monitoredCIName": "lo3wplogtapp01",
        "raisedLocalTimestamp": omp_time,
        "locationCode": "europe-west2-a",
        "severity": 4,
        "count": "1",
        "stateTriggerId": "%s" % triggerId,
        "activeDuration": "",
        "monitoredCIClass": "",
        "assignedToId": "",
        "resetTimestamp": None,
        "geolocLat": "",
        "products": [""],
        "timestampUpdated": omp_time,
        "raisedTimestamp": omp_time,
        "dateHourEnded": None,
        "priority": "P1"
    }

    alert['eventId'] = get_hash(event_hash_string(alert))
    alert['id'] = get_hash(event_id_string(alert))

    print alert
    print '-' * 30
    return alert


for i in range(80, 82):
    alert_json = get_event(i)
    message_writer.send_message(alert_json)
