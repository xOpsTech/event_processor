from elasticsearch import Elasticsearch

LIVE_ALERT_INDEX = 'live_alert_index'
HISTORICAL_ALERT_INDEX = 'historical_alert_index'
ALERT_TYPE = 'alert'


class EsWriter(object):
    def __init__(self, host, port=9200):
        self.es_client = Elasticsearch(host.split(','), timeout=20)

    def send_message(self, message):
        self.es_client.index(index=LIVE_ALERT_INDEX, doc_type=ALERT_TYPE, id=message['eventId'],
                             body=message)
