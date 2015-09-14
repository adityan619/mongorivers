_ES_READ_FILE = 'es_update_events.json'

def push_data_file_to_es():
    data = open(_ES_READ_FILE,'rb').read()
    if not data:
        return
    LOGGER.debug("{0} pushing data to es")
    req = urllib2.Request(es_conf[_ES_HOST]+'_bulk', data)
    req.add_header('Content-Length', '%d' % len(data))
    req.add_header('Content-Type', 'application/octet-stream')
    res = urllib2.urlopen(req)
    LOGGER.debug("pushing data to es success")

def dump_data_dict_to_es_readable_file(mongo_fetched_data_dict):
    put = {"index": {"_index": es_conf[_ES_INDEX], "_type": "wevent"}}
    events_dump_file = open(_ES_READ_FILE, 'w')
    try:
        for document in mongo_fetched_data_dict:
            put['index']['_type'] = document['category']
            json.dump(put,events_dump_file)
            events_dump_file.write('\n')
            json.dump(document,events_dump_file)
            events_dump_file.write('\n')
    except Exception, e:
        LOGGER.error(
                "For document {0} encountered dumping data error {1} ".format(document, e))