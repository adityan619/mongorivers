from basic_imports import LOGGER

def fetch_mongo_data_for_fetch_range(collection, offset, fetch_limit,query_filters):
    _mongo_fetched_data_dict = []
    data_cursor = collection.find(query_filters, {'_id': 0}).skip(offset).limit(fetch_limit)
    for document in data_cursor:
        try:
            if not 'category' in document:
                continue
            _mongo_fetched_data_dict.append(document)
        except Exception, e:
            LOGGER.error(
                "For document {0} encountered error {1} ".format(doc, e))
    return _mongo_fetched_data_dict
