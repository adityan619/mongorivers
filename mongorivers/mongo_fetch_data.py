
def fetch_mongo_data_for_fetch_range(collection, offset, fetch_limit):
    _mongo_fetched_data_dict = []
    data_cursor = collection.find({'_id': {'$gte': server_object_ids_range[0], '$lte': server_object_ids_range[1]}, 'category': {
        '$in': categories}}, {'_id': 0}).skip(offset).limit(fetch_limit)
    for document in data_cursor:
        try:
            if not 'category' in document or doc['category'] not in _categories:
                continue
            _mongo_fetched_data_dict.append(document)
        except Exception, e:
             LOGGER.error(
                "For document {0} encountered error {1} ".format(doc, e))
    return _mongo_fetched_data_dict