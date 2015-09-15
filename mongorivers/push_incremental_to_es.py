import time
import mongo_helpers
import mongo_fetch_data
import push_data_to_es
from basic_imports import LOGGER
from conf import mongo_conf, es_conf

_MONGO_PARAMS_DB = 'params_db'
_MONGO_PARAMS_COLLECTION = 'params_collection'
_MONGO_RAW_DATA_DB = 'raw_data_db'
_MONGO_RAW_DATA_COLLECTION = 'raw_data_collection'
_ES_HOST = 'es_host'
_ES_INDEX = 'es_index'

_categories = ['Infowindow-pg', 'Infowindow-rent', 'Infowindow-new-projects', 'Infowindow-buy', 'Form',
               'Filters-rent', 'Filters-buy', 'Filters-pg', 'search', 'page_type', 'np_list_select', 'details_page', 'search']

def process_pipeline(collection, mongo_fetch_generator, query_filters):
    for fetch_range in mongo_fetch_generator:
        query_filter = {'category':_categories}
        mongo_fetched_data_dict = mongo_fetch_data.fetch_mongo_data_for_fetch_range(collection, fetch_range[0],fetch_range[1],query_filters = query_filters)
        push_data_to_es.dump_data_dict_to_es_readable_file(mongo_fetched_data_dict)
        push_data_to_es.push_data_file_to_es()


def obtain_time_ranges():
    params_collection = mongo_helpers.get_mongo_db_con(
        database=_MONGO_PARAMS_DB)[mongo_conf[_MONGO_PARAMS_COLLECTION]]
    lastUpdatedTimeStamp = int(params_collection.find_one(
        {'elasticsearch.lastUpdated': {'$exists': 'true'}})['elasticsearch']['lastUpdated'])
    currentTimeStamp = int(time.time()*1000)
    return [lastUpdatedTimeStamp, currentTimeStamp]


def push_incremental_data_to_es():
    #lastUpdated = 1409250599992
    timestamp_range = obtain_time_ranges()
    LOGGER.debug(
        "Started river to push data to ES for {0}".format(timestamp_range))
    mongo_object_ids_range = mongo_helpers.get_server_object_ids(
        timestamp_range=timestamp_range)
    raw_data_collection = mongo_helpers.get_mongo_db_con(
        database=_MONGO_RAW_DATA_DB)[mongo_conf[_MONGO_RAW_DATA_COLLECTION]]
    query_filters = {'_id': {'$gte': mongo_object_ids_range[0], '$lte': mongo_object_ids_range[
        1]}, 'category': {'$in': _categories}}
    mongo_fetch_generator = mongo_helpers.create_mongo_fetch_generator(raw_data_collection,query_filters)
    process_pipeline(raw_data_collection,mongo_fetch_generator,query_filters)
    params_collection.update({'elasticsearch.lastUpdated': {'$exists': 'true'}}, {
                             '$set': {'elasticsearch.lastUpdated': str(timestamp_range[1])}})


if __name__ == '__main__':
    push_incremental_data_to_es()

# "{elasticsearch:{lastUpdated:str(1426012200000)}}"
