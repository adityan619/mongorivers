import mongo_helpers
import mongo_fetch_data
import push_data_to_es
from basic_imports import LOGGER
from conf import mongo_conf


_MONGO_PARAMS_DB = 'params_db'
_MONGO_PARAMS_COLLECTION = 'params_collection'
_MONGO_RAW_DATA_DB = 'raw_data_db'
_MONGO_RAW_DATA_COLLECTION = 'raw_data_collection'
_NUMBER_OF_DAYS = 30


_collections = ['Infowindow-pg', 'Infowindow-rent', 'Infowindow-new-projects', 'Infowindow-buy', 'Form',
                'Filters-rent', 'Filters-buy', 'Filters-pg', 'search', 'page_type', 'np_list_select', 'details_page', 'search']


def get_collection(collection):
    return db[collection]


def process_pipeline(collection, mongo_fetch_generator, server_object_ids_range):
    for fetch_range in mongo_fetch_generator:
        mongo_fetched_data_dict = mongo_fetch_data.fetch_mongo_data_for_fetch_range(
            collection, fetch_range[0], fetch_range[1])
        push_data_to_es.dump_data_dict_to_es_readable_file(
            mongo_fetched_data_dict)
        push_data_to_es.push_data_file_to_es()


def push_data_to_elasticsearch():
    for collection in _collections:
        for date_range in mongo_helpers.get_date_range_list(_NUMBER_OF_DAYS):
            print "Started pushing events for {collection} of {date_range}".format(collection=collection, date_range=date_range)
            raw_data_collection = mongo_helpers.get_mongo_db_con(
                database=_MONGO_RAW_DATA_DB)[collection]
            mongo_object_ids_range = mongo_helpers.get_server_object_ids(
                date_range)
            query_filters = {'_id': {'$gte': mongo_object_ids_range[0], '$lte': mongo_object_ids_range[
                1]}}
            mongo_fetch_generator = mongo_helpers.create_mongo_fetch_generator(
                raw_data_collection, query_filters)
            process_pipeline(
                raw_data_collection, mongo_fetch_generator, query_filters)


if __name__ == '__main__':
    push_data_to_elasticsearch()
