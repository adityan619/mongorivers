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
_FETCH_LIMIT = 80000


_categories = ['Infowindow-pg', 'Infowindow-rent', 'Infowindow-new-projects', 'Infowindow-buy', 'Form',
               'Filters-rent', 'Filters-buy', 'Filters-pg', 'search', 'page_type', 'np_list_select', 'details_page', 'search']


def get_collection(category):
    return db[category]

def process_pipeline(collection, mongo_fetch_generator, server_object_ids_range):
    for fetch_range in mongo_fetch_generator:
        mongo_fetched_data_dict = mongo_fetch_data.fetch_mongo_data_for_fetch_range(collection, fetch_range[0],fetch_range[1])
        push_data_to_es.dump_data_dict_to_es_readable_file(mongo_fetched_data_dict)
        push_data_to_es.push_data_file_to_es()

def push_data_to_elasticsearch():
    for category in _categories:
        for date_range in get_date_range_list(_NUMBER_OF_DAYS):
            print "Started pushing events for {category} of {date_range}".format(category=category, date_range=date_range)
            raw_data_collection = mongo_helpers.get_mongo_db_con(
                    database=_MONGO_RAW_DATA_DB)[category]
            server_object_ids_range = mongo_helpers.get_server_object_ids(date_range)
            mongo_fetch_generator = mongo_helpers.create_mongo_fetch_generator(raw_data_collection)
            process_pipeline(raw_data_collection,mongo_fetch_generator,server_object_ids_range)


if __name__ == '__main__':
    push_data_to_elasticsearch()
