from pymongo import MongoClient, ReadPreference
from bson.objectid import ObjectId
from collections import defaultdict
import datetime
import itertools
import time
from conf import mongo_conf
#current_day =datetime.datetime.now().strftime('%d/%m/%Y')

_MONGO_URI = 'mongo_uri'
_MONGO_DB = 'test'

_date_range_list = ( [(datetime.datetime.today().date()-datetime.timedelta(x+1)) , (datetime.datetime.today().date()-datetime.timedelta(x))]  for x in itertools.count())

def get_mongo_db_con(database=_MONGO_DB):
    connection = MongoClient(mongo_conf[_MONGO_URI]+'/'+mongo_conf[database])[mongo_conf[database]]
    return connection

def get_server_object_ids(**kwargs):
    # y = datetime.datetime.now().date() - datetime.timedelta(days=1)
    # t= datetime.datetime.now().date()
    if kwargs is not None:
        if 'time_range' in kwargs:
            prev, curr = kwargs['time_range']
            print prev, curr
            prev_timestamp =  int(time.mktime(prev.timetuple())*1000)
            curr_timestamp = int(time.mktime(curr.timetuple())*1000)
        elif 'timestamp_range' in kwargs:
            prev_timestamp, curr_timestamp = kwargs['timestamp_range']
        else:
            raise Exception('Keyword Error')

    prev_utc_datetime = datetime.datetime.utcfromtimestamp(prev_timestamp/1000)
    curr_utc_datetime = datetime.datetime.utcfromtimestamp(curr_timestamp/1000)

    prev_utc_objectId = ObjectId.from_datetime(prev_utc_datetime)
    curr_utc_objectId = ObjectId.from_datetime(curr_utc_datetime)

    return prev_utc_objectId, curr_utc_objectId

def create_mongo_fetch_generator(collection, server_object_ids_range):
    fetch_limit = _FETCH_LIMIT
    records = collection.find({'_id': {'$gte': server_object_ids_range[0], '$lte': server_object_ids_range[
        1]}, 'category': {'$in': _categories}}, {'_id': 0}).count() + fetch_limit
    offset = 0
    while (records > 0):
        yield offset, fetch_limit
        offset += fetch_limit
        records -= fetch_limit
        LOGGER.debug("Fetching another limit of  {0}".format(str(fetch_limit)))


def get_date_range_list(days=1):
    return [ [(datetime.datetime.today().date()-datetime.timedelta(x+1)) , (datetime.datetime.today().date()-datetime.timedelta(x))]  for x in xrange(0,days) ]
# print get_date_range_list(5)
