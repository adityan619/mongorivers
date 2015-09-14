mongo_conf = {
    'mongo_uri' : "mongodb://read_usr:read_pwd@my-mongo-server:27017/",
    'mongo_params_uri' : "mongodb://write_usr:write_pwd@my-mongo-server:27017/",
    'raw_data_db' : 'analytics',
    'raw_data_collection' : 'analytics_data',
    'params_db' : 'dsl_params',
    'params_collection' : 'params'
    ''
}

es_conf = {
    'es_host' : 'http://localhost:9200'
    'es_index' : 'live_events_v1',
}