# mongorivers
Has modules to fetch data from mongodb and push it into other services

This is a very basic framework to use it as a river from MongoDB to ES.
To Start using this framework, make a copy of settings_sample in config folder and name it settings_local.py
Server configurations and appropriate database configurations are to be modified in the settings_local.py of 'config' folder.
Data is fetched from the mongo server from appropriate databases and pushed to elasticsearch.
There are helper files to fetch data from mongodb and write the data to a file in a format which ES bulk API expects.
