import os
import imp

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
# from sys import path
# path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.path.pardir)))

# print path
# print BASE_DIR
if os.path.isfile(BASE_DIR+'/config/settings_local.py'):
    print "found settings_local"
    # from settings_local import *
    settings_local = imp.load_source('settings_local',BASE_DIR+'/config/settings_local.py')
    from settings_local import *