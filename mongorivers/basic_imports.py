import logging


LOGGER = logging.getLogger("MongoEsRiver")
# logging.basicConfig(filename="log_user_profile.log",level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.basicConfig(filename="log_MongoEsRiver.log",level=logging.DEBUG, format='%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")