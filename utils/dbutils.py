from db import mongobase
from config import constants


def get_mongodb_connection(uri=constants.LOCAL_MONGO_HOSTNAME,port_no=constants.LOCAL_MONGO_PORT,db_name=constants.DB_NAME):
    mongo = mongobase.MongoConnector(uri,port_no)
    mongo.set_db(db_name)
    return mongo
