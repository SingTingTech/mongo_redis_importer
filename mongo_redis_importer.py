#!/usr/local/bin/python3

import redis
import pymongo
import json
import getopt
import sys
import logging

logging.basicConfig(format="%(asctime)s -- %(levelname)8s -- %(processName)20.20s -- %(message)s")

mongo = pymongo.MongoClient('mongodb://raisy:raisy@192.168.22.21:27016/?authSource=admin')
db = mongo.get_database('test')

r = redis.StrictRedis(host="192.168.22.21", port=6379, db=0)


def redis_data_listener(keys: list):
    while True:
        value: list
        try:
            value = r.brpop(keys)
            collection = db.get_collection(str(value[0], encoding='utf-8'))
            data = json.loads(str(value[1], encoding='utf-8'))

            collection.insert_one(data)
        except json.decoder.JSONDecodeError:
            logging.error(
                "data from redis list is not a valid json, abandoning: {}".format(str(value[1], encoding='utf-8')))
        except redis.exceptions.ResponseError:
            logging.error("some key is not a list key")
            sys.exit(-1)


if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv, '', [])
    if len(args) > 1:
        redis_data_listener(args[1:])
    else:
        print('''Useage:

    mongo_redis_importer listK1 [listK2] [listK3] ..
        ''')
