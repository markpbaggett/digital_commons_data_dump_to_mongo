from pymongo import MongoClient
import json
import xmltodict
import os
import argparse

parser = argparse.ArgumentParser(description='Enter Your OAI Endpoint Information')
parser.add_argument("-p", "--path", dest="path", help="Specify the path to your files", required=True)
parser.add_argument("-c", "--collection", dest="collection", help="Which collection?", required=True)
args = parser.parse_args()

client = MongoClient()
db = client.dltndata
path = args.path
collection = args.collection
mongocollection = db[collection]


def remove_other_bad_stuff(some_bytes):
    good_string = some_bytes.replace(u'\u000B', u'')
    good_string = good_string.replace(u'\u000C', u'')
    good_bytes = good_string.encode("utf-8")
    report = open('metadatadump.txt', 'w')
    report.write(good_string)
    report.close()
    return good_bytes


def add_to_mongo(a_document):
    metadata = a_document['documents']
    record_id = a_document['documents']['document']['coverpage-url']
    result = mongocollection.update({"record_id": record_id},
                                    {"record_id": record_id, "metadata": metadata}, True)
    return


total_records = 0
for x in os.walk(path):
    test = list(x)
    new_path = test[0]
    if new_path != path:
        full_path = ('{0}/metadata.xml'.format(new_path))
        f = open(full_path)
        s = f.read()
        clean = remove_other_bad_stuff(s)
        json_string = json.dumps(xmltodict.parse(clean))
        json_document = json.loads(json_string)
        try_adding = add_to_mongo(json_document)
        total_records += 1
print('Done!')
print('Created {0} total records.'.format(total_records))
