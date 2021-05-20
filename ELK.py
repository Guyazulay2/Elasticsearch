import json
from datetime import datetime
from elasticsearch import Elasticsearch
elastic_client = Elasticsearch(hosts=["localhost"])
import os
import argparse
import bz2
from time import sleep


def main():
    if args.create:
        with open(args.data) as f:
            parsed_json = (json.load(f))
            res = elastic_client.index(index=args.create, id=1, body=parsed_json)
            print(res['result'],res)
    else:
        try:
            res = elastic_client.get(index=args.get, id=1)
            print(res['_source']["name"])
        except:
            print("No such index !\nUse --create to create a new index")




if __name__ == '__main__':
    parser = argparse.ArgumentParser('Create index or get data from exists index')
    parser.add_argument('--create', type=str, help='Create new index')
    parser.add_argument('--get', type=str, help='Get data from index')
    parser.add_argument('--data', type=str, help='Json file')
    args = parser.parse_args()
    main()


# es.indices.refresh(index="test-index")

# res = es.search(index="test-index", body={"query": {"match_all": {}}})
# print("Got %d Hits:" % res['hits']['total']['value'])
# for hit in res['hits']['hits']:
#     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])


# es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# i = 0
# with open('el_dharan.json') as raw_data:
#     json_docs = json.load(raw_data)
#     for json_doc in json_docs:
#             i = i + 1
#             es.index(index='ind_dharan', doc_type='doc_dharan', id=i, body=json.dumps(json_doc))
