import PyPDF2
from io import StringIO
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

INDEX_NAME = "paper"


def readPDF(path):
    file = open(path, 'rb')
    pdfReader = PyPDF2.PdfFileReader(file)

    title = str(pdfReader.getDocumentInfo().title)
    creator = str(pdfReader.getDocumentInfo().creator)
    numOfPages = pdfReader.getNumPages()
    concat = StringIO()

    for i in range(0, numOfPages):
        print(i)
        pageObject = pdfReader.getPage(i)
        concat.write(pageObject.extractText())
    concat.getvalue().replace("\n", " ")
    file.close()
    return [title, creator, concat.getvalue().replace("\n", " ")]


def readJSON(path):
    jsondata = []
    for line in open(path, 'r', encoding='utf8'):
        jsondata.append(json.loads(line))

    return jsondata


def create_index(es_client):
    """ Creates an Elasticsearch index."""
    is_created = False
    # Index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {
            "dynamic": "true",
            "_source": {
                "enabled": "true"
            },
            "properties": {
                "title": {"type": "text"},
                "dblpKey": {"type": "keyword"},
                "doi": {"type": "keyword"}, # typo read "dpi" first
                "authors": {"type": "text"},
                "publisher": {"type": "text"},
                "booktitle": {"type": "text"}
            }
        }
    }
    print('Creating `Question` index...')
    try:
        if es_client.indices.exists(INDEX_NAME):
            es_client.indices.delete(index=INDEX_NAME, ignore=[404])
        es_client.indices.create(index=INDEX_NAME, body=settings)
        is_created = True
        print('index created successfully.')
    except Exception as ex:
        print(str(ex))
    finally:
        return is_created


def index_data(es_client, data, BATCH_SIZE=100000):
    """ Indexs all the rows in data"""
    """
    docs = []
    count = 0
    for line in data:
        js_object = {}
        js_object['body'] = line
        docs.append(js_object)
        count += 1

        if count % BATCH_SIZE == 0:
            index_batch(docs)
            docs = []
            print('Indexed {} documents.'.format(count))
    if docs:
        index_batch(docs)
        print('Indexed {} documents.'.format(count))

    es_client.indices.refresh(index=INDEX_NAME)
    print("Done indexing.")
    """
    for i in data:
        insert_one_data(i)
        print('Indexed {} document.'.format(i))

    print("Done indexing!!! Wuhu")


def insert_one_data(doc):
    res = es_client.index(index=INDEX_NAME, body=doc)
    print(res)


def index_batch(docs):
    """ Indexes a batch of documents."""
    requests = []
    for i, doc in enumerate(docs):
        request = doc
        request["_op_type"] = "index"
        request["_index"] = INDEX_NAME
        request["body"] = doc['body']
        requests.append(request)
    bulk(es_client, requests)


def createIndexAndIndexDocs(es_client, path):
    data = readJSON("dump_no_bodytext.json")
    create_index(es_client)
    index_data(es_client, data)


def run_query_loop():
    """ Asks user to enter a query to search."""
    while True:
        try:
            handle_query()
        except KeyboardInterrupt:
            break
    return


def handle_query():
    """ Searches the user query and finds the best matches using elasticsearch."""
    query = input("Enter query: ")
    # to search more then one field, use multi search api
    # search = {"size": SEARCH_SIZE,"query": {"match": {"title": query}}}
    search = {"size": SEARCH_SIZE, "query": {"multi_match": {"query": query, "fields": ["title", "authors"]}}}
    search = {
        "query": {
            "match": {
                "title": {
                    "query": query,
                    "operator": "and"
                }
            }
        }
    }
    print(search)
    response = es_client.search(index=INDEX_NAME, body=json.dumps(search))
    print()
    print("{} total hits.".format(response["hits"]["total"]["value"]))
    for hit in response["hits"]["hits"]:
        print("id: {}, score: {}".format(hit["_id"], hit["_score"]))
        print(hit["_source"])
        print()


es_client = Elasticsearch()
""" only run this the first time!!! Might take 15-20 Minutes """
# createIndexAndIndexDocs(es_client, "dump_no_bodytext.json")

SEARCH_SIZE = 10
run_query_loop()
