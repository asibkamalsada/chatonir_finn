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


def create_index(es_client):
    """ Creates an Elasticsearch index."""
    is_created = False
    # Index settings
    settings = {
        "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 1
        },
        "mappings": {
            "dynamic": "true",
            "_source": {
            "enabled": "true"
            },
            "properties": {
                "body": {
                    "type": "text"
                }
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
    """ Indexs all the rows in data (python questions)."""
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

    search = {"size": 100,"query": {"match": {"body": query}}}
    print(search)
    response = es_client.search(index=INDEX_NAME, body=json.dumps(search))
    print()
    print("{} total hits.".format(response["hits"]["total"]["value"]))
    for hit in response["hits"]["hits"]:
        print("id: {}, score: {}".format(hit["_id"], hit["_score"]))
        print(hit["_source"])
        print()

data = [readPDF('Decomposability, Transparency and Multipoles (50 min).pdf')]
data.append(["wir schreiben hier viele sachen rein", "eigentlich muss ", "ich mehr sachen"])
data.append(["test", "newtest", "noch ein test weil es so lustig ist"])
es_client = Elasticsearch()
create_index(es_client)
index_data(es_client, data)
SEARCH_SIZE = 3
run_query_loop()
