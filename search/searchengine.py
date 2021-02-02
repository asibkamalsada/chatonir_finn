import itertools
from collections import deque

import PyPDF2
from io import StringIO
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
import json
import textrank
import keyqueries


class Searchengine():

    def __init__(self):
        self.INDEX_NAME = "paper"
        self.SEARCH_SIZE = 10
        self.es_client = Elasticsearch()

    def readPDF(self, path):
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

    def readJSON_(self, path):
        with open(path, 'r', encoding='utf8') as file:
            return [json.loads(line) for line in file.readlines()]

    def readJSON(self, path):
        with open(path, 'r', encoding='utf8') as file:
            return json.load(file)

    def read2lines(self, path):
        with open(path, 'r', encoding='utf8') as file:
            return file.readlines()

    def create_index(self):
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
                    "doi": {"type": "keyword"},
                    "authors": {"type": "text"},
                    "publisher": {"type": "text"},
                    "booktitle": {"type": "text"},
                    "keyqueries": {"type": "flattened"},
                    "abstract": {"type": "text"}
                }
            }
        }
        print('Creating `Paper` index...')
        try:
            if self.es_client.indices.exists(self.INDEX_NAME):
                self.es_client.indices.delete(index=self.INDEX_NAME, ignore=[404])
            self.es_client.indices.create(index=self.INDEX_NAME, body=settings)
            is_created = True
            print('index created successfully.')
        except Exception as ex:
            print(str(ex))
        finally:
            return is_created

    def chunk_iterate_docs(self, pagesize=10000, scroll_timeout="10m"):
        is_first = True
        while True:
            if is_first:
                result = self.es_client.search(index=self.INDEX_NAME, scroll=scroll_timeout,
                                               body={"query": {"match_all": {}}, "size": pagesize})
                is_first = False
            else:
                result = self.es_client.scroll(body={
                    "scroll_id": scroll_id,
                    "scroll": scroll_timeout})
            scroll_id = result["_scroll_id"]
            hits = result["hits"]["hits"]
            if not hits:
                break
            yield hits

    def index_data(self, data, batch_size=10000):
        """ Indexs all the rows in data"""
        for chunk in [data[x:x + batch_size] for x in range(0, len(data), batch_size)]:
            self.index_batch(chunk)
            # print(f'Indexed {doc} document.')

        print("Done indexing!!! Wuhu")

    def insert_one_data(self, doc):
        res = self.es_client.index(index=self.INDEX_NAME, body=doc)
        # print(res)

    def index_batch(self, docs):
        """ Indexes a batch of documents."""
        requests = []
        for doc in docs:
            request = dict()
            request["_op_type"] = "index"
            request["_index"] = self.INDEX_NAME
            request["_source"] = doc
            requests.append(request)
        deque(parallel_bulk(self.es_client, requests), maxlen=0)  # the deque stuff is just to discard results

    def createIndexAndIndexDocs(self, path):
        self.create_index()
        self.index_data(self.readJSON_(path))

    def run_query_loop(self):
        """ Asks user to enter a query to search."""
        while True:
            try:
                self.title_search(input('enter query\n'))
            except KeyboardInterrupt:
                break
        return

    def title_search(self, title, size=10000):
        """ Searches the user query and finds the best matches using elasticsearch."""
        # query = input("Enter query: ")
        # to search more then one field, use multi search api
        # search = {"size": SEARCH_SIZE,"query": {"match": {"title": query}}}
        search = {"size": self.SEARCH_SIZE, "query": {"multi_match": {"query": title, "fields": ["title", "authors"]}}}
        search = {
            "size": size,
            "query": {
                "match": {
                    "title": {
                        "query": title,
                        "operator": "and"
                    }
                }
            }
        }
        # print(search)
        response = self.es_client.search(index=self.INDEX_NAME, body=search)
        """
        print("{} total hits.".format(response["hits"]["total"]["value"]))
        for hit in response["hits"]["hits"]:
            print("id: {}, score: {}".format(hit["_id"], hit["_score"]))
            print(hit["_source"]["title"])
            print()
        """
        return response

    def search(self):
        papers = []
        while True:
            query = input("Enter the paper you want to keyquerie: (empty input to cancel)")
            if not query:
                break

            response = self.title_search(query)

            print("Please select the paper(s) you want to use like so ('1, 3, 10')")
            print('\n'.join(
                [f'{index} : {paper["_source"]["title"]}' for index, paper in enumerate(response["hits"]["hits"])]))

            while True:
                numbers = input()
                numbers = numbers.split(",")
                if all(elem.isdigit() and int(elem) in range(0, len(response["hits"]["hits"])) for elem in numbers):
                    for n in numbers:
                        papers.append(response["hits"]["hits"][int(n)])
                    break
                else:
                    print("Wrong Input, pls try again!")

            print('currently selected papers: [\n    {}\n]'.format(
                "\n    ".join(paper['_source']['title'] for paper in papers)))

            ask = input("Do you want to add another paper [Y/n]?")
            if ask == 'n':
                break

        for paper in papers:
            print(paper["_source"].get("keyqueries", "duh"))

    def update_abstracts(self, path):
        doi_abs = self.readJSON(path)
        doi_id = {hit["_source"]["doi"]: hit["_id"] for hits in self.chunk_iterate_docs() for hit in hits}

        gen = ((doi_id[d_a["doi"]], d_a["abstract"]) for d_a in doi_abs if d_a["doi"] in doi_id)

        self.chunk_update_field("abstracts", gen)

    def update_keyqueries(self):
        chunk_size = 1000
        k = keyqueries.Keyqueries()
        for entries in self.chunk_iterate_docs(pagesize=chunk_size):
            self.chunk_update_field("keyqueries",
                                    ((entry["_id"], {" ".join(kws): score for (kws, score) in k.optimized(entry)}) for entry in entries),
                                    chunk_size=chunk_size)

    def chunk_update_field(self, field, gen, chunk_size=1000, page_size=None):
        gen_ = ({"_index": self.INDEX_NAME,
                 "_op_type": "update",
                 "_id": _id,
                 "doc": {field: value}} for _id, value in itertools.islice(gen, chunk_size))

        if page_size:
            if page_size <= chunk_size:
                bulk(self.es_client, gen_)
        else:
            try:
                while True:
                    actions = [next(gen_)]
                    bulk(self.es_client, actions)
                    bulk(self.es_client, itertools.islice(gen_, chunk_size))
            except StopIteration:
                pass
        print(f"{field} successfully read")

    def print_kqs(self):
        kqs = [hit["_source"].get("keyqueries", "") for hits in self.chunk_iterate_docs() for hit in hits]
        print(len(kqs))

    def extract_json(self, search_phrase, file_name=None):
        if not file_name:
            file_name = f"{search_phrase}.json"
        with open(file_name, "w") as file:
            file.write(json.dumps([hit["_source"] for hit in self.title_search(search_phrase, size=1000)["hits"]["hits"]]))
