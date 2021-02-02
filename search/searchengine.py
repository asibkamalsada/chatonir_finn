import PyPDF2
from io import StringIO
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
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

    def readJSON(self, path):
        with open(path, 'r', encoding='utf8') as file:
            return json.load(file)

        return jsondata

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
                    "keyqueries": {"type": "text"}
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

    def iterate_all_doc(self, pagesize=10000, scroll_timeout="10m"):
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
            yield from hits


    def precalc_keyquerys(self):
        for entry in self.iterate_all_doc(scroll_timeout="5m"):
            kq = [list(x) for x in keyqueries.full_keyquery(self.es_client, entry)]
            self.es_client.update(index=self.INDEX_NAME, id = entry["_id"], body={"doc": {"keyqueries": kq}})
            print("Updated \"" + entry["_source"]["title"] + "\"")

        print("Finished updating")

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
        bulk(self.es_client, requests)

    def createIndexAndIndexDocs(self, path):
        self.create_index()
        self.index_data(self.readJSON(path))

    def run_query_loop(self):
        """ Asks user to enter a query to search."""
        while True:
            try:
                self.handle_query(input('enter query\n'))
            except KeyboardInterrupt:
                break
        return

    def handle_query(self, query):
        """ Searches the user query and finds the best matches using elasticsearch."""
        # query = input("Enter query: ")
        # to search more then one field, use multi search api
        # search = {"size": SEARCH_SIZE,"query": {"match": {"title": query}}}
        search = {"size": self.SEARCH_SIZE, "query": {"multi_match": {"query": query, "fields": ["title", "authors"]}}}
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

            response = self.handle_query(query)

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

        for seed in papers:
            print(seed)
            k = keyqueries.full_keyquery(self.es_client, seed)
            print(f"{seed['_source']['title']} {k}")
