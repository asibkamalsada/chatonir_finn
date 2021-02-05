import itertools
from collections import Counter

import PyPDF2
from io import StringIO
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

import keyqueries


def print_return(param):
    print(param)
    return param


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
                    "publisher": {"type": "keyword"},
                    "booktitle": {"type": "text"},
                    "keyqueries": {"type": "flattened"},
                    "keywords": {"type": "flattened"},
                    "abstract": {"type": "text"},
                    "fulltext": {"type": "text"}
                },
            },
        }
        print(f'Creating {self.INDEX_NAME} index...')
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

    def chunk_iterate_docs(self, page_size=10000, scroll_timeout="3m"):
        if page_size > 10000:
            page_size = 10000
        is_first = True
        while True:
            if is_first:
                result = self.es_client.search(index=self.INDEX_NAME, scroll=scroll_timeout,
                                               body={"size": page_size})
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
        bulk(self.es_client, requests, refresh=True)

    def createIndexAndIndexDocs_(self, path):
        self.create_index()
        self.index_data(self.readJSON_(path))

    def createIndexAndIndexDocs(self, path):
        self.create_index()
        self.index_data(self.readJSON(path))

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

        print(self.select_keyquerie(papers))
        #for paper in papers:
        #    print(paper["_source"].get("keyqueries", "duh"))

    def fill_documents(self, path):
        docs = self.readJSON(path)
        doi_id = {hit["_source"]["doi"]: hit["_id"] for hits in self.chunk_iterate_docs() for hit in hits}

        gen = ((doi_id[doc["doi"]], {
            "abstract": doc.get("abstract"),
            "fulltext": doc.get("fulltext"),
            "acmId": doc.get("acmId")
        }) for doc in docs if doc["doi"] in doi_id)

        self.chunk_update_field(gen)

    def update_keyqueries(self):
        chunk_size = 1000
        k = keyqueries.Keyqueries()
        for entries in self.chunk_iterate_docs(page_size=chunk_size):
            keywordss = {entry["_id"]: k.extract_keywords(entry) for entry in entries}
            self.chunk_update_field(((_id, {"keywords": keywords}) for (_id, keywords) in keywordss.items()),
                                    page_size=chunk_size)
            self.chunk_update_field(((entry["_id"], {"keyqueries": {" ".join(kws): score for (kws, score) in k.optimized(entry["_id"], keywordss[entry["_id"]])}}) for entry in entries),
                                    page_size=chunk_size)

    def chunk_update_field(self, gen, chunk_size=1000, page_size=None):
        gen_ = ({"_index": self.INDEX_NAME,
                 "_op_type": "update",
                 "_id": _id,
                 "doc": p(fields)} for (_id, fields) in itertools.islice(gen, chunk_size))

        if page_size:
            if page_size <= chunk_size:
                bulk(self.es_client, gen_)
        else:
            page_size = "unknown amount of"
            try:
                while True:
                    actions = [next(gen_)]
                    bulk(self.es_client, actions)
                    bulk(self.es_client, itertools.islice(gen_, chunk_size))
            except StopIteration:
                pass
        print(f"{page_size} documents successfully read")

    def debug_print(self):
        kqs = "\n".join([str(hit["_source"]) for hits in self.chunk_iterate_docs() for hit in hits])
        print(kqs)
        print(len(kqs))

    def extract_json(self, search_phrase, file_name=None):
        if not file_name:
            file_name = f"{search_phrase}.json"
        with open(file_name, "w") as file:
            file.write(json.dumps([hit["_source"] for hit in self.title_search(search_phrase, size=1000)["hits"]["hits"]]))


    def select_keyquerie(self, papers):
        alldic = []
        for paper in papers:
            alldic.append(paper["_source"].get("keyqueries"))
        allkeys = []
        for dic in alldic:
            for key in dic:
                allkeys.append(key)
        revindex = Counter(allkeys)
        # option 1
        print([k for k,v in revindex.items() if float(v) == revindex.most_common(1)[0][1]])

        candidates = [k for k,v in revindex.items() if float(v) >= len(papers)]
        if candidates:
            score = 0
            for temp in candidates:
                aver = 0
                for number in alldic:
                    aver += number[temp]
                maybe = aver / len(papers)
                if maybe > score:
                    score = maybe
                else:
                    candidates.remove(temp)
            return "Option 1: " + str(candidates)

        # option 2
        print(revindex.most_common(1))
        if revindex.most_common(1)[0][1] > 1:
            candidates = [k for k,v in revindex.items() if float(v) == revindex.most_common(1)[0][1]]
            unoccourrentpaper = {}
            for candidate in candidates:
                unoccourrentpaper[candidate] = [paper["_source"]["title"] for paper in papers if candidate not in paper["_source"].get("keyqueries")]
            score = 0
            query = ""
            for kq in unoccourrentpaper:
                temp = 0
                for pap in unoccourrentpaper[kq]:
                    query_body = {
                        "size": 100000,
                        "query": {
                            "multi_match": {
                                "query": kq,
                                "fields": ["title", "abstract"],
                                "operator": "and"
                            },
                        },
                    }
                    responses = self.es_client.search(body=query)
                    print(pap)
                    print([lel["_score"] for lel in responses["hits"]["hits"] if lel["_source"]["title"] == pap])
        else:
            return "Those paper are not compatible for the keyquerie search. Sorry."

        #if list(x.values()).__contains__(len(papers)):
        '''



        match = []
        for key in papers[0]["_source"].get("keyqueries"):
            bool = True
            for paper in papers:
                if key not in paper["_source"].get("keyqueries"):
                    bool = False

            if bool:
                match.append(key)

        if match:
            max = 0
            result = ""
            for canidate in match:
                score = 0
                for paper in papers:
                    score = paper["_source"].get("keyqueries")[canidate] + score
                score = score/len(papers)
                if score > max:
                    max = score
                    result = canidate

            return result

        #case 2
        if not match:
            matches = {}
            for paper in papers:
                for key in paper["_source"].get("keyqueries"):
                    ocurrence = 0
                    for temp in papers:
                        if temp["_source"].get("keyqueries").keys().contains(key):
                            ocurrence += 1
'''