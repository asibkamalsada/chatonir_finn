import itertools
import json
import math
import multiprocessing
from collections import Counter
from concurrent.futures.thread import ThreadPoolExecutor
from io import StringIO
from math import ceil
from statistics import mean
from threading import Thread

import PyPDF2
from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.helpers import bulk

import keyqueries


def print_return(param):
    # print(param)
    return param


def calc_kwss_kqss(entries):
    k = keyqueries.Keyqueries()
    old_size = len(entries)
    entries = [entry for entry in entries if entry["_source"].get("keyqueries", -1) == -1]
    if entries:
        print(f"first entry: {entries[0]}\ndid not read {len(entries)} out of {old_size} possible docs in chunk... ")
        kwss = dict()
        kqss = dict()
        for entry in entries:
            kws = k.extract_keywords(entry)
            kqs = {" ".join(qws): score for (qws, score) in k.single_kq(entry["_id"], kws)}
            kwss[entry["_id"]] = kws
            kqss[entry["_id"]] = kqs
        return kwss, kqss
        # kwss = {entry["_id"]: k.extract_keywords(entry) for entry in entries}
        # kqss = {entry["_id"]: create_kq_dict(entry) for entry in entries}
    else:
        print(f"already read chunk of {old_size}")


class Searchengine:

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

    def chunk_iterate_docs(self, page_size=10000, keep_alive="12h"):
        if page_size > 10000:
            page_size = 10000
        pit: dict = self.es_client.open_point_in_time(index=self.INDEX_NAME, keep_alive=keep_alive)

        query = {
            "size": page_size,
            "sort": [{"_doc": "asc"}],
            "pit": pit
        }

        response = self.es_client.search(body=query)

        while response["hits"]["hits"]:
            yield response["hits"]["hits"]
            query["search_after"] = response["hits"]["hits"][-1]["sort"]
            query["pit"]["id"] = response["pit_id"]
            response = self.es_client.search(body=query)

        self.es_client.close_point_in_time(body={"id": response["pit_id"]})

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
        return self.es_client.search(index=self.INDEX_NAME, body=search)

    def normal_search(self, query, size=10000):
        """ Searches the user query and finds the best matches using elasticsearch."""
        search = {
            "size": size,
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title", "abstract", "fulltext"]
                },
            },
        }
        return self.es_client.search(index=self.INDEX_NAME, body=search)

    def id_search(self, _id, size=10000):
        return self.es_client.get(index=self.INDEX_NAME, id=_id)

    def normal_search_exclude_ids(self, query, ids, size=10000):
        """ Searches the user query and finds the best matches using elasticsearch."""
        if not isinstance(ids, list):
            return None
        search = {
            "size": size,
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": ["title", "abstract", "fulltext"]
                            },
                        }
                    ],
                    "must_not": [
                        {
                            "ids": {
                                "values": ids
                            }
                        }
                    ]
                }
            }
        }
        return self.es_client.search(index=self.INDEX_NAME, body=search)

    def start(self):
        papers = []
        while True:
            query = input("Enter the paper you want to keyquerie: (empty input to cancel)")
            if not query:
                break

            response = self.title_search(query)

            if response["hits"]["total"]["value"] == 0:
                print("no documents found, specify another search string please")
                continue

            print("Please select the paper(s) you want to use like so ('1, 3, 10')")
            print('\n'.join(
                [f'{index} : {paper["_source"]["title"]}' for index, paper in enumerate(response["hits"]["hits"])]))

            while True:
                numbers = input()
                if not numbers:
                    break
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
        if papers:
            print(papers)
            print(self.select_keyquerie(papers))

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
        self.es_client.indices.refresh(index=self.INDEX_NAME)
        doc_count = self.es_client.indices.stats(index=self.INDEX_NAME, metric="docs")["indices"][self.INDEX_NAME]["total"]["docs"]["count"]
        cpu_count = ceil(multiprocessing.cpu_count()*2)
        chunk_size = min(max(ceil(doc_count / cpu_count), 1), 1000)
        # cpu_count = ceil(doc_count / chunk_size)

        # cpu_count = multiprocessing.cpu_count()
        # chunk_size = 10000

        exceptions = list()

        with ThreadPoolExecutor(max_workers=cpu_count) as executor:
            for kwss, kqss in executor.map(calc_kwss_kqss, self.chunk_iterate_docs(page_size=chunk_size)):
                try:
                    self.chunk_update_field(((_id, {"keywords": kws}) for (_id, kws) in kwss.items()), page_size=len(kwss))
                    '''kws_t = Thread(target=self.chunk_update_field, args=(
                        ((_id, {"keywords": kws}) for (_id, kws) in kwss.items()),
                    ), kwargs={"page_size": len(kwss)})
                    kws_t.start()
                    threads.append(kws_t)'''
                    # self.chunk_update_field(((_id, {"keywords": kws}) for (_id, kws) in kwss.items()),
                    #                         page_size=len(kwss))
                    # every "_id" has a field called "keyqueries" which contains a dict consisting of a space separated
                    # concatenation of the keywords of the keyquery and the score of the respective "_id" regarding
                    # this particular keyquery
                    self.chunk_update_field(((_id, {"keyqueries": kqs}) for (_id, kqs) in kqss.items()), page_size=len(kqss))
                    '''kqs_t = Thread(target=self.chunk_update_field, args=(
                        ((_id, {"keyqueries": kqs}) for (_id, kqs) in kqss.items()),
                    ), kwargs={"page_size": len(kqss)})
                    kqs_t.start()
                    threads.append(kqs_t)'''
                    # self.chunk_update_field(((_id, {"keyqueries": kqs}) for (_id, kqs) in kqss.items()),
                    #                         page_size=len(kqss))
                    print(f"done {len(kqss)}")
                except ElasticsearchException:
                    exceptions.append(ElasticsearchException)
                    print("exception occured")

        with open("exceptions.txt", "w") as file:
            file.writelines(str(e) for e in exceptions)

    def chunk_update_field(self, gen, chunk_size=1000, page_size=None):
        """

        :param gen: iterable yielding at most page_size update lines
        :param chunk_size: batching the update process via bulk(client, update) in steps of size chunk_size
        :param page_size: how many updates are retrievable by gen
        :return: None
        """
        gen_ = ({"_index": self.INDEX_NAME,
                 "_op_type": "update",
                 "_id": _id,
                 "doc": print_return(fields)} for (_id, fields) in itertools.islice(gen, chunk_size))

        if page_size and page_size <= chunk_size:
            bulk(self.es_client, gen_)
        else:
            try:
                while True:
                    actions = [next(gen_)]
                    bulk(self.es_client, actions)
                    bulk(self.es_client, itertools.islice(gen_, chunk_size))
            except StopIteration:
                pass
        print(f"{page_size} documents successfully read")
        # self.es_client.indices.refresh(index=self.INDEX_NAME)

    def debug_print(self):
        print("hits with kq:", len(list(hit for hits in self.chunk_iterate_docs() for hit in hits if hit["_source"].get("keyqueries"))))

    def extract_noise(self, size=5000):
        query = {
            "size": size,
            "query": {
                "function_score": {
                    "query": {"match_all": {}},
                    "random_score": {}
                }
            }
        }
        content = json.dumps([hit["_source"] for hit in self.es_client.search(body=query, index=self.INDEX_NAME)["hits"]["hits"]])
        with open("json/noise.json", "w") as file:
            file.write(content)

    def extract_json(self, search_phrase, file_name=None):
        if not file_name:
            file_name = f"{search_phrase}.json"
        with open(file_name, "w") as file:
            file.write(
                json.dumps([hit["_source"] for hit in self.title_search(search_phrase, size=1000)["hits"]["hits"]]))

    def select_keyquerie(self, papers):
        alldic = [paper["_source"].get("keyqueries") for paper in papers if paper["_source"].get("keyqueries")]
        allkeys = []
        for dic in alldic:
            for key in dic:
                allkeys.append(key)

        revindex = Counter(allkeys)

        candidates = [k for k, v in revindex.items() if float(v) >= len(papers)]
        selected = ""
        if candidates:
            print("\n---------------------- Option 1 ------------------------")
            score = 0
            for temp in candidates:
                aver = 0
                for number in alldic:
                    aver += number[temp]
                maybe = aver / len(papers)
                if maybe > score:
                    score = maybe
                    selected = temp
                else:
                    candidates.remove(temp)
            return selected

        print("\n---------------------- Option 2 ------------------------")
        solutions = self.select_keyqueries(papers)
        keywords = []
        max_keywords = list(frozenset.union(*solutions.keys()))
        ids = list(set.union(*solutions.values()))
        k = keyqueries.Keyqueries()
        keyout = []

        if len(max_keywords) < 10:
            output = k.best_kq(_ids=ids, keywords=max_keywords)
            return output

        max_anz = sum(len(v) for v in solutions.values())
        for solution in solutions.keys():
            x = math.floor((len(solutions[solution])/max_anz) * 9)
            for id in solutions[solution]:
                keywords.append(self.id_search(id)["_source"].get("keywords"))
            sorted_merge = {k: v for k, v in sorted({k: v for d in keywords for k, v in d.items()}.items(), key=lambda item: item[1], reverse=True)}
            keyout.append(list(sorted_merge.keys())[:x])
            keywords.clear()
        output = k.best_kq(_ids=ids, keywords=[j for i in keyout for j in i])
        return output

    '''
        if revindex.most_common(1)[0][1] > 1:
            candidates = [k for k, v in revindex.items() if float(v) == revindex.most_common(1)[0][1]]
            unoccourrentpaper = {}
            for candidate in candidates:
                unoccourrentpaper[candidate] = [paper["_source"]["title"] for paper in papers if
                                                candidate not in paper["_source"].get("keyqueries")]
            score = 0
            query = ""
            for kq in unoccourrentpaper:
                temp = 0
                for pap in unoccourrentpaper[kq]:
                    query_body = {
                        "size": 10000,
                        "query": {
                            "multi_match": {
                                "query": kq,
                                "fields": ["title", "abstract"],
                                "operator": "and"
                            },
                        },
                    }
                    responses = self.es_client.search(body=query_body)
                    # print(pap)
                    # print([lel["_score"] for lel in responses["hits"]["hits"] if lel["_source"]["title"] == pap])
                    print("--------------------------------")
                    return revindex.most_common(1)[0][0]
        else:
            return "Those paper are not compatible for the keyquerie search. Sorry."
    '''

    def select_keyqueries(self, docs_p):
        docs = []
        for doc_p in docs_p:
            doc = dict()
            doc["_id"] = doc_p["_id"]
            try:
                doc["keyqueries"] = {frozenset(kq_str.split()): score
                                     for kq_str, score in doc_p["_source"]["keyqueries"].items()}
                docs.append(doc)
            except KeyError:
                pass

        id_src = {doc["_id"]: doc["keyqueries"] for doc in docs}

        kqs = set(kq for doc in docs for kq, score in doc["keyqueries"].items())

        ms = {kq: set(doc["_id"] for doc in docs if kq in doc["keyqueries"]) for kq in kqs}

        def kq_sort(kq__doc_ids):
            """
            :param kq__doc_ids: tuple in ms.items(), 1st is a keyquery and 2nd is the set of ids of the corresponding docs
            :return: a tuple, 1st is the amount of corresponding docs and second the avg of the scores of the kq across the docs
            """
            kq = kq__doc_ids[0]
            doc_ids = kq__doc_ids[1]
            return len(doc_ids), mean(id_src[_id].get(kq) for _id in doc_ids)

        ms = dict(sorted(ms.items(), key=kq_sort, reverse=True))

        return self.greedy(ms, set(id_src.keys()))

    def greedy(self, ms, _ids):
        found_docs = set()
        solution = dict()
        for kq, kq_docs in ms.items():
            if not kq_docs.issubset(found_docs):
                solution[kq] = kq_docs
                found_docs.update(kq_docs)
                if found_docs == _ids:
                    break
        return solution

    def full_text_search(self):
        pass
