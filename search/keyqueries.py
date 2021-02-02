import sys
import time
from math import ceil

from concurrent import futures as fs

from multiprocessing import Pool, Queue, Process

import textrank
from elasticsearch import Elasticsearch
import json


def parallel_kq(keywords, bitcode):
    es = Elasticsearch()

    querywords = tuple(keywords[index] for index in range(0, len(keywords)) if 1 << index & bitcode)

    query = {
        "size": 100,
        "query": {
            "multi_match": {
                "query": " ".join(querywords),
                "fields": [f"title^2", "abstract"],
                "operator": "and"
            }
        },
        # "sort": ["_doc"]
    }

    return querywords, es.search(index='paper', body=query)


class Keyqueries:
    def __init__(self):
        self.es = Elasticsearch()
        self.extractor = textrank.TextRank4Keyword()

        self.MAX_SEARCH = 10000
        self.MIN_RANK = 50
        self.TITLE_BOOST = 2
        self.INDEX_NAME = "paper"
        self.MAX_DEPTH = sys.maxsize

    def extract_keywords(self, seed):
        self.extractor.analyze(f'{seed["_source"].get("abstract", "")}\n{seed["_source"]["title"]}',
                               candidate_pos=['NOUN', 'PROPN'], window_size=4, lower=True)
        keywords = self.extractor.get_keywords(9)
        # print(keywords)
        return keywords

    def q(self, sentence, size=None, search_after=None):
        if size is None:
            size = self.MAX_SEARCH
        query = {
            "size": size,
            "query": {
                "multi_match": {
                    "query": sentence,
                    "fields": [f"title^{self.TITLE_BOOST}", "abstract"],
                    "operator": "and"
                }
            },
            # "sort": ["_doc"]
        }

        if search_after:
            query['search_after'] = search_after

        return self.es.search(index=self.INDEX_NAME, body=query)

    def legal_query(self, seed, query):

        response = self.q(query, self.MIN_RANK)

        for hit in response["hits"]["hits"]:
            if hit["_id"] == seed["_id"]:
                return True, hit["_score"]

        return False, -1

    def query_extraction(self, seed):
        keywords = self.extract_keywords(seed)
        for bitcode in range(0, 2 ** len(keywords)):
            querywords = tuple(keywords[index] for index in range(0, len(keywords)) if 1 << index & bitcode)
            found, score = self.legal_query(seed, " ".join(querywords))
            if found:
                yield querywords, score

    def full_keyquery(self, seed):
        return dict(self.query_extraction(seed))

    def parallel_kq(self, seed):
        keywords = self.extract_keywords(seed)
        responses = []
        with Pool(20) as pool:
            for bitcode in range(0, 2 ** len(keywords)):
                responses.append(pool.apply(func=parallel_kq, args=(keywords, bitcode,)))

        results = dict()

        for querywords, response in responses:
            for hit in response["hits"]["hits"]:
                if hit["_id"] == seed["_id"]:
                    results[querywords] = hit["_score"]
        return results

    def optimized(self, seed):
        keywords = self.extract_keywords(seed)
        querywordss = [tuple(keywords[index] for index in range(0, len(keywords)) if 1 << index & bitcode) for bitcode
                       in range(0, 2 ** len(keywords))]
        chunk_size = 2 ** len(keywords)  # this actually means all at once
        query_header = {"index": self.INDEX_NAME}

        for chunk in [querywordss[x:x + chunk_size] for x in range(0, len(querywordss), chunk_size)]:
            query = []
            for qws in chunk:
                query_body = {
                    "size": self.MIN_RANK,
                    "query": {
                        "multi_match": {
                            "query": " ".join(qws),
                            "fields": [f"title^{self.TITLE_BOOST}", "abstract"],
                            "operator": "and"
                        },
                    },
                }
                query.append(query_header)
                query.append(query_body)
            responses = self.es.msearch(body=query)["responses"]
            for qws, response in zip(chunk, responses):
                for hit in response["hits"]["hits"]:
                    if hit["_id"] == seed["_id"]:
                        yield qws, hit["_score"]
                        break


def main():
    bitcodes = range(0, 181)
    print(bitcodes)
    chunk_size = 10
    for chunk in [bitcodes[x:x + chunk_size] for x in range(0, len(bitcodes), chunk_size)]:
        for bitcode in chunk:
            print(bitcode)

    es = Elasticsearch()
    index_name = 'paper'
    hit1 = es.search(index=index_name, body={"query": {"match": {
        "title": "Incorporating Historical Test Case Performance Data and Resource Constraints into Test Case Prioritization."}}})[
        "hits"]["hits"][0]
    k = Keyqueries()

    startx = time.time()

    print(dict(k.optimized(hit1)))

    print(time.time() - startx)

    start152 = time.time()

    print(k.full_keyquery(hit1))

    print(time.time() - start152)


"""        
    hit2 = es.search(index=index_name,
                     body={"query": {"match": {"title": "The test data challenge for database-driven applications."}}})[
        "hits"]["hits"][0]
    hit3 = es.search(index=index_name,
                     body={"query": {"match": {"title": "Strong higher order mutation-based test data generation."}}})[
        "hits"]["hits"][0]
    hit4 = es.search(index=index_name,
                     body={"query": {"match": {"title": "Efficiently monitoring data-flow test coverage."}}})["hits"][
        "hits"][0]
    hit5 = es.search(index=index_name,
                     body={"query": {"match": {
                         "title": "Research of Survival-Time-Based Dynamic Adaptive Replica Allocation Algorithm in Mobile Ad Hoc Networks."}}})[
        "hits"][
        "hits"][0]

    hits = [hit1, hit2, hit3, hit4, hit5]

    k = Keyqueries()

    # print(k.smolpp(hit1))
    start0 = time.time()

    print(k.parallel_kq(hit1))
    # print(k.full_keyquery(hit1))

    print(time.time() - start0)




    start1 = time.time()

    with Pool() as pool:
        print(pool.map(full_keyquery, hits))

    print(f"v1 took {time.time() - start1}")

    start2 = time.time()

    for hit in hits:
        print(full_keyquery2(hit))

    print(f"v2 took {time.time() - start2}")
"""

if __name__ == '__main__':
    main()
