import sys
import time
from functools import reduce

import textrank
from elasticsearch import Elasticsearch


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
        source = seed["_source"]
        self.extractor.analyze(f'{source.get("abstract", "")}\n{source.get("title", "")}\n{source.get("fulltext", "")}',
                               candidate_pos=['NOUN', 'PROPN'], window_size=4, lower=True)
        keywords_dict = self.extractor.get_keywords(9)
        # print(keywords_dict)
        return keywords_dict

    def single_kq(self, _id, keywords):
        for qws, seed_scores in self.multi_kq([_id], keywords):
            yield qws, seed_scores[_id]

    def multi_kq(self, _ids, keywords):
        if not keywords:
            return
        if isinstance(keywords, dict):
            keywords = [*keywords]
        querywordss = [tuple(keywords[index] for index in range(0, len(keywords)) if 1 << index & bitcode) for bitcode
                       in range(0, 2 ** len(keywords))]
        # this actually means chunk_size = half of amount of possible keyqueries or 10000
        chunk_size = min(2 ** (len(keywords) - 1), 10000)
        query_header = {"index": self.INDEX_NAME}

        for chunk in [querywordss[x:x + chunk_size] for x in range(0, len(querywordss), chunk_size)]:
            query = []
            for qws in chunk:
                query_body = {
                    "size": self.MIN_RANK,
                    "query": {
                        "multi_match": {
                            "query": " ".join(qws),
                            "fields": [f"title^{self.TITLE_BOOST}", "abstract", "fulltext"],
                            "operator": "and"
                        },
                    },
                }
                query.append(query_header)
                query.append(query_body)
            responses = self.es.msearch(body=query, request_timeout=100)["responses"]
            for qws, response in zip(chunk, responses):
                seed_scores = dict()
                for hit in response["hits"]["hits"]:
                    if hit["_id"] in _ids:
                        seed_scores[hit["_id"]] = hit["_score"]
                        if len(seed_scores) == len(_ids):
                            break
                if seed_scores:
                    yield qws, seed_scores

    def best_kq(self, _ids, keywords):
        kqs = {sum(seed_scores.values()): (kq, seed_scores) for kq, seed_scores in self.multi_kq(_ids, keywords)}
        return kqs[max(kqs)]


def main():
    k = Keyqueries()
    print(list(k.single_kq(_id=None, keywords=[])))


"""    
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

    print(dict(k.single_kq(hit1["_id"], k.extract_keywords(hit1))))

    print(time.time() - startx)

    start152 = time.time()

    print(time.time() - start152)


    bitcodes = range(0, 181)
    print(bitcodes)
    chunk_size = 10
    for chunk in [bitcodes[x:x + chunk_size] for x in range(0, len(bitcodes), chunk_size)]:
        for bitcode in chunk:
            print(bitcode)

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
