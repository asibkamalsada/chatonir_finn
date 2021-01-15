import random
import re
import sys
from contextlib import suppress
import textrank

from elasticsearch import Elasticsearch
import json

from elasticsearch.helpers import scan

INDEX_NAME = "paper"
MAX_SEARCH = 10000


def extract_keywords(seeds):
    extractor = textrank.TextRank4Keyword()
    extractor.analyze(' '.join([s["_source"]["title"] for s in seeds]), candidate_pos=['NOUN', 'PROPN'], window_size=4, lower=False)
    keywords = extractor.get_keywords(10)
    return keywords


def q(es, sentence, search_after=None):
    query = {
        "size": MAX_SEARCH,
        "query": {
            "match": {
                "title": {
                    "query": sentence,
                    "operator": "and"
                }
            }
        },
        "sort": ["_doc"]
    }

    if search_after:
        query['search_after'] = search_after

    return es.search(index=INDEX_NAME, body=query)


def legal_query(es, seeds, query, min_, max_=sys.maxsize):
    left_seeds = set([seed["_id"] for seed in seeds])

    response = q(es, query)
    hits_n = response["hits"]["total"]["value"]
    too_many = hits_n > max_

    if hits_n < min_:
        #print('too few ({}) hits'.format(hits_n))
        return [], False, too_many

    ids = []

    while response["hits"]["hits"]:
        ids.extend([hit["_id"] for hit in response["hits"]["hits"]])
        # left_seeds -= set([hit["_id"] for hit in response["hits"]["hits"]])
        # left_seeds = [seed for seed in left_seeds if seed not in [x["_source"] for x in response["hits"]["hits"]]]
        search_after = response["hits"]["hits"][-1]["sort"]
        response = q(es, query, search_after=search_after)

    return ids, left_seeds.issubset(ids), too_many

    #    if not all(item in [hit["_source"] for hit in response["hits"]["hits"]] for item in seeds):
    #        return False

    # return len(left_seeds) == 0, too_many


"""
    left_seeds = list(seeds)

    print("{} total hits.".format(response["hits"]["total"]["value"]))
    for hit in response["hits"]["hits"]:
        print("id: {}, score: {}".format(hit["_id"], hit["_score"]))
        print(hit["_source"])
        print()
        # hit["_source"] gives a dict

        with suppress(ValueError):
            left_seeds.remove(hit["_source"])
    return len(left_seeds) == 0
"""


def query_extraction(es, seeds, min_, max_):
    """

    :type es: Elasticsearch
    :type min_: int
    :type max_: int
    :type seeds: list
    """
    if not seeds:
        print('please give seeds')
        return seeds
    if min_ > max_:
        print('no proper range of number of expected results')
        return seeds
    if min_ <= len(seeds):
        print('trivial search')
        # return seeds
    keywords = extract_keywords(seeds)

    filtered = [keyword for keyword in keywords if legal_query(es, seeds, keyword, min_)[1]]

    yield from query_loop(es, seeds, set(filtered), set(), min_, max_, set())


"""print(len(filtered))
    for i in range(len(filtered)):
        print(filtered[i])

    return filtered"""


def query_loop(es, seeds, keywords, querywords, min_, max_, results):
    for next_ in keywords:
        keywords.remove(next_)
        querywords.add(next_)
        #print(querywords)
        if querywords not in results:
            ids, legal, too_many = legal_query(es, seeds, ' '.join(querywords), min_, max_)
            if legal and too_many:
                yield from query_loop(es, seeds, keywords, querywords, min_, max_, results)
            if legal and not too_many:
                result = frozenset(querywords)
                results.add(result)
                yield result, ids
        keywords.add(next_)
        querywords.remove(next_)


def read_json(path):
    jsondata = []
    for line in open(path, 'r', encoding='utf8'):
        jsondata.append(json.loads(line))

    return jsondata


def start(es, seed):
    count = 0
    result = []
    for keyquery, ids in query_extraction(es, seed, 2, 50):
        result.append(keyquery)
        count += 1
        print(str(keyquery) + " " + str(ids))

    return result


def main():
    es = Elasticsearch()
    hit1 = es.search(index=INDEX_NAME, body={"query": {"match": {
        "title": "Incorporating Historical Test Case Performance Data and Resource Constraints into Test Case Prioritization."}}})[
        "hits"]["hits"][0]
    hit2 = es.search(index=INDEX_NAME,
                     body={"query": {"match": {"title": "The test data challenge for database-driven applications."}}})[
        "hits"]["hits"][0]
    hit3 = es.search(index=INDEX_NAME,
                     body={"query": {"match": {"title": "Strong higher order mutation-based test data generation."}}})[
        "hits"]["hits"][0]
    hit4 = es.search(index=INDEX_NAME,
                     body={"query": {"match": {"title": "Efficiently monitoring data-flow test coverage."}}})["hits"][
        "hits"][0]
    start(es, [hit1])


if __name__ == '__main__':
    main()
