import sys

import textrank
from elasticsearch import Elasticsearch
import json


INDEX_NAME = "paper"
MAX_SEARCH = 10000
MAX_DEPTH = sys.maxsize


def extract_keywords(seed):
    extractor = textrank.TextRank4Keyword()
    extractor.analyze(seed["_source"]["title"], candidate_pos=['NOUN', 'PROPN'], window_size=4, lower=True)
    keywords = extractor.get_keywords(9)
    print(keywords)
    return keywords


def q(es, sentence, size=MAX_SEARCH, search_after=None):
    query = {
        "size": size,
        "query": {
            "match": {
                "title": {
                    "query": sentence,
                    "operator": "and"
                }
            }
        },
        # "sort": ["_doc"]
    }

    if search_after:
        query['search_after'] = search_after

    return es.search(index=INDEX_NAME, body=query)


def legal_query(es, seed, query):
    size = MAX_SEARCH

    response = q(es, query, size)

    found = False

    ids = [hit['_id'] for hit in response["hits"]["hits"]]
    try:
        first = ids.index(seed['_id']) == 0
        found = True
    except ValueError:
        first = False
        while response["hits"]["total"]["value"] == size:
            response = q(es, query, size, response["hits"]["hits"][-1]["sort"])
            ids = [hit['_id'] for hit in response["hits"]["hits"]]
            if seed['_id'] in ids:
                found = True
                break

    return found, first


def query_extraction(es, seed):
    yield from query_loop(es, seed, set(extract_keywords(seed)), querywords=set(), results=set(), depth=0)


def query_loop(es, seed, keywords, querywords, results, depth):
    if depth < MAX_DEPTH:
        for next_ in keywords:
            keywords.remove(next_)
            querywords.add(next_)
            if querywords not in results:
                found, first = legal_query(es, seed, ' '.join(querywords))
                if first:
                    result = frozenset(querywords)
                    results.add(result)
                    yield result
                if found:
                    yield from query_loop(es, seed, keywords, querywords, results, depth + 1)
            keywords.add(next_)
            querywords.remove(next_)


def read_json(path):
    jsondata = []
    for line in open(path, 'r', encoding='utf8'):
        jsondata.append(json.loads(line))

    return jsondata


def full_keyquery(es, seed):
    return [keyquery for keyquery in query_extraction(es, seed)]


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
    hit4 = es.search(index=INDEX_NAME,
                     body={"query": {"match": {"title": "Research of Survival-Time-Based Dynamic Adaptive Replica Allocation Algorithm in Mobile Ad Hoc Networks."}}})["hits"][
        "hits"][0]
    print(full_keyquery(es, hit4))


if __name__ == '__main__':
    main()
