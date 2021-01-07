import re
import sys
from contextlib import suppress

from elasticsearch import Elasticsearch
import json

INDEX_NAME = "paper"


def extract_keywords(seeds):
    keywords = set()
    for seed in seeds:
        keywords.update(re.sub(r'[^a-zA-Z ]', '', seed['title']).split())
    return keywords


def legal_query(es, min_, max_, query, seeds):
    response = es.search(index=INDEX_NAME, body=json.dumps(
        {"query": {"multi_match": {"query": query, "fields": ["title", "authors"]}}}))

    hits_n = response["hits"]["total"]["value"]

    if hits_n < min_:
        print('too few ({}) hits'.format(hits_n))
        return False
    if hits_n > max_:
        print('too many ({}) hits'.format(hits_n))
        return False

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


def query_loop(es, min_, max_, seeds):
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

    filtered = [keyword for keyword in keywords if legal_query(es, min_, max_, keyword, seeds)]

    return filtered


def read_json(path):
    jsondata = []
    for line in open(path, 'r', encoding='utf8'):
        jsondata.append(json.loads(line))

    return jsondata


def main():
    es = Elasticsearch()
    print(query_loop(es, 5, sys.maxsize, read_json('seed.json')))


if __name__ == '__main__':
    main()
