import re
import sys
from contextlib import suppress

from elasticsearch import Elasticsearch
import json

from elasticsearch.helpers import scan

INDEX_NAME = "paper"
MAX_SEARCH = 10000


def extract_keywords(seeds):
    keywords = set()
    for seed in seeds:
        title_words = re.sub(r'[^a-zA-Z ]', '', seed['title']).lower().split()
        title_words2 = ['{} {}'.format(x, y) for x, y in zip(title_words[::2], title_words[1::2])]
        keywords.update(title_words)
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


def legal_keyword(es, min_, query, seeds):
    left_seeds = list(seeds)

    response = q(es, query)
    hits_n = response["hits"]["total"]["value"]
    if hits_n < min_:
        print('too few ({}) hits'.format(hits_n))
        return False

    while response["hits"]["hits"]:
        left_seeds = [seed for seed in left_seeds if seed not in [x["_source"] for x in response["hits"]["hits"]]]
        search_after = response["hits"]["hits"][-1]["sort"]
        response = q(es, query, search_after=search_after)

#    if not all(item in [hit["_source"] for hit in response["hits"]["hits"]] for item in seeds):
#        return False

    return len(left_seeds) == 0


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

    filtered = [keyword for keyword in keywords if legal_keyword(es, min_, keyword, seeds)]

    print(len(filtered))
    for i in range(len(filtered)):
        print(filtered[i])

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
