from collections import Counter
from math import ceil

import textrank
from elasticsearch import Elasticsearch


class Keyqueries:
    def __init__(self):

        self.es = Elasticsearch()
        self.extractor = textrank.TextRank4Keyword()

        self.INDEX_NAME = "paper"

    def extract_keywords_kqc(self, seeds, num_keywords=9, candidate_pos=('NOUN', 'PROPN')):
        kw_list = []
        for seed in seeds:
            seed["_source"]["keywords"] = self.extract_keywords(seed, num_keywords=num_keywords, candidate_pos=candidate_pos)
            kw_list.extend(seed["_source"]["keywords"].keys())
        common_kws = [k for (k, v) in Counter(kw_list).items() if v == len(seeds)]

        if len(common_kws) >= num_keywords:
            return common_kws[:num_keywords]
        else:
            n_per_seed = ceil((num_keywords - len(common_kws)) / len(seeds))
            for seed in seeds:
                d = [(k, v) for (k, v) in seed["_source"]["keywords"].items() if k not in common_kws]
                common_kws.extend([e[0] for e in sorted(d, key=lambda x: x[1], reverse=True)[:n_per_seed]])

        return common_kws

    def extract_keywords(self, seed, num_keywords=9, candidate_pos=('NOUN', 'PROPN'), analyzer=None):
        if not analyzer:
            analyzer = {"tokenizer": "standard", "filter": ["lowercase", "porter_stem"]}
        source = seed["_source"]
        plaintexts = f'{source.get("abstract", "")}\n{source.get("title", "")}\n{source.get("fulltext", "")}'
        body = analyzer.copy()
        body["text"] = plaintexts
        analyzed = self.es.indices.analyze(index=self.INDEX_NAME, body=body)
        stems = [token['token'] for token in analyzed["tokens"]]
        stemmed_plaintexts = " ".join(stems)
        self.extractor.analyze(stemmed_plaintexts, candidate_pos=candidate_pos, window_size=4, lower=True)
        keywords_dict = self.extractor.get_keywords(num_keywords)
        print(f"{source.get('title', '')}\n{keywords_dict.keys()}\n")
        # keywords_dict = dict((kw.lower(), i) for (i, kw) in enumerate(source.get("title").split()))
        # print(keywords_dict)
        return keywords_dict

    def single_kq(self, _id, keywords, min_rank=50):
        for qws, seed_scores in self.multi_kq([_id], keywords, min_rank=min_rank):
            # print(qws, seed_scores)
            yield qws, seed_scores[_id]

    def multi_kq(self, _ids, keywords, min_rank=50, title_boost=1, abstract_boost=1):
        if not keywords:
            return
        if isinstance(keywords, dict):
            keywords = [*keywords]
        # TODO replace list comprehension with generator in order to save RAM
        querywordss = [tuple(keywords[index] for index in range(0, len(keywords)) if 1 << index & bitcode) for bitcode
                       in range(0, 2 ** len(keywords))]
        # this actually means chunk_size = amount of possible keyqueries or 10000
        chunk_size = min(2 ** len(keywords), 10000)
        query_header = {"index": self.INDEX_NAME}

        # TODO when querywordss is replaced by a generator, use the correct slice operator here
        for chunk in [querywordss[x:x + chunk_size] for x in range(0, len(querywordss), chunk_size)]:
            query = []
            for qws in chunk:
                query_body = {
                    "size": min_rank,
                    "query": {
                        "multi_match": {
                            "query": " ".join(qws),
                            "fields": [f"title^{title_boost}", f"abstract^{abstract_boost}", "fulltext"],
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

    def best_kq(self, _ids, keywords, min_rank=50):
        kqs = list(self.multi_kq(_ids=_ids, keywords=keywords, min_rank=min_rank))
        kqs = sorted(kqs, key=lambda x: (len(x[1]), sum(x[1].values()),), reverse=True)
        if kqs:
            return kqs[0]


def main():
    k = Keyqueries()
    print(list(k.single_kq(_id=None, keywords=[])))


if __name__ == '__main__':
    main()
