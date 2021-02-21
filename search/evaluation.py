import glob
import json
import os
import sys
from collections import Counter
from datetime import datetime

import searchengine
import time
from sklearn.metrics import ndcg_score, dcg_score
import numpy as np
import pandas as pd
import itertools


def evaluate(new_index=False, num_keywords=9, min_rank=50, buchstabe=None, k=10, candidate_pos=('NOUN', 'PROPN'), bsln_b=False):
    queryinputs = {tuple(["Halo: a technique for visualizing off-screen objects", "Wedge: clutter-free visualization of off-screen locations"]) : ["Visualizing references to off-screen content on mobile devices: A comparison of Arrows, Wedge, and Overview+Detail","Visualizing locations of off-screen objects on mobile devices: a comparative evaluation of three approaches"],
                   tuple(["Towards optimum query segmentation: in doubt without", "Query segmentation based on eigenspace similarity", "Unsupervised query segmentation using click data: preliminary results"]): ["An IR-based evaluation framework for web search query segmentation","Query segmentation using conditional random fields."],
                   tuple(["On the effects of dimensionality reduction on high dimensional similarity search"]) : ["Automatic subspace clustering of high dimensional data for data mining applications","Can shared-neighbor distances defeat the curse of dimensionality?", "Density-based indexing for approximate nearest-neighbor queries"],
                   tuple(["Staying informed: supervised and semi-supervised multi-view topical analysis of ideological perspective", "Summarizing contrastive viewpoints in opinionated text"]): ["Which side are you on?: identifying perspectives at the document and sentence levels", "Vocabulary choice as an indicator of perspective", "Modeling perspective using adaptor grammars","Mining contrastive opinions on political texts using cross-perspective topic model"],
                   tuple(["Hexastore: sextuple indexing for semantic web data management", "Binary RDF for scalable publishing, exchanging and consumption in the web of data"]): ["A complete translation from SPARQL into efficient SQL"],
                   tuple(["Dynamic external hashing: the limit of buffering.", "Linear hashing with separators—a dynamic hashing scheme achieving one-access", "External-memory algorithms and data structures"]): ["External hashing with limited internal storage", "File organization using composite perfect hashing"],
                   tuple(["Scatter/Gather: a cluster-based approach to browsing large document collections", "Reexamining the cluster hypothesis: scatter/gather on retrieval results"]): ["A survey of Web clustering engines", "Beyond precision@10: clustering the long tail of web search results", "Essential Pages.", "A hierarchical monothetic document clustering algorithm for summarization and browsing search results", "Topical query decomposition"],
                   tuple(["Enhancing cluster labeling using wikipedia", "Topical clustering of search results."]): ["Frequent term-based text clustering", "Topical query decomposition"],
                   tuple(["A source independent framework for research paper recommendation.", "SOFIA SEARCH: a tool for automating related-work search"]): ["Recommending citations: translating papers into references", "Context-aware citation recommendation"],
                   tuple(["Model-driven formative evaluation of exploratory search: A study under a sensemaking framework Share on", "Exploratory search: from finding to understanding"]): ["Model-driven formative evaluation of exploratory search: A study under a sensemaking framework", "Exploratory search: from finding to understanding"]}

    newinputs = {1: ['Visualizing Locations of Off-screen Objects on Mobile Devices: A Comparative Evaluation of Three Approaches',
                     'City Lights: Contextual Views in Minimal Space'],  # x
                 2: ['An Adaptive and Dynamic Dimensionality Reduction Method for High-dimensional Indexing',
                     'The IGrid index: reversing the dimensionality curse for similarity indexing in high dimensional space'],
                 3: ['Generating comparative summaries of contradictory opinions in text',
                     'Mining Contrastive Opinions on Political Texts Using Cross-perspective Topic Model'],  # x
                 4: ['RDF-3X: A RISC-style Engine for RDF',
                     'SW-Store: A Vertically Partitioned DBMS for Semantic Web Data Management'],
                 5: ['Cache-oblivious hashing', 'External Hashing with Limited Internal Storage'],
                 6: ['A Survey of Web Clustering Engines',
                     'A Comprehensive Comparison Study of Document Clustering for a Biomedical Digital Library MEDLINE'],
                 7: ['Web Search Clustering and Labeling with Hidden Topics',
                     'A Search Result Clustering Method Using Informatively Named Entities'],
                 8: ['Automatically Building Research Reading ',
                     'Recommending Academic Papers via Users\' Reading Purposes'],
                 9: ['Search-logger Analyzing Exploratory Search Tasks',
                     'Collaborative Multi-paradigm Exploratory Search'],
                 10: ['Unsupervised query segmentation using generative language models and wikipedia',
                      'Two-stage query segmentation for information retrieval'],
                 11: ['Search-logger Analyzing Exploratory Search Tasks',
                      'Clustering Versus Faceted Categories for Information Exploration'],
                 12: ['A unified and discriminative model for query refinement',
                      'Exploring web scale language models for search query processing']
                 }

    newinputs2 = {1: ['Visualization of Off-screen Objects in Mobile Augmented Reality',
                      'EdgeSplit: Facilitating the Selection of Off-screen Objects'],
                  2: ['An Adaptive and Dynamic Dimensionality Reduction Method for High-dimensional Indexing',
                      'Dimensionality reduction and similarity computation by inner product approximations'],
                  3: ['Mining Contrastive Opinions on Political Texts Using Cross-perspective Topic Model',
                      'Mining contentions from discussions and debates'],
                  4: ['RDF-3X: A RISC-style Engine for RDF', 'The RDF-3X Engine for Scalable Management of RDF Data'],
                  5: ['File Organization Using Composite Perfect Hashing',
                      'External Hashing with Limited Internal Storage'],
                  6: ['Exploring the cluster hypothesis, and cluster-based retrieval, over the web',
                      'The cluster hypothesis revisited'],
                  7: ['Web Search Clustering and Labeling with Hidden Topics',
                      'A Search Result Clustering Method Using Informatively Named Entities'],
                  8: ['Beyond keyword search: discovering relevant scientific literature ',
                      'Towards an effective and unbiased ranking of scientific literature through mutual reinforcement'],
                  9: ['Search-logger Analyzing Exploratory Search Tasks',
                      'Collaborative Multi-paradigm Exploratory Search'],
                  10: ['The power of naive query segmentation', 'Unsupervised query segmentation using only query logs'],
                  11: ['Search-logger Analyzing Exploratory Search Tasks',
                       'Clustering Versus Faceted Categories for Information Exploration'],
                  12: ['Unsupervised query segmentation using clickthrough for information retrieval',
                       'Mining query structure from click data: a case study of product queries']
                  }

    se = searchengine.Searchengine()
    if new_index:
        se.create_index()
        se.index_data(se.readJSON("json/data.json"))
        # se.index_data(se.readJSON("json/testdata.json"))
        se.index_data(se.readJSON('json/noise9998.json'))
        # se.fill_documents('json/abstracts.json')
        # se.fill_documents('json/fulltexts.json')
    read_kq = se.update_keyqueries_without_noise(newinputs, num_keywords=num_keywords, min_rank=min_rank, candidate_pos=candidate_pos)
    if read_kq:
        se.es_client.indices.refresh(se.INDEX_NAME)
        ev_json = newtest(newinputs, se, num_keywords=num_keywords, min_rank=min_rank, k=k)
        if bsln_b:
            b_ndcg, b_prec, b_rec = baseline(newinputs, se, k=k)
            ev_json["baseline"] = {"avg_ndcg": b_ndcg, "avg_precision": b_prec, "avg_recall": b_rec}
        return ev_json


def baseline(newinputs, se, k=10):
    print("----------------- Baseline Test --------------------")
    with open('json/evaluation.csv') as csvDataFile:
        evaluationcsv = pd.read_csv(csvDataFile, delimiter=',')
    papers = []
    testcase = 0
    counter = 0
    goodhitcounter = 0
    miss = 0
    nDCG_scores = []
    recall = []
    pressision = []
    ids = []
    del evaluationcsv['Unnamed: 0']
    evaluationcsv.columns = [str('topicid'), str('title'), str('ranking'), str('acmId')]

    for i in newinputs:
        rel_score = []
        for j in newinputs[i]:
            response = se.title_search(j)
            ids.append(response["hits"]["hits"][0]["_id"])
            papers.append(response["hits"]["hits"][0])
        score = 0
        score_this = []
        for title in papers:
            score_this.append(se.normal_search_exclude_ids(title["_source"]["title"], ids=ids, size=k)["hits"]["hits"])
        thislist = list(itertools.chain.from_iterable(score_this))
        thislist.sort(key=lambda x: x["_score"], reverse=True)
        for item in thislist:
            for seconditem in thislist:
                if not item == seconditem:
                    if item["_id"] == seconditem["_id"]:
                        thislist.remove(item)

        df = evaluationcsv[(evaluationcsv['topicid'] == int(i))]
        for hit in thislist[:k]:
            found = False
            for row in df['title']:
                if hit["_source"]["title"] == row:
                    counter += 1
                    score += hit["_score"]
                    lel = df[df['title'] == row]['ranking']
                    if int(lel) > 0:
                        goodhitcounter += 1
                    rel_score.append(int(lel))
                    found = True
                    break
            if not found:
                rel_score.append(0)
        true_rel = sorted(rel_score, reverse=True)
        if goodhitcounter == 0:
            ndcg_score = 0
        else:
            ndcg_score = dcg_score(np.asarray([true_rel]), np.asarray([rel_score])) / dcg_score(np.asarray([true_rel]),
                                                                                                np.asarray([true_rel]))
        nDCG_scores.append(ndcg_score)
        testcase += 1
        if counter == 0:
            miss += 1
        df = df[df['ranking'] > 0]
        for j in newinputs[i]:
            df = df[df['title'] != j]

        temprecall = len([x for x in rel_score if x > 0]) / len(df)
        temppressision = len([x for x in rel_score if x > 0]) / len(rel_score)
        print("For topicid:" + str(i) + " the score of found paper is: " + str(score))
        print(f"Pressision@{k}: " + str(temppressision))
        print(f"Recall@{k}: " + str(temprecall))
        print(f"The nDCG@{k} score for this search is " + str(ndcg_score) + ".")
        print("")
        counter = 0
        goodhitcounter = 0
        recall.append(temprecall)
        pressision.append(temppressision)

        papers.clear()

    avg_ndcg = sum(nDCG_scores) / len(nDCG_scores)
    avg_precision = sum(pressision) / len(pressision)
    avg_recall = sum(recall) / len(recall)

    print(f"\nThe average nDCG@{k} score is " + str(avg_ndcg) + ".")
    print(f"The average precision@{k} score is " + str(avg_precision) + ".")
    print(f"The average recall@{k} score is " + str(avg_recall) + ".")

    return avg_ndcg, avg_precision, avg_recall


def newtest(newinputs, se, **kwargs):
    print("----------------- Test with \"new\" data --------------------")
    ev_json = {"topics": dict(), "stats": dict(kwargs)}
    with open('json/evaluation.csv') as csvDataFile:
        evaluationcsv = pd.read_csv(csvDataFile, delimiter=',')
    papers = []
    testcase = 0
    counter = 0
    goodhitcounter = 0
    miss = 0
    nDCG_scores = []
    recall = []
    precision = []
    ids = []
    del evaluationcsv['Unnamed: 0']
    evaluationcsv.columns = [str('topicid'), str('title'), str('ranking'), str('acmId')]

    for i in newinputs:
        rel_score = []
        for j in newinputs[i]:
            response = se.title_search(j)
            ids.append(response["hits"]["hits"][0]["_id"])
            papers.append(response["hits"]["hits"][0])
        kq, option = se.select_keyquerie(papers, final_kws=kwargs["num_keywords"])
        if kq:
            if isinstance(kq, tuple):
                score_this = se.normal_search_exclude_ids(" ".join(kq[0]), ids=ids, size=kwargs["k"])["hits"]["hits"]
            else:
                score_this = se.normal_search_exclude_ids(kq, ids=ids, size=kwargs["k"])["hits"]["hits"]
        else:
            option = 4
            score_this = se.option4(papers)
            # recall.append(0)
            # precision.append(0)
            # nDCG_scores.append(0)
            # continue
        score = 0

        df = evaluationcsv[(evaluationcsv['topicid'] == i)]
        for hit in score_this[:kwargs["k"]]:
            found = False
            for row in df['title']:
                if hit["_source"]["title"] == row:
                    counter += 1
                    score += hit["_score"]
                    lel = df[df['title'] == row]['ranking'].head(1)
                    if int(lel) > 0:
                        goodhitcounter += 1
                    rel_score.append(int(lel))
                    found = True
                    break
            if not found:
                rel_score.append(0)
        true_rel = sorted(rel_score, reverse=True)
        if goodhitcounter == 0:
            ndcg_score = 0
        else:
            ndcg_score = dcg_score(np.asarray([true_rel]), np.asarray([rel_score])) / dcg_score(np.asarray([true_rel]), np.asarray([true_rel]))
        nDCG_scores.append(ndcg_score)
        testcase += 1
        if counter == 0:
            miss += 1
        df = df[df['ranking'] > 0]
        for j in newinputs[i]:
            df = df[df['title'] != j]

        recall_t = len([x for x in rel_score if x > 0]) / len(df)
        precision_t = len([x for x in rel_score if x > 0])/len(rel_score)
        print("For topicid:" + str(i) + " the score of found paper is: " + str(score))
        print(f"The nDCG@{kwargs['k']} score for this search is " + str(ndcg_score) + ".")
        print(f"Precision@{kwargs['k']}: " + str(precision_t))
        print(f"Recall@{kwargs['k']}: " + str(recall_t))
        counter = 0
        goodhitcounter = 0
        recall.append(recall_t)
        precision.append(precision_t)

        topic = {"option": option, "score": score, "precision": precision_t, "recall": recall_t, "ndcg": ndcg_score}
        ev_json["topics"][i] = topic

        papers.clear()

    ev_json["stats"]["avg_ndcg"] = sum(nDCG_scores)/len(nDCG_scores)
    ev_json["stats"]["avg_precision"] = sum(precision)/len(precision)
    ev_json["stats"]["avg_recall"] = sum(recall)/len(recall)
    ev_json["stats"]["options_count"] = Counter((topic["option"] for (topic_n, topic) in ev_json["topics"].items()))
    ev_json["stats"]["k"] = kwargs["k"]


    print(f"\nThe average nDCG@{kwargs['k']} score is {ev_json['stats']['avg_ndcg']}.")
    print(f"The average precision@{kwargs['k']} score is {ev_json['stats']['avg_precision']}.")
    print(f"The average recall@{kwargs['k']} score is {ev_json['stats']['avg_recall']}.")

    return ev_json


def oldtest(queryinputs, se):
    print("----------------- Test with \"old\" data --------------------")
    papers = []
    testcase = 0
    counter = 0
    miss = 0
    perfect = 0
    nDCG_scores = []
    ids = []
    for i in queryinputs:
        rel_score = []
        for j in i:
            response = se.title_search(j)
            ids.append(response["hits"]["hits"][0]["_id"])
            papers.append(response["hits"]["hits"][0])
        kq, _ = se.select_keyquerie(papers)
        score = 0
        if isinstance(kq, tuple):
            score_this = se.normal_search_exclude_ids(" ".join(kq[0]), ids=ids, size=10)
        else:
            score_this = se.normal_search_exclude_ids(kq, ids=ids, size=10)
        for hit in score_this["hits"]["hits"]:
            for x in queryinputs[tuple(i)]:
                if hit["_source"]["title"] == x:
                    counter += 1
                    score += hit["_score"]
                    rel_score.append(1)
                    break
                rel_score.append(0)
        true_rel = sorted(rel_score, reverse=True)
        if counter == 0:
            ndcg_score = 0
        else:
            ndcg_score = dcg_score(np.asarray([true_rel]), np.asarray([rel_score])) / dcg_score(np.asarray([true_rel]), np.asarray([true_rel]))
        nDCG_scores.append(ndcg_score)
        testcase += 1
        if counter == 0:
            miss += 1
        if counter == len(queryinputs[tuple(i)]):
            perfect += 1
        print("For testcase:"+str(testcase)+" -" + i[0] + ", [...]-  the score of found paper is: " + str(score))
        print(str(counter) + " out of " + str(len(queryinputs[tuple(i)])) + " was/where found.")
        print("The nDCG@10 score for this search is " + str(ndcg_score) + ".")
        counter = 0

        papers.clear()
    print("\n""We did not find any paper in " + str(miss) + " out of " + str(len(queryinputs)) + " cases!")
    print("\n""Every expected paper was found in " + str(perfect) + " out of " + str(len(queryinputs)) + " cases!")
    print("\nThe average nDCG@10 score is " + str(sum(nDCG_scores)/len(nDCG_scores)) + ".")


def start(**kwargs):
    filepath = f"evaluation/{'_'.join(str(v) for k, v in kwargs.items() if k not in ('new_index', 'bsln_b'))}"
    if not glob.glob(filepath.replace(kwargs['buchstabe'], '*')):
        print(f"{filepath.replace(kwargs['buchstabe'], '*')} does not exist")
        ev_json = evaluate(**kwargs)
        if ev_json:
            with open(filepath, "w") as fp:
                json.dump(obj=ev_json, fp=fp)
            print(str(ev_json))
    else:
        print(f"{filepath.replace(kwargs['buchstabe'], '*')} exists already")


def full_eval():
    buchstabe = datetime.today().strftime('%Y%m%d')
    new_index = True
    k = 10

    num_keywordss = (5, 9, 11)
    min_ranks = (10, 50, 100, 10000)
    candidate_poss = (('NOUN', 'PROPN'), ('NOUN', 'PROPN', 'ADJ', 'VERB'))

    params = {"buchstabe": buchstabe, "new_index": new_index, "k": k}
    for num_keywords in num_keywordss:
        params["num_keywords"] = num_keywords
        for min_rank in min_ranks:
            params["min_rank"] = min_rank
            for candidate_pos in candidate_poss:
                params["candidate_pos"] = candidate_pos
                try:
                    start(**params)
                except Exception as e:
                    print(e)


def k_eval():
    for k in (20, 50):
        start(buchstabe="changedk/",
              new_index=True,
              k=k,
              num_keywords=9,
              min_rank=10,
              candidate_pos=('NOUN', 'PROPN', 'ADJ', 'VERB'),
              bsln_b=True)


if __name__ == '__main__':
    k_eval()
