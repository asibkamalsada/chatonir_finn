import searchengine
import time
from sklearn.metrics import ndcg_score, dcg_score
import numpy as np
import pandas as pd

def evaluate(new_index=False):
    se = searchengine.Searchengine()
    if new_index:
        se.create_index()
        se.index_data(se.readJSON("json/data.json"))
        se.index_data(se.readJSON("json/testdata.json"))
        se.index_data(se.readJSON('json/noise.json'))
  #      se.fill_documents('json/fulltexts.json')
        se.update_keyqueries()
        time.sleep(1)  # elasticsearch is too slow lol

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

    newinputs = {str(1) : ['Visualizing Locations of Off-screen Objects on Mobile Devices: A Comparative Evaluation of Three Approaches', 'City Lights: Contextual Views in Minimal Space'],
                 str(2) : ['An Adaptive and Dynamic Dimensionality Reduction Method for High-dimensional Indexing', 'The IGrid index: reversing the dimensionality curse for similarity indexing in high dimensional space'],
                 str(3) : ['Generating comparative summaries of contradictory opinions in text','Mining Contrastive Opinions on Political Texts Using Cross-perspective Topic Model'],
                 str(4): ['RDF-3X: A RISC-style Engine for RDF','SW-Store: A Vertically Partitioned DBMS for Semantic Web Data Management'],
                 str(5): ['Cache-oblivious hashing','External Hashing with Limited Internal Storage'],
                 str(6): ['A Survey of Web Clustering Engines', 'A Comprehensive Comparison Study of Document Clustering for a Biomedical Digital Library MEDLINE'],
                 str(7): ['Web Search Clustering and Labeling with Hidden Topics','A Search Result Clustering Method Using Informatively Named Entities'],
                 str(8): ['Automatically Building Research Reading ','Recommending Academic Papers via Users\' Reading Purposes'],
                 str(9): ['Search-logger Analyzing Exploratory Search Tasks','Collaborative Multi-paradigm Exploratory Search'],
                 str(10): ['Unsupervised query segmentation using generative language models and wikipedia','Two-stage query segmentation for information retrieval'],
                 str(11): ['Search-logger Analyzing Exploratory Search Tasks','Clustering Versus Faceted Categories for Information Exploration'],
                 str(12): ['A unified and discriminative model for query refinement','Exploring web scale language models for search query processing']
                 }
    newtest(newinputs, se)
    #oldtest(queryinputs, se)



def newtest(newinputs, se):
    print("----------------- Test with \"new\" data --------------------")
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
        kq = se.select_keyquerie(papers)
        score = 0
        if isinstance(kq, tuple):
            score_this = se.normal_search_exclude_ids(" ".join(kq[0]), ids=ids, size=10)
        else:
            score_this = se.normal_search_exclude_ids(kq, ids=ids, size=10)
        df = evaluationcsv[(evaluationcsv['topicid'] == int(i))]
        for hit in score_this["hits"]["hits"]:
            for row in df['title']:
                if hit["_source"]["title"] == row:
                    counter += 1
                    score += hit["_score"]
                    lel = df[df['title'] == row]['ranking']
                    if(int(lel) > 0):
                        goodhitcounter += 1
                    rel_score.append(int(lel))
                    break
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

        temprecall = len([x for x in rel_score if x > 0])/ len(df)
        temppressision = len([x for x in rel_score if x > 0])/len(rel_score)
        print("For topicid:"+str(i)+" the score of found paper is: " + str(score))
        print("Pressision@k: "+ str(temppressision))
        print("Recall@k: "+ str(temprecall))
        print("The nDCG@10 score for this search is " + str(ndcg_score) + ".")
        counter = 0
        goodhitcounter = 0
        recall.append(temprecall)
        pressision.append(temppressision)

        papers.clear()

    print("\nThe average nDCG@10 score is " + str(sum(nDCG_scores)/len(nDCG_scores)) + ".")
    print("The average precision@k score is " + str(sum(pressision)/len(pressision)) + ".")
    print("The average recall@k score is " + str(sum(recall)/len(recall)) + ".")



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
        kq = se.select_keyquerie(papers)
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