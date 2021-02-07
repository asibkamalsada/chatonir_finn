import searchengine
import time

def evaluate(new_index=False):
    se = searchengine.Searchengine()
    papers = []
    if new_index:
        se.create_index()
        se.index_data(se.readJSON("json/testdata.json"))
        #se.update_abstracts("abstracts.json")
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

    testcase = 0
    counter = 0
    miss = 0
    perfect = 0
    for i in queryinputs:
        for j in i:
            response = se.title_search(j)
            papers.append(response["hits"]["hits"][0])
        kq = se.select_keyquerie(papers)
        score = 0
        if isinstance(kq, tuple):
            score_this = se.normal_search(" ".join(kq[0]), size=1000)
        else:
            score_this = se.normal_search(kq, size=1000)
        for x in queryinputs[tuple(i)]:
            for hit in score_this["hits"]["hits"]:
                if hit["_source"]["title"] == x:
                    counter += 1
                    score += hit["_score"]
                    break
        testcase += 1
        if counter == 0:
            miss += 1
        if counter == len(queryinputs[tuple(i)]):
            perfect += 1
        print("For testcase:"+str(testcase)+" -" + i[0] + ", [...]-  the score of found paper is: " + str(score))
        print(str(counter) + " out of " + str(len(queryinputs[tuple(i)])) + " was/where found.")
        counter = 0

        papers.clear()
    print("\n""We did not find any paper in " + str(miss) + " out of " + str(len(queryinputs)) + " cases!")
    print("\n""Every expected paper was found in " + str(perfect) + " out of " + str(len(queryinputs)) + " cases!")