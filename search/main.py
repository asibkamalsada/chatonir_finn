import searchengine
import time
import evaluation
import csv
import pandas as pd

def main():
    se = searchengine.Searchengine()
    '''use this to test new changes
            True if you want to index the test data (obviously has to be done), this will override current index'''
   # se.start()
    evaluation.evaluate(new_index=False)


    #t1 = time.time()
    #se.createIndexAndIndexDocs("dump_no_bodytext.json")
    #t2 = time.time()
    #print(t2-t1)
    #se.create_index()
    #se.index_data(se.readJSON_("dump_no_bodytext.json"))
    #se.update_abstracts("abstracts.json")
    #t3 = time.time()
    #print(t3-t2)
    #se.update_keyqueries()
    #t4 = time.time()
    #print(t4-t3)
    # se.print_kqs()
    # hits = [hit for hits in se.chunk_iterate_docs(1000) for hit in hits]
    # for hit in hits:
    #    print(hit)
    '''
    t1 =time.time()
    s1 = {"a", "b", "d"}
    s2 = {"a", "b", "c"}
    s3 = {"a", "c", "d"}
    s4 = {"x", "y"}
    s5 = {"x", "z"}
    s6 = {"u"}
    print(set.intersection(s1, s2, s3, s4, s5, s6))
    sets = [s1, s2, s3, s4, s5, s6]
    result={}
    counter = 0
    while True:
        if counter >= len(sets):
            break
        for i in sets:
            for j in sets:
                if not i == j:
                    temp = i.intersection(j)
                    if temp:
                        result[frozenset(temp)] = [frozenset(i), frozenset(j)]
        counter += 1
        newresult = dict(result)

        for i in result:
            for j in sets:
                if frozenset(set(i)) not in result:
                    break
                if frozenset(j) not in result[i]:
                    temp = i.intersection(j)
                    if temp:
                        x = result[frozenset(i)]
                        x.append(frozenset(j))
                        newresult.pop(i)
                        newresult[frozenset(temp)] = x

        result = dict(newresult)
    ihatemylife = frozenset()
    for nah in result.values():
        for lel in nah:
            ihatemylife = ihatemylife.union(frozenset(lel))

    for myset in sets:
        for v in myset:
            if v not in ihatemylife:
                result[v] = frozenset(myset)
    print(result)
    t2 = time.time()
    print(t2-t1)
    '''

def creating_new_data():
    with open('json/ranking.csv') as csvDataFile:
        mycsv = pd.read_csv(csvDataFile, delimiter='\t')
        mycsv.columns = ['topicid', 'ni', 'acmId', 'ranking']
        print(mycsv)
    with open('json/newdata.json', 'r', encoding='utf8') as file:
        df = pd.read_json(file)
    mytest = pd.merge(mycsv, df, on='acmId', how='inner')
    lel = mytest[['title', 'abstract', 'acmId']].drop_duplicates()
    lel.columns = ['title', 'abstract', 'doi']
    lel.to_json('data.json', orient='records')
    #mytest[['topicid', 'title', 'ranking', 'acmId']].to_csv('evaluation.csv')


if __name__ == '__main__':
    main()
