import searchengine
import time
import evaluation


def main():
    se = searchengine.Searchengine()
    '''use this to test new changes
            True if you want to index the test data (obviously has to be done), this will override current index'''
    evaluation.evaluate(False)


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


if __name__ == '__main__':
    main()
