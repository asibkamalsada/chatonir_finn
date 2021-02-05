import searchengine
import time
import evaluation


def main():
    se = searchengine.Searchengine()
    '''use this to test new changes
            True if you want to index the test data (obviously has to be done), this will override current index'''
    evaluation.evaluate(False)

'''
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
    s1 = {"a", "b", "d"}
    s2 = {"a", "b", "c"}
    s3 = {"a", "c", "d"}
    s4 = {"x", "y"}
    s5 = {"x", "z"}
    s6 = {"u"}
    print(set.intersection(s1, s2, s3, s4, s5, s6))
'''


if __name__ == '__main__':
    main()
