import searchengine
import time


def main():
    se = searchengine.Searchengine()
    t1 = time.time()
    # se.createIndexAndIndexDocs("dump_no_bodytext.json")
    t2 = time.time()
    print(t2-t1)
    se.create_index()
    se.index_data(se.readJSON("test.json"))
    se.update_abstracts("abstracts.json")
    t3 = time.time()
    print(t3-t2)
    se.update_keyqueries()
    t4 = time.time()
    print(t4-t3)
    # se.print_kqs()
    hits = [hit for hits in se.chunk_iterate_docs(1000) for hit in hits]
    for hit in hits:
        print(hit)
    se.search()


if __name__ == '__main__':
    main()
