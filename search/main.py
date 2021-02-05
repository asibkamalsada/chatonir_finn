import time

import searchengine


def main():
    se = searchengine.Searchengine()
    t1 = time.time()
    # se.createIndexAndIndexDocs("dump_no_bodytext.json")
    t2 = time.time()
    print(t2 - t1)
    # se.extract_json("test", "test.json")
    se.createIndexAndIndexDocs("test.json")
    t3 = time.time()
    print(t3 - t2)
    # se.fill_documents("fulltexts.json")
    t4 = time.time()
    print(t4 - t3)
    se.update_keyqueries()
    t5 = time.time()
    print(t5 - t4)
    # hits = [hit for hits in se.chunk_iterate_docs() for hit in hits]
    # print(len(hits))
    se.debug_print()
    # se.search()


"""
    hits = [hit for hits in se.chunk_iterate_docs() for hit in hits]
    for hit in hits:
        print(hit)
"""

if __name__ == '__main__':
    main()
