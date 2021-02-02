import time

import searchengine


def main():
    se = searchengine.Searchengine()
    t1 = time.time()
    se.createIndexAndIndexDocs("dump_no_bodytext.json")
    t2 = time.time()
    print(t2-t1)
    se.update_abstracts("abstracts.json")
    t3 = time.time()
    print(t3-t2)
    se.update_keyqueries()
    t4 = time.time()
    print(t4-t3)
    se.print_kqs()
    se.search()


if __name__ == '__main__':
    main()
