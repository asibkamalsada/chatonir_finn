import searchengine


def main():
    se = searchengine.Searchengine()
    #se.createIndexAndIndexDocs("dump_no_bodytext.json")
    se.precalc_keyquerys()
    #se.search()


if __name__ == '__main__':
    main()
