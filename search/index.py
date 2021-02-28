import searchengine


def main():
    se = searchengine.Searchengine()
    se.create_index()
    se.index_data(se.readJSON("json/dump_no_bodytext.json"))
    se.fill_documents('json/abstracts.json')
    se.fill_documents('json/fulltexts.json')
    se.update_keyqueries(num_keywords=9, min_rank=10, candidate_pos=('NOUN', 'PROPN', 'ADJ', 'VERB'))
    se.es_client.indices.refresh(se.INDEX_NAME)
    print("--- Finish ---")


if __name__ == '__main__':
    main()
