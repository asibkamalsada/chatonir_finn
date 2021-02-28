import searchengine

import pandas as pd


def main():
    se = searchengine.Searchengine()
    se.start(10)


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
