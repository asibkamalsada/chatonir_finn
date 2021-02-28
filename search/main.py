import searchengine
import time
import evaluation
import csv
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def main():
    se = searchengine.Searchengine()
    se.start()


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

def plot():
    #evaluation Data plot
    x = np.arange(4)
    #recall@k + NOUN
    y1 = [0.257, 0.230, 0.242, 0.209]  #5
    y2 = [0.302, 0.223, 0.220, 0.166]  #9
    y3 = [0.248, 0.211, 0.221, 0.200]  #11

    #nDCG@10 + NOUN
    y12 = [0.538, 0.515, 0.487, 0.421]  # 5
    y22 = [0.645, 0.524, 0.501, 0.401]  # 9
    y32 = [0.620, 0.507, 0.529, 0.492]  # 11

    #recall@k +NOUN,ADJ,VERB
    y13 = [0.217, 0.148, 0.146, 0.228]  # 5
    y23 = [0.324, 0.294, 0.280, 0.174]  # 9
    y33 = [0.295, 0.265, 0.246, 0.246]  # 11

    # nDCG@10 + NOUN,ADJ,VERB
    y14 = [0.444, 0.332, 0.328, 0.424]  # 5
    y24 = [0.631, 0.628, 0.592, 0.401]  # 9
    y34 = [0.661, 0.659, 0.625, 0.575]  # 11


    width = 0.2

    # plot data in grouped manner of bar type
    plt.bar(x - 0.2, y1, width, color='dodgerblue')
    plt.bar(x, y2, width, color='orange')
    plt.bar(x + 0.2, y3, width, color='green')
    plt.title('NOUN', fontsize=16)
    plt.xticks(x, ['10', '50', '100', '10000'])
    plt.xlabel("Score for Keyqueries")
    plt.ylabel("recall@10")
    plt.yticks(np.arange(0, 0.35, 0.025))
    plt.legend(["5 KW", "9 KW", "11 KW"])
    plt.axhline(y=0.277, color='r', linestyle=':')
    plt.savefig('Nrecall.png')
    plt.show()

    plt.bar(x - 0.2, y13, width, color='dodgerblue')
    plt.bar(x, y23, width, color='orange')
    plt.bar(x + 0.2, y33, width, color='green')
    plt.title('NOUN/ADJ/VERB', fontsize=16)
    plt.xticks(x, ['10', '50', '100', '10000'])
    plt.xlabel("Score for Keyqueries")
    plt.ylabel("recall@10")
    plt.yticks(np.arange(0, 0.35, 0.025))
    plt.legend(["5 KW", "9 KW", "11 KW"])
    plt.axhline(y=0.277, color='r', linestyle=':')
    plt.savefig('NAVrecall.png')
    plt.show()

    plt.bar(x - 0.2, y12, width, color='dodgerblue')
    plt.bar(x, y22, width, color='orange')
    plt.bar(x + 0.2, y32, width, color='green')
    plt.title('NOUN', fontsize=16)
    plt.xticks(x, ['10', '50', '100', '10000'])
    plt.xlabel("Score for Keyqueries")
    plt.ylabel("nDCG@10")
    plt.legend(["5 KW", "9 KW", "11 KW"])
    plt.axhline(y=0.603, color='r', linestyle=':')
    plt.savefig('NnDCG.png')
    plt.show()

    plt.bar(x - 0.2, y14, width, color='dodgerblue')
    plt.bar(x, y24, width, color='orange')
    plt.bar(x + 0.2, y34, width, color='green')
    plt.title('NOUN/ADJ/VERB', fontsize=16)
    plt.xticks(x, ['10', '50', '100', '10000'])
    plt.xlabel("Score for Keyqueries")
    plt.ylabel("nDCG@10")
    plt.legend(["5 KW", "9 KW", "11 KW"])
    plt.axhline(y=0.603, color='r', linestyle=':')
    plt.savefig('NAVnDCG.png')
    plt.show()


if __name__ == '__main__':
    main()
