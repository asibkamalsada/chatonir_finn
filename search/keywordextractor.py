import nltk
# nltk.download('stopwords')
# nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from operator import itemgetter
import math
import textrank

#doc = 'In information retrieval, tf–idf, TF*IDF, or TFIDF, short for term frequency–inverse document frequency, is a numerical statistic that is intended to reflect how important a word is to a document in a collection or corpus.[1] It is often used as a weighting factor in searches of information retrieval, text mining, and user modeling. The tf–idf value increases proportionally to the number of times a word appears in the document and is offset by the number of documents in the corpus that contain the word, which helps to adjust for the fact that some words appear more frequently in general. tf–idf is one of the most popular term-weighting schemes today. A survey conducted in 2015 showed that 83% of text-based recommender systems in digital libraries use tf–idf.[2] Variations of the tf–idf weighting scheme are often used by search engines as a central tool in scoring and ranking a documents relevance given a user query. tf–idf can be successfully used for stop-words filtering in various subject fields, including text summarization and classification. One of the simplest ranking functions is computed by summing the tf–idf for each query term; many more sophisticated ranking functions are variants of this simple model. '
doc = 'This is a python test. With python you can do a lot. Python is good for automated test.'
stopWords = set(stopwords.words('english'))
doc = doc.lower()
totalWords = doc.split()
totalWordsCount = len(totalWords)
totalSentences = sent_tokenize(doc)
totalSentencesCount = len(totalSentences)


def tf():
    tfScore = {}
    for eachWord in totalWords:
        eachWord = eachWord.replace('.', '')
        if eachWord not in stopWords:
            if eachWord in tfScore:
                tfScore[eachWord] += 1
            else:
                tfScore[eachWord] = 1

    tfScore.update((x, y / int(totalWordsCount)) for x, y in tfScore.items())
    return tfScore


def checkSent(word, sentences):
    final = [all([w in x for w in word]) for x in sentences]
    sentLen = [sentences[i] for i in range(0, len(final)) if final[i]]
    return int(len(sentLen))


def idf():
    idfScore = {}
    for eachWord in totalWords:
        eachWord = eachWord.replace('.', '')
        if eachWord not in stopWords:
            if eachWord in idfScore:
                idfScore[eachWord] = checkSent(eachWord, totalSentences)

            else:
                idfScore[eachWord] = 1

    idfScore.update((x, math.log(int(totalSentencesCount) / y)) for x, y in idfScore.items())
    return idfScore


def getTopN(dictElem, n):
    result = dict(sorted(dictElem.items(), key=itemgetter(1), reverse=True)[:n])
    return result


def main():
    tfScore = tf()
    idfScore = idf()
    tfidfScore = {key: tfScore[key] * idfScore.get(key, 0) for key in tfScore.keys()}
    print(getTopN(tfidfScore, 10))

    tr4w = textrank.TextRank4Keyword()
    tr4w.analyze(doc, candidate_pos=['NOUN', 'PROPN'], window_size=4, lower=False)
    tr4w.get_keywords(10)


if __name__ == '__main__':
    main()
