# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
from operator import add
sc = SparkContext.getOrCreate(SparkConf())


# Lager RDD som map. Brukernavn er Key. Value er en liste med strings.
# Listen med ord er ordnet sånn at alle ord fra alle tweets er i samme map.
# Value er en tuple: (ord som en liste, 0 som fungerer som counter)
def RDD(filePath):
    sc = SparkContext.getOrCreate(SparkConf())
    path = filePath
    rdd = sc.textFile(name=path)
    rdd = rdd.map(lambda line: tuple(line.split('\t')))
    rdd = rdd.reduceByKey(add)
    rdd = rdd.map(lambda line: (line[0], line[1].split(' ')))
    return rdd


def compare(user_list, x):
    x_list = x[1]
    count = 0
    for word in user_list:
        if word in x_list:
            count += 1
            x_list.remove(word)
    new_x = (x[0], count)
    return new_x


def counter(user_name, rdd):
    user_list = rdd.lookup(user_name)[0]
    similar = rdd.map(lambda x: compare(user_list, x))
    return similar


def sort(rdd):
    sorted = rdd.sortBy(lambda x: x[1])
    return sorted

# Metoden.
def recommend_user(user_name, k, file_path, output_path):
    rdd = RDD(file_path)
    counted = counter(user_name=user_name, rdd=rdd)
    sorted = sort(counted)
    recommendation = sorted.take(k)
    recommendation.map(lambda x: '{username}\t{count}'.format(username=x[0], count=x[1])).saveAsTextFile(output_path)


recommend_user('98kenedy', 10, './Dataset/tweets.tsv', './Dataset/result.tsv')