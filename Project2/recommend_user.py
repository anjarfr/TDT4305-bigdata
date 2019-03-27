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
    rdd = rdd.map(lambda line: (line[0], (line[1].split(' '), 0)))
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
    rdd.foreach(lambda x: compare(user_list, x))
    return rdd


# Metoden.
def recommend_user(user_name, k, file_path, output_path):



rdd = RDD('./Dataset/tweets.tsv')
print counter('98kenedy', rdd).take(10)