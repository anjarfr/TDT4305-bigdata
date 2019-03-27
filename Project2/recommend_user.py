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


def compare(user_name, x):




def counter(user_name, rdd):
    similarity_rdd = rdd
    list_word = rdd.lookup(user_name)[0]
    rdd.lookup()
    rdd.foreach(lambda x:  compare(user_name, x))


# Metoden.
def recommend_user(user_name, k, file_path, output_path):



rdd = RDD('./Dataset/tweets.tsv')
print rdd.take(1)
print counter('98kenedy', rdd)