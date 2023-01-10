from pyspark import SparkContext
import re

sc = SparkContext()
movieProfiles = sc.textFile("data/movieProfile.txt")
intm = movieProfiles.collect()[:] #to remove end of line from input
movieMap = sc.parallelize(intm) #conversion to rdd dataframe from list

#Splitting the text based on the lines
lines = movieMap.flatMap(lambda txt: txt.lower().split("/n"))

#splitting the lines based on ":"
movieName = lines.flatMap(lambda x: [list(x.split(":"))[0]])

#taking count for each movie entry
movieNameCount = movieName.map(lambda word: (word, 1))
movieRdd = movieNameCount.reduceByKey(lambda a, b: a+b).sortByKey()
movieArr = movieRdd.collect()

f = open("sparkOutput/movieProfileOutput.txt", "w")
for r in movieArr:
    f.write("{:} : [{:}]\n".format(r[0], str(r[1])))
f.close()
