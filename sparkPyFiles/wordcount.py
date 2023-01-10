from pyspark import SparkContext, SparkConf

#setting up spark configuration and connecting it
conf = SparkConf().setAppName('Word Count').setMaster('local')
sc = SparkContext(conf=conf)

#reading text file
txtFile = sc.textFile("data/harryPotter.txt")

#splitting the text into words and avoiding any null spaces as part of word list
text = txtFile.flatMap(lambda x: x.split()).filter(lambda x:x!='')

#mapping all words to 1 i.e. initialising every word count to 1
words = text.map(lambda x: (x,1))

#adding the values for every different key (which are words in this case)
wordCounts = words.reduceByKey(lambda x,y: x+y)
#print(wordCounts.collect())

f = open("sparkOutput/wordCountOutput.txt", "w")
for r in wordCounts.collect():
    f.write("{:} : [{:}]\n".format(r[0], str(r[1])))
f.close()
