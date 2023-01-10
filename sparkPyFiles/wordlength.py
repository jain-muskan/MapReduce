from pyspark import SparkContext, SparkConf

#setting up spark configuration and connecting it
conf = SparkConf().setAppName('Word Length').setMaster('local')
sc = SparkContext(conf=conf)

#reading text file
txtFile = sc.textFile("data/harryPotter.txt")

#splitting the text into words and avoiding any null spaces as part of word list
text = txtFile.flatMap(lambda x: x.split()).filter(lambda x:x!='')

#mapping all words to its length 
eachWordLength = text.map(lambda x: (x,len(x)))


wordLengths = eachWordLength.reduceByKey(lambda x,y: x)
#print(wordLengths.collect())

f = open("sparkOutput/wordLengthOutput.txt", "w")
for r in wordLengths.collect():
    f.write("{:} : [{:}]\n".format(r[0], str(r[1])))
f.close()
