import filecmp
from typing import DefaultDict

from pip import main

print("\n COMPARISON RESULTS\n")
#Test Case 1 - Word Count
d1 = "finalOutput/com.mapreduce.tasks.wordcount.WordCountMapperoutput.txt"
d2 = "sparkOutput/wordCountOutput.txt"

f1, f2 = open(d1, 'r'), open(d2, 'r')

File1, File2 = open(d1, "r"), open(d2, "r")
Dict1, Dict2 = File1.readlines(), File2.readlines()

print("****WORD COUNT RESULTS****")

Finaldict1 =[]
for line in Dict1:
    Finaldict1.append(line.lower())

Finaldict2 =[]
for line in Dict2:
    Finaldict2.append(line.lower())

sorted1, sorted2 = sorted(Finaldict1), sorted(Finaldict2)

main_list = list(set(sorted2) - set(sorted1)) #taking difference of the outputs from the two files
#print(main_list)
if len(main_list)<1   :
    
    print("MapReduce library and PySpark outputs have matched!\n")
else:
    print("Word Count outputs don't match.\n")


#Test Case 2 - Word Length
d1 = "finalOutput/com.mapreduce.tasks.wordlength.WordLengthMapperoutput.txt"
d2 = "sparkOutput/wordLengthOutput.txt"

#f1, f2 = open(d1, 'r'), open(d2, 'r')

File1, File2 = open(d1,"r"), open(d2,"r")
Dict1, Dict2 = File1.readlines(), File2.readlines()

print("****WORD LENGTH RESULTS****")

Finaldict1 =[]
for line in Dict1:
    Finaldict1.append(line.lower())

Finaldict2 =[]
for line in Dict2:
    Finaldict2.append(line.lower())

sorted1, sorted2 = sorted(Finaldict1), sorted(Finaldict2)

main_list = list(set(sorted1) - set(sorted2)) #taking difference of the outputs from the two files
#print(main_list)
if len(main_list)<1 :
    print("MapReduce library and PySpark outputs have matched!\n")
else:
    print("Word Length outputs don't matched.\n")


#Test Case 3 - Movie Profile
d1 = "finalOutput/com.mapreduce.tasks.movieprofile.MovieProfileMapperoutput.txt"
d2 = "sparkOutput/movieProfileOutput.txt"

f1, f2 = open(d1, 'r'), open(d2, 'r')

File1, File2 = open(d1,"r"), open(d2,"r")
Dict1, Dict2 = File1.readlines(), File2.readlines()

print("****MOVIE PROFILE RESULTS****")

Finaldict1 =[]
for line in Dict1:
    Finaldict1.append(line.lower())

Finaldict2 =[]
for line in Dict2:
    Finaldict2.append(line.lower())

sortedd1, sortedd2 = sorted(Finaldict1), sorted(Finaldict2)


main_list = list(set(sortedd1) - set(sortedd2)) #taking difference of the outputs from the two files
# print(main_list)
if len(main_list)<1 :
    print("MapReduce library and PySpark outputs have matched!.\n")
else:
    print("Movie Profile outputs don't matched.\n")
