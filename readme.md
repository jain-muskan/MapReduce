
# PROJECT 1 - MAPREDUCE 

## **Overview**
For this project, we have implemented MapReduce Library using Java. We have taken three test cases to show the working of the MapReduce function.
 The three test cases are:
 1. Word Count - Given a large text fule, find the frequency of each word.
 2. Word Length - Given a large text file, find the number of words of each word length. 
 3. Movie Profile -  Given a large text file which consists of the names of people and the movies they have watches, find the number of movies each person watched.

## **Code Execution**
To execute the three test cases, the shell script "runTestCases.sh" should be run from the source directory. This compiles all the library code with the test cases and then runs them.
Execute ```sh runTestCases.sh``` / ``` ./runTestCases.sh``` on MacOS or
Execute ```runTestCases.bat``` on Windows 



On running the bash script, outputComparison.py is also executed. This compares the output of MapReduce Library and the PySpark implementation for each of the test case and tells if the results are matching or not, which can be seen as output in the terminal.

## **Output**
The MapReduce library will give N output files, based on the number of processes. The PySpark implementation will give one output file for a single test case execution.
The output for the test cases are present in docs directory. The output for the PySpark implementation is present in the “sparkOutput” folder in the root directory.
