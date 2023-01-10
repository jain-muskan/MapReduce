package com.mapreduce.tasks.wordcount;

import com.mapreduce.MapReduce;
import com.mapreduce.MapReduceSpecs;

import java.io.File;

public class WordCount {

    public static void main(String[] args) {
        MapReduceSpecs mp = new MapReduceSpecs();
        mp.inputFileLocation = System.getProperty("user.dir") + "/data/harryPotter.txt"; // test cases will be run from
                                                                                         // the root dir
        mp.outputFileLocation = System.getProperty("user.dir") + "/testCasesOutput/wordcount";
        File directory = new File(mp.outputFileLocation);
        directory.mkdir();
        mp.mapper = "com.mapreduce.tasks.wordcount.WordCountMapper";
        mp.reducer = "com.mapreduce.tasks.wordcount.WordCountReducer";
        MapReduce obj = new MapReduce();
        obj.mapReduce(mp);

    }
}
