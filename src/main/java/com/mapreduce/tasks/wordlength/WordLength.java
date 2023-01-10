package com.mapreduce.tasks.wordlength;

import com.mapreduce.MapReduce;
import com.mapreduce.MapReduceSpecs;

import java.io.File;

public class WordLength {

    public static void main(String[] args) {

        MapReduceSpecs mp = new MapReduceSpecs();
        mp.inputFileLocation = System.getProperty("user.dir") + "/data/harryPotter.txt";
        mp.outputFileLocation = System.getProperty("user.dir") + "/testCasesOutput/wordlength";
        File directory = new File(mp.outputFileLocation);
        directory.mkdir();
        mp.mapper = "com.mapreduce.tasks.wordlength.WordLengthMapper";
        mp.reducer = "com.mapreduce.tasks.wordlength.WordLengthReducer";
        MapReduce obj = new MapReduce();
        obj.mapReduce(mp);

    }
}
