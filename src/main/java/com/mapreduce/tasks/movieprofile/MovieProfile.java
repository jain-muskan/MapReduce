package com.mapreduce.tasks.movieprofile;

import com.mapreduce.MapReduce;
import com.mapreduce.MapReduceSpecs;

import java.io.File;

public class MovieProfile {

    public static void main(String[] args) {

        MapReduceSpecs mp = new MapReduceSpecs();

        mp.inputFileLocation = System.getProperty("user.dir") + "/data/movieProfile.txt"; // test cases will be run from
                                                                                          // the root dir
        mp.outputFileLocation = System.getProperty("user.dir") + "/testCasesOutput/movieprofile";
        System.out.println(mp.outputFileLocation);
        File directory = new File(mp.outputFileLocation);
        directory.mkdir();
        mp.mapper = "com.mapreduce.tasks.movieprofile.MovieProfileMapper";
        mp.reducer = "com.mapreduce.tasks.movieprofile.MovieProfileReducer";
        MapReduce obj = new MapReduce();

        obj.mapReduce(mp);
        System.exit(0);

    }
}
