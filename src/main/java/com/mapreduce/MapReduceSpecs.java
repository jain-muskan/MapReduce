package com.mapreduce;

import java.io.Serializable;
import java.rmi.Remote;

public class MapReduceSpecs implements Remote, Serializable {
    //This represents the number of mapper and reducer processes
    public int numOfProcesses = 5;
    //Represents the input file location
    public String inputFileLocation;
    //Represents the output file location
    public String outputFileLocation;
    //Function which will execute the mapping processes(mapreduce.utils.Mapper)
    public String mapper;
    //Function which will execute the reducer processes(mapreduce.utils.Reducer)
    public String reducer;
    //Time limit for worker process(ms)
    public int timeout = 5000;
}
