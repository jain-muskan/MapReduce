package com.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.mapreduce.util.RMITools;

/**
 * The ReducerWorker class that defines the working of a worker node acting as a
 * reducer
 */
public class MRReducerWorker {

    
    /**
     * method to call user defined reduce function and write output to N files 
     * @param reducerInput
     */
    public void execute(MRReducer reducerObj, Set<String> workerKeys, int reducerNumber, String outputFileLocation,
            HashMap<String, List<String>> reducerInput) {
        // create output file if it does not exist
        String outputFileName = "output_" + reducerNumber + ".txt";
        File outputFile = new File(outputFileLocation + "/" + outputFileName);
        try {
            FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            // perform reduce operation for each key
            reducerInput.entrySet().forEach(entry -> {
                List<String> reduceOutput = null;
                try {
                    if (workerKeys.contains(entry.getKey())) {
                        reduceOutput = reducerObj.reduce(entry.getKey(), entry.getValue());
                    }
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                try {
                    if (reduceOutput != null) {
                        bw.write(entry.getKey() + " : " + reduceOutput.toString() + "\n");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    
    /**
     * method to get all keys abd their corresponding values 
     * @param specs
     * @return HashMap<String, List<String>>
     */
    private HashMap<String, List<String>> getMapFromTextFile(MapReduceSpecs specs) {
        File dir = new File(new File(specs.inputFileLocation).getParentFile().getName() + "mapperoutput");
        HashMap<String, List<String>> map = new HashMap<>();
        // all values for a given key will be in the same file
        for (File file : dir.listFiles()) {
            try (Stream<String> stream = Files.lines(Paths.get(file.getAbsolutePath()))) {
                stream.forEach(line -> {
                    String[] parts = line.split(":");
                    map.computeIfAbsent(parts[0], k -> new ArrayList<>());
                    map.get(parts[0]).add(parts[1]);
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    
    /** 
     * will be invoked by the master as a separate process
     * @param args
     * @throws Exception
     */
    // 
    public static void main(String[] args) throws Exception {
        // read all input parameters provided by master
        int reducerNumber = Integer.parseInt(args[0]);
        int timeout = Integer.parseInt(args[1]);
        timeout = timeout > 0 ? timeout : 5000;
        while (args.length > 2 && reducerNumber == Integer.parseInt(args[2]))
            ; // to simulate fault tolerance
        MRReducer reducerObj = null;
        MapReduceSpecs specs = null;
        ReducerKeys reducerKeys = null;
        try {
            specs = RMITools.getSpecs();
            reducerKeys = RMITools.getReducerKeys();
            Class<?> userDefinedReducer = Class.forName(specs.reducer);
            Constructor<?> ct = userDefinedReducer.getConstructor();
            reducerObj = (MRReducer) ct.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        int keysPartitionSize = (int) Math.ceil((double) reducerKeys.getReducerKeys().length / specs.numOfProcesses);
        int startKey = reducerNumber * keysPartitionSize;
        int endKey = startKey + keysPartitionSize;
        Set<String> workerKeys = new HashSet<>();
        for (int i = startKey; i < endKey && i < reducerKeys.getReducerKeys().length; i++) {
            workerKeys.add(reducerKeys.getReducerKeys()[i]);
        }

        if (reducerObj == null)
            return;
        MRReducerWorker r = new MRReducerWorker();
        HashMap<String, List<String>> tempData = r.getMapFromTextFile(specs);
        r.execute(reducerObj, workerKeys, reducerNumber, specs.outputFileLocation, tempData);

    }
}
