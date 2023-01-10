package com.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import com.mapreduce.util.RMITools;

//This is the main class for MapReduce functionality which invokes map and reduce code.
public class MapReduce {

    
    /** 
     * @param specs
     * Main Method to run for MapReduce application
     */
    // String mapperRmiKey;
    // String reducerRmiKey;

    public void mapReduce(MapReduceSpecs specs) {
        try {
            RMITools.insertInRmiRegistry(specs, "specs");
        } catch (Exception e) {
            e.printStackTrace();
        }
        String inputLocation = specs.inputFileLocation;
        int totalLines = getTotalLines(inputLocation);
        List<Process> mapperProcesses = new ArrayList<>();
        List<Process> reducerProcesses = new ArrayList<>();
        int numberOfProcesses = specs.numOfProcesses;
        int divisionLength = (int)Math.ceil((double)totalLines / (specs.numOfProcesses));
        // Randomly mark one mapper and one reducer as faulty.
        int faultyMapper = selectFaultyWorker(numberOfProcesses);
        int faultyReducer = selectFaultyWorker(numberOfProcesses);

        // int faultyMapper = -1;
        // int faultyReducer = -1;

        try {
            System.out.println(specs.mapper + " has started execution.");
            // start all mapper nodes
            for (int i = 0; i < numberOfProcesses; i++) {
                int startLine = i * divisionLength;
                int endLine = startLine + divisionLength;
                if (endLine >= totalLines) {
                    endLine = totalLines;
                }
                // System.out.println(specs.mapperRmiObj + "-"+i+" working from line " +
                // startLine + " to line " + endLine);
                ProcessBuilder processBuilderMapper = new ProcessBuilder("java",
                        "-cp", ((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs()[0].getFile(),
                        "com.mapreduce.MRMapperWorker", String.valueOf(startLine),
                        String.valueOf(endLine),
                        String.valueOf(i), String.valueOf(specs.timeout), String.valueOf(faultyMapper));

                Process mprocess = processBuilderMapper.inheritIO().start();
                mapperProcesses.add(mprocess);
            }

            // wait for all mapper processes to complete
            for (int i = 0; i < mapperProcesses.size(); i++) {
                boolean timeOut = false;

                if (!mapperProcesses.get(i).waitFor(specs.timeout, TimeUnit.MILLISECONDS)) {
                    mapperProcesses.get(i).destroy();
                    timeOut = true;
                }
                System.out.println(specs.mapper + "-" + (i >= numberOfProcesses ? faultyMapper : i)
                        + (timeOut || mapperProcesses.get(i).exitValue() != 0 ? " id Straggler node failed! Retrying..."
                                : ""));
                if (i < numberOfProcesses && (timeOut || mapperProcesses.get(i).exitValue() != 0)) {
                    // if there was a fault in any original worker, then restart that worker
                    int startLine = i * divisionLength;
                    int endLine = startLine + divisionLength;
                    if (endLine >= totalLines) {
                        endLine = totalLines;
                    }
                    ProcessBuilder processBuilderMapper = new ProcessBuilder("java",
                            "-cp", ((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs()[0].getFile(),
                            "com.mapreduce.MRMapperWorker", String.valueOf(startLine),
                            String.valueOf(endLine),
                            String.valueOf(i), String.valueOf(specs.timeout));
                    Process mp = processBuilderMapper.inheritIO().start();
                    mapperProcesses.add(mp);
                }
            }

            System.out.println(specs.mapper + " has finished executing successfully.");

            ReducerKeys reducerKeys = getKeys(specs);
            try {
                RMITools.insertInRmiRegistry(reducerKeys, "reducerKeys");
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            // System.out.println(((URLClassLoader)ClassLoader.getSystemClassLoader()).getURLs()[0].getFile());

            System.out.println(specs.mapper + " has started execution.");
            // start all reducer processes
            for (int i = 0; i < numberOfProcesses; i++) {
                ProcessBuilder processBuilderReducer = new ProcessBuilder("java",
                        "-cp", ((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs()[0].getFile(),
                        "com.mapreduce.MRReducerWorker",
                        String.valueOf(i),
                        String.valueOf(specs.timeout),
                        String.valueOf(faultyReducer));
                Process reducer = processBuilderReducer.inheritIO().start();
                reducerProcesses.add(reducer);
            }

            // wait for all reducer processes to complete
            for (int i = 0; i < reducerProcesses.size(); i++) {
                boolean timeOut = false;
                if (!reducerProcesses.get(i).waitFor(specs.timeout, TimeUnit.MILLISECONDS)) {
                    reducerProcesses.get(i).destroy();
                    timeOut = true;
                }
                System.out.println(specs.reducer + "-" + (i >= numberOfProcesses ? faultyReducer : i)
                        + (timeOut || reducerProcesses.get(i).exitValue() != 0
                                ? " id Straggler node failed! Retrying..."
                                : " finished successfully."));
                if (i < numberOfProcesses && (timeOut || reducerProcesses.get(i).exitValue() != 0)) {
                    // if there was a fault in any original worker, then restart that worker
                    ProcessBuilder processBuilderReducer = new ProcessBuilder("java",
                        "-cp", ((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs()[0].getFile(),
                        "com.mapreduce.MRReducerWorker",
                        String.valueOf(i),
                        String.valueOf(specs.timeout));
                    Process reducer = processBuilderReducer.inheritIO().start();
                    reducerProcesses.add(reducer);
                }
            }
            System.out.println(specs.reducer + " has finished executing successfully.");
            System.out.println("Map Reduce completed for " + specs.mapper + "/" + specs.reducer + "\n");
            // for (int i = 0; i < numberOfProcesses; i++) {
            // // delete all the temporary files
            // File tempDir = new File("reduceroutput_" + i);
            // for (File file : tempDir.listFiles())
            // if (!file.isDirectory())
            // file.delete();
            // tempDir.delete();

            // }
            mergefiles(specs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
    /** 
     * @param specs
     * Merge files from reducers, not needed for ouput.
     */
    public void mergefiles(MapReduceSpecs specs) {
        Path path = null;
        try {

            path = Paths.get(System.getProperty("user.dir") + "/finalOutput");

            // java.nio.file.Files;
            Files.createDirectories(path);

        } catch (Exception e) {

            System.err.println("Failed to create directory!" + e.getMessage());

        }

        File dir = new File(specs.outputFileLocation);
        PrintWriter pw = null;
        System.out.println("Combining contents of " + specs.reducer + " outputs to one final.");
        // create object of PrintWriter for output file
        try {

            pw = new PrintWriter(System.getProperty("user.dir") + "/finalOutput/" + specs.mapper + "output.txt");
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Get list of all the files in form of String Array
        String[] fileNames = dir.list();

        // loop for reading the contents of all the files
        // in the directory
        BufferedReader br = null;
        for (String fileName : fileNames) {

            // create instance of file from Name of
            // the file stored in string Array
            File f = new File(dir, fileName);
            try {
                br = new BufferedReader(new FileReader(f));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            try {
                // Read from current file
                String line = br.readLine();
                while (line != null) {
                    // write to the output file
                    pw.println(line);
                    line = br.readLine();
                }
                pw.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Combining results from all files in directory " + dir.getName() + " completed");
    }

    
    /** 
     * Randomly select faulty process
     * @param numberOfProcesses
     * @return int
     */
    // Generate a faulty node
    private int selectFaultyWorker(int numberOfProcesses) {
        int faultyNode = (int) (Math.random() * numberOfProcesses);
        return faultyNode;
    }

    
    /** 
     * Read all files for totallines
     * @param inputLocation
     * @return int
     */
    // counts total lines in file
    private int getTotalLines(String inputLocation) {
        int lineCount = 0;
        try (Stream<String> lines = Files.lines(Paths.get(inputLocation))) {
            lineCount = (int) lines.count();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lineCount;
    }

    
    /** 
     * Read all intermediate files for keys.
     * @param specs
     * @return ReducerKeys
     */
    private ReducerKeys getKeys(MapReduceSpecs specs) {
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
        return new ReducerKeys(map.keySet());
    }

    // inserts Specs object in the RMI registry

}
