package com.mapreduce;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;

import com.mapreduce.util.RMITools;

//This class defines the implementation of a mapper worker node
public class MRMapperWorker {
    
    /** 
     * This function reads from input file and writes to intermediate files
     * @param mapperObj
     * @param inputFileLocation
     * @param startLine
     * @param endLine
     * @param currProcessNum
     */
    public void mapperExecute(MRMapper mapperObj, String inputFileLocation, int startLine, int endLine, int currProcessNum) {

        StringBuilder input = new StringBuilder();
        // This block reads from the input file
        try {
            FileInputStream filestream = new FileInputStream(inputFileLocation);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(filestream));
            for (int i = 0; i < startLine; ++i)
                bufferedReader.readLine();
            for (int i = startLine; i < endLine; i++) {
                input.append(bufferedReader.readLine());
                input.append("\n");
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        // This block creates writers to intermediate files for each mapper
        HashMap<String, List<String>> output;
        try {
            output = mapperObj.map(String.valueOf(startLine), input.toString());
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            // intermediate file name will be of the form mapperoutput_processNUmber.txt
            File directory = new File(new File(inputFileLocation).getParentFile().getName() + "mapperoutput");
            if (!directory.exists()) directory.mkdir();
            String tempMapperFileName = "mapperoutput_" + currProcessNum + ".txt";
            File mapperOutputFile = new File(directory.getPath() + "/" + tempMapperFileName);
            FileWriter fw = new FileWriter(mapperOutputFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);

            output.forEach((key, values) -> {
                values.forEach((value)->{
                    try {
                        bw.write(key + ":" + value);
                        bw.write("\n");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                

            });
            bw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    
    /**
     * This gets invoked by master as a separate process 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String startLine = args[0];
        String endLine = args[1];
        String process_num = args[2];
        int timeout = Integer.parseInt(args[3]);
        timeout = timeout > 0 ? timeout : 5000;
        while (args.length > 4 && Integer.parseInt(process_num) == Integer.parseInt(args[4]))
            ; // to simulate fault tolerance
        MapReduceSpecs specs = null;
        MRMapper mapperObj = null;
        try {
            specs = RMITools.getSpecs();
            Class<?> userDefinedMapper = Class.forName(specs.mapper);
            Constructor<?> ct = userDefinedMapper.getConstructor();
            mapperObj = (MRMapper) ct.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        if (mapperObj == null)
            return;
        MRMapperWorker mw = new MRMapperWorker();
        mw.mapperExecute(mapperObj, specs.inputFileLocation, Integer.parseInt(startLine), Integer.parseInt(endLine),
                Integer.parseInt(process_num));

    }
}
