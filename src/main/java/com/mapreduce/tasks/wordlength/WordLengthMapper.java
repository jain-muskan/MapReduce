package com.mapreduce.tasks.wordlength;

import java.io.*;
import java.util.*;
import com.mapreduce.MRMapper;

public class WordLengthMapper implements MRMapper, Serializable {
    public WordLengthMapper() {
        super();
    }

    public HashMap<String, List<String>> map(String docId, String input) {
        HashMap<String,List<String>> map = new HashMap<>();
        //String[] lines = input.split(System.lineSeparator());
        String[] words = input.split("\\s+");
        for(int word =0 ; word<words.length;word++)
        {
            words[word] = words[word].trim();
            if(words[word].length()>0) 
            {   
                
                
                map.computeIfAbsent(words[word], k -> new ArrayList<String>());
                String wordLength = String.valueOf(words[word].length());
                map.get(words[word]).add(wordLength);

                    
                
                
            }
        }
        return map;

    }
}
