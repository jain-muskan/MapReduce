package com.mapreduce.tasks.wordcount;

import java.io.*;
import java.util.*;
import com.mapreduce.MRMapper;

public class WordCountMapper implements MRMapper, Serializable {
    public WordCountMapper() {
        super();
    }

    public HashMap<String, List<String>> map(String docId, String input) {
        HashMap<String,List<String>> map = new HashMap<>();
        //String[] lines = input.split(System.lineSeparator());
        String[] words = input.split("\\s+");
        for (int word = 0; word < words.length; word++){
            words[word] = words[word].trim();
            if(words[word].length()>0){
                map.computeIfAbsent(words[word], k -> new ArrayList<String>());
                map.get(words[word]).add("1");
            }
        }
        return map;

    }
}
