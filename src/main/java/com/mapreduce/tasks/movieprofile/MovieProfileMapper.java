package com.mapreduce.tasks.movieprofile;

import java.io.*;
import java.util.*;
import com.mapreduce.MRMapper;

public class MovieProfileMapper implements MRMapper, Serializable {

    public MovieProfileMapper() {
        super();
    }

    public HashMap<String, List<String>> map(String docId, String input) {
        HashMap<String, List<String>> map = new HashMap<>();

        String[] words = input.toLowerCase(Locale.ROOT).split("\\n");

        for (String str : words) {
            if (!str.equals("null")) {
                String updatedString = str;
                String[] key_value = updatedString.split(":");
                map.computeIfAbsent(key_value[0], k -> new ArrayList<String>());
                if (!map.get(key_value[0]).contains(key_value[1]))
                    map.get(key_value[0]).add(key_value[1]);
            }
        }
        return map;

    }
}
