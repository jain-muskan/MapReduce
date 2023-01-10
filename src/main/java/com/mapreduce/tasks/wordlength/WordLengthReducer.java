package com.mapreduce.tasks.wordlength;

import com.mapreduce.MRReducer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class WordLengthReducer implements MRReducer, Serializable {
    public WordLengthReducer() {
        super();
    }

    public List<String> reduce(String strin, List<String> list) {
        ArrayList<String> result = new ArrayList<String>();

        String res = list.get(0);
        result.add(res);

        return result;

    }
}
