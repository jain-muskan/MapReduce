package com.mapreduce.tasks.movieprofile;

import com.mapreduce.MRReducer;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.List;

public class MovieProfileReducer implements MRReducer, Serializable {

    public MovieProfileReducer() {
        super();
    }

    public List<String> reduce(String strin, List<String> list) {
        ArrayList<String> result = new ArrayList<String>();
        int count = 0;
        for (String str : list) {
            try {
                count += 1;
            } catch (Exception e) {
            }
        }
        String res = Integer.toString(count);
        result.add(res);
        return result;

    }
}
