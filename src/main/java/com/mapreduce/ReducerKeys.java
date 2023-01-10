package com.mapreduce;

import java.io.Serializable;
import java.rmi.Remote;
import java.util.Iterator;
import java.util.Set;
/**
 * internal use to pass keys through rmi
 */
public class ReducerKeys implements Remote, Serializable {
    private String[] keys;

    public ReducerKeys(Set<String> keys) {
        this.keys = new String[keys.size()];
        Iterator<String> itr = keys.iterator();
        for (int i = 0; i < keys.size() && itr.hasNext(); i++) {
            String key = itr.next();
            this.keys[i] = key;
        }
    }

    
    /** 
     * @return String[]
     */
    public String[] getReducerKeys() {
        return keys;
    }
}
