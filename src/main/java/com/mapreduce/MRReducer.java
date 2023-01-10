package com.mapreduce;

import java.rmi.RemoteException;
import java.util.List;

//Interface of Reducer class which user will implement
public interface MRReducer {
    List<String> reduce(String key, List<String> values) throws RemoteException;
}
