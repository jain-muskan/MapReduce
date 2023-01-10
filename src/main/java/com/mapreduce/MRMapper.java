package com.mapreduce;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;

//Interface of Mapper class which user will implement
public interface MRMapper {
    HashMap<String, List<String>> map(String k, String v) throws RemoteException;
}
