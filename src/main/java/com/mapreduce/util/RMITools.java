package com.mapreduce.util;

import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import com.mapreduce.MRMapper;
import com.mapreduce.MRReducer;
import com.mapreduce.MapReduceSpecs;
import com.mapreduce.ReducerKeys;

public class RMITools {

    static Registry registry = null;

    public static void insertInRmiRegistry(Remote specs, String objKey) throws Exception {
        // if the RMI registry doesn't already exist, then create it
        if (registry == null) {
            LocateRegistry.createRegistry(1099);
            registry = LocateRegistry.getRegistry();
        }
        // Bind to the registry
        registry.bind(objKey, specs);
    }

    // returns user defined mapper object from RMI registry
    public static MRMapper getMapperObject(String objKey) throws Exception {
        MRMapper mapperObj = (MRMapper) Naming.lookup("rmi://localhost/" + objKey);
        return mapperObj;
    }

    // returns user defined reducer object from RMI registry
    public static MRReducer getReducerObject(String objKey) throws Exception {
        MRReducer reducerObj = (MRReducer) Naming.lookup("rmi://localhost/" + objKey);
        return reducerObj;
    }

    public static MapReduceSpecs getSpecs() throws Exception {
        MapReduceSpecs reducerObj = (MapReduceSpecs) Naming.lookup("rmi://localhost/specs");
        return reducerObj;
    }

    public static ReducerKeys getReducerKeys() throws Exception {
        ReducerKeys reducerKeys = (ReducerKeys) Naming.lookup("rmi://localhost/reducerKeys");
        return reducerKeys;
    }
}
