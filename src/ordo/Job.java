package ordo;

import java.io.*;
import java.net.*;
import java.rmi.*;

import formats.*;
import map.MapReduce;
import java.util.*;

import hdfs.*;


public class Job implements JobInterface { 

    // ATTRIBUTS

    private int nbNodes;
    private String[] hosts;
    private String[] ports;
    private Format.Type type;
    private String fname;

    // SETTERS

    public void setInputFormat(Format.Type type) {
        this.type = type;
    }

    public void setInputFname(String fname) {
        this.fname = fname;
    }

    public void setNodesInfos() {
        nbNodes = config.Project.nbNodes;
        hosts = config.Project.hosts;
        ports = config.Project.ports;
    }

    // MAIN

    /** 
     * On lance les Map en appelant runMap() avec les Workers
     */
    public void startJob(MapReduce mapReduce) {
        try {
            CallBack[] cb = new CallBack[nbNodes];

            // Ensemble des threads qui vont lancer les Workers
    		Thread[] tasks = new Thread[nbNodes]; 
    		for (int i=0; i < nbNodes; i++) {
                cb[i] = new Report();
    			tasks[i] = new Thread(new Task(hosts[i], ports[i], mapReduce, type, i+fname, cb[i]));
    		}
    		
    		// On lance les threads pour le Mappage
    		for (int i=0; i < nbNodes; i++) {
    			tasks[i].start();
    		}
    		
            // on attend le callback de tous les workers pour mettre fin au Mappage
    		for (int i=0; i < nbNodes; i++) {
                cb[i].isDone();
    		}
            
            // On concatène les fragments avec HTFS
            HdfsClient.HdfsRead(fname, fname + "-concat");
            
            // On lance Reduce
            Format formatReader = new KVFormat(fname + "-concat");
            Format formatWriter = new KVFormat(fname + "-res");
            formatReader.open(Format.OpenMode.R);
            formatWriter.open(Format.OpenMode.W);
            mapReduce.reduce(formatReader, formatWriter);
            formatReader.close();
            formatWriter.close();

        } catch(Exception e){
            e.printStackTrace();
        }
    }
    
    
}