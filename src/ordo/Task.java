package ordo;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import formats.*;
import map.*;

public class Task extends Thread {

    // Paramètres utiles afin de lancer runMap
    private Format formatReader;
    private Format formatWriter;
    private WorkerInt worker;
    private MapReduce mapReduce;
    private CallBack cb;

    // CONSTRUCTEUR
    public Task(String host, String port, MapReduce mr, Format.Type type, String fName, CallBack cb)
            throws MalformedURLException, RemoteException, NotBoundException {
        
        String localWorker = "//" + host + "/worker";
        String localInput = config.Project.srcPATH + fName;
        String localOutput = config.Project.destPATH + fName;

    	worker = (WorkerInt) Naming.lookup(localWorker);
        mapReduce = mr;
        if(type == Format.Type.KV) {
            formatReader = new KVFormat(localInput);
        }
        else {
            formatReader = new LineFormat(localInput);
        }
        this.formatWriter = new KVFormat(localOutput);
        this.cb = cb;
    }

    // RUN : lancement du thread
    public void run() {
        try {
            this.worker.runMap(mapReduce, formatReader, formatWriter, cb);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
