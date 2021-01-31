package ordo;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class TaskTest extends Thread {

    // Paramètres utiles afin de lancer runTest
    private WorkerInt worker;
    private CallBack cb;
    private int t;

    // CONSTRUCTEUR
    public TaskTest(int t, String host, String port, CallBack cb)
            throws MalformedURLException, RemoteException, NotBoundException {
        
        String localWorker = "//" + host + ":" + port + "/worker";

    	worker = (WorkerInt) Naming.lookup(localWorker);
        this.cb = cb;
        this.t = t;
    }

    // RUN : lancement du thread
    public void run() {
        try {
            this.worker.runTest(t, cb);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
