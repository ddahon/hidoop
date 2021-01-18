package ordo;

import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import formats.Format;
import map.Mapper;

public class Worker extends UnicastRemoteObject implements WorkerInt {

    private String host;
    private String port;

    public Worker(String host, String port) throws RemoteException, MalformedURLException {
        this.host = host;
        this.port = port;
        LocateRegistry.createRegistry(Integer.parseInt(port));
        Naming.rebind("//" + host + ":" + port + "/worker", this);
    }

    @Override
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
        try {
            cb.start();
            reader.open(Format.OpenMode.R);
            writer.open(Format.OpenMode.W);
            m.map(reader, writer);
            reader.close();
            writer.close();
            cb.finish();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void runTest(int t, CallBack cb) throws RemoteException {
        try {
            System.out.println("Je commence mon travail..");
            cb.start();
            Thread.sleep(t*1000);
            cb.finish();
            cb.setInfo(new Info("worker " + host + ":" + port + " > OK"));
            System.out.println("J'ai fini !");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) 
	{
		try {
            Worker worker = new Worker(args[0], args[1]);
            System.out.println("Worker " + args[0] + ":" + args[1] + " démarré");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    
}
