package ordo;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;

import formats.Format;
import map.Mapper;

public class TesteurRMI extends UnicastRemoteObject implements WorkerInt {

    public TesteurRMI(String host, String port) throws AlreadyBoundException, RemoteException, MalformedURLException {
    }

    @Override
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
        System.out.println("ÇA MARCHE PTN");
    }

    @Override
    public void runTest(int t, CallBack cb) throws RemoteException {
        try {
            System.out.println("ÇA MARCHE PTN");
            cb.setInfo(new Info("Le message est bien arrivé"));
        } catch (Exception e) {}
    }

    public static void main(String args[]) 
	{
		try {
            Registry registre = LocateRegistry.createRegistry(Integer.parseInt(args[1]));
            TesteurRMI testeur = new TesteurRMI(args[0], args[1]);
            Naming.rebind("rmi://" + args[0] + "/testeur", testeur);
            System.out.println("Testeur " + args[0] + ":" + args[1] + " démarré");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    
}
