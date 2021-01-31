package ordo;

import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;

import formats.Format;
import map.Mapper;

public class TesteurRMI extends UnicastRemoteObject implements WorkerInt {

    public TesteurRMI(String host, String port) throws AlreadyBoundException, RemoteException, MalformedURLException {
        Registry registre = LocateRegistry.createRegistry(Integer.parseInt(port));
        Naming.rebind("//" + host + ":" + port + "/testeur", this);
    }

    @Override
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
        System.out.println("ÇA MARCHE PTN");
    }

    @Override
    public void runTest(int t, CallBack cb) throws RemoteException {
        System.out.println("ÇA MARCHE PTN");
    }

    public static void main(String args[]) 
	{
		try {
            TesteurRMI testeur = new TesteurRMI(args[0], args[1]);
            System.out.println("Testeur " + args[0] + ":" + args[1] + " démarré");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    
}
