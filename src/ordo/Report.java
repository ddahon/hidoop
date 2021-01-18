package ordo;

import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class Report extends UnicastRemoteObject implements CallBack {

    private Info info;
    private Semaphore infoAccess;
    private Semaphore infoWritten;
    private Semaphore done;

    protected Report() throws RemoteException {
        super();
        info = new Info("");
        infoAccess = new Semaphore(1);
        infoWritten = new Semaphore(0);
        done = new Semaphore(0);
    }

    @Override
    public void start() throws RemoteException, InterruptedException {
        infoAccess.acquire();
    }

    @Override
    public void finish() throws RemoteException {
        done.release();
        infoAccess.release();
    }

    @Override
    public void isDone() throws RemoteException, InterruptedException {
        done.acquire();
        done.release();
    }

    @Override
    public void setInfo(Info info) throws RemoteException, InterruptedException {
        infoAccess.acquire();
        this.info = info;
        infoAccess.release();
        infoWritten.release();
    }

    @Override
    public Info getInfo() throws RemoteException, InterruptedException {
        infoWritten.acquire();
        infoAccess.acquire();
        Info report = info;
        infoAccess.release();
        infoWritten.release();
        return report;
    }
    
}
