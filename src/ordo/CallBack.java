package ordo;
import java.rmi.*;

public interface CallBack extends Remote{

    public void start() throws RemoteException, InterruptedException;

    public void finish() throws RemoteException;

    public void isDone() throws RemoteException, InterruptedException;

    public void setInfo(Info info) throws RemoteException, InterruptedException;

    public Info getInfo() throws RemoteException, InterruptedException;
}
