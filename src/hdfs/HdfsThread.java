package hdfs;
import java.io.File;
import java.net.Socket;
public class HdfsThread extends Thread {
    private Socket mySocket;
    private Message message;
    public HdfsThread (Message mes,Socket s){
        this.message = mes;
        this.mySocket = s;
    }

    public void run(){
        File f = new File(this.message.getPremierNomFichier());
        f.delete();
        mySocket.close();
    }
}
