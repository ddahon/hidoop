package hdfs;
import java.io.File;
public class HdfsThread extends Thread {
    
    private Message message;
    public HdfsThread (Message mes){
        this.message = mes;
    }

    public void run(){
        File f = new File(this.message.getPremierNomFichier());
        f.delete();
    }
}
