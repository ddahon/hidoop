
package hdfs;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

import config.Project;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.KVS;
import formats.Format.OpenMode;
import hdfs.HdfsThread;
import hdfs.Message.Commande;

public class HdfsServer {
    private int port;
    final static int tailleMaxEnvoi = Project.tailleMaxEnvoi;  
    
    public static void usage() {
        System.out.println("Usage : java HdfsServer <port>");
    }

    public HdfsServer(int port) {
        this.port = port;
        this.launch();
    }

    public void launch() {
        try {
            ServerSocket ss = new ServerSocket(this.port);

            while (true) {
                System.out.println("Listening on port " + this.port);
                Socket s = ss.accept();
                System.out.println("Connexion établie avec "+s.getInetAddress());
                ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(s.getInputStream());

                Message message = (Message) ois.readObject();
                System.out.println("Réception de la commande : " + message.getCommande() + " " + message.getPremierNomFichier());
                switch (message.getCommande()) {
                    case CMD_READ:  
                        Format formatR = new KVFormat(message.getPremierNomFichier());
                        formatR.open(Format.OpenMode.R);
                        Utilities.envoyerFichierAuClient(message.getPremierNomFichier(), formatR, oos);
                        formatR.close();
                        s.close();
                        break;
                    case CMD_WRITE:
                        Format formatW = new KVFormat(message.getPremierNomFichier());
                        formatW.open(OpenMode.W);
                        Utilities.recevoirFichier(ois, formatW);
                        formatW.close();
                        s.close();
                        break;
                    case CMD_DELETE:
                        File f = new File(message.getPremierNomFichier());
                        f.delete();
                        s.close();
                        break;
                 
                }
                System.out.println("Connexion fermée\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        try {
            new HdfsServer(Integer.parseInt(args[0])).launch();
        } catch(Exception e) {
            usage();
            return;
        }
        
    }
}