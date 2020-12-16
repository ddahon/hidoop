  
package hdfs;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.KVS;
import formats.Format.OpenMode;

public class HdfsServer {
    final static int ports[] = {8081,8082};
    final static int nb = 2;
    
    public static void main(String args[]) {

        try {

            int i = Integer.parseInt(args[0]);
            System.out.println("Listening on port " + ports[i]);
            ServerSocket ss = new ServerSocket(ports[i]);

            while (true) {
                Socket s = ss.accept();
                ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(s.getInputStream());

                Message message = (Message) ois.readObject();
                System.out.println("Réception de la commande : " + message.getCommande() + " " + message.getPremierNomFichier() + " " + message.getTaillePremierNomFichier());
                switch (message.getCommande()) {
                    case CMD_READ:  
                        
                        break;
                    case CMD_WRITE:

                        System.out.println("Mode:Ecriture");
                        Format format = new KVFormat(message.getPremierNomFichier());
                        format.open(OpenMode.W);
                        Message messageContinuer = (Message) ois.readObject();
                        System.out.print(messageContinuer.getPremierNomFichier());
                        while (messageContinuer.getPremierNomFichier().equals("continue")) {
                            // Réception du chunk
                            Chunk chunk = new Chunk();
                            try {
                                System.out.println("Attente d'un morceau");
                                chunk = (Chunk) ois.readObject();
                                System.out.println("Reception d'un morceau");
                            } catch (EOFException e) {
                            System.out.println("Morceau reçu");
                            }
                            System.out.println("Nombre de lignes du fichier reçu : " + chunk.size());
                        
                            for (KVS kvs : chunk) {
                                format.write(new KV(kvs.k, kvs.v));
                            }

                            messageContinuer = (Message) ois.readObject();
                        }
                        format.close();
                        break;
                    case CMD_DELETE:
                        break;
                 
                }
                s.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}