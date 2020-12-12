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

import formats.KV;

public class HdfsServer {
    final static int ports[] = {8081,8082};
    final static int nb = 2;
    
    public static void main(String args[]) {

        try {

            int i = Integer.parseInt(args[0]);

            ServerSocket ss = new ServerSocket(ports[i]);

            while (true) {
                System.out.println("Listening on port " + ports[i]);
                Socket s = ss.accept();

                OutputStream os = s.getOutputStream();
                InputStream is = s.getInputStream();
                ObjectOutputStream oos = new ObjectOutputStream(os);
                ObjectInputStream ois = new ObjectInputStream(is);

                // Réception du message envoyé par HdfsClient
                Message message = (Message) ois.readObject();
                System.out.println("Réception de la commande : " + message.getCommande() + " " + message.getPremierNomFichier() + " " + message.getTaillePremierNomFichier());
                
                // Envoi de l'accusé de réception
                oos.writeObject("ok");
                System.out.println("Accusé de réception envoyé");

                // Réception du chunk
                Chunk chunk = new Chunk();
                try {
                    chunk = (Chunk) ois.readObject();
                } catch (EOFException e) {
                    System.out.println("Fichier reçu");
                }
                System.out.println("Nombre de lignes du fichier reçu : " + chunk.size());
                
                // TODO ecriture du chunk dans un nouveau fichier message.getPremierNomFichier()


                os.close();
                is.close();
                s.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
