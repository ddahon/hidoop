
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
import hdfs.Message.Commande;

public class HdfsServer {
    final static int ports[] = {8081,8082};
    final static int nb = 2;
    final static int nbChunks = 1;
    final static int tailleMaxEnvoi = 10;   
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
                        System.out.println("lecture");
                        Format formatR = new KVFormat(message.getPremierNomFichier());
                        formatR.open(Format.OpenMode.R);

                        // On envoie le chunk par morceaux
                        long nbLignes = Utilities.countLines(formatR.getFname());
                        long tailleChunk = nbLignes / nbChunks;
                        int nbLignesRestantes = (int) nbLignes % nbChunks;
                        int nbEnvoi = (int) Math.max(1, tailleChunk/tailleMaxEnvoi);
                        long tailleEnvoi = Math.min(tailleChunk, tailleMaxEnvoi);

                        Message messageContinue = new Message(Commande.CMD_WRITE, "continue");
                        for (int envoi = 0; envoi<nbEnvoi; envoi++) {
                            oos.writeObject(messageContinue);
                            Chunk morceauAEnvoyer = new Chunk();
                            for (int j = 0; j<tailleEnvoi; j++) {
                                KV kv = formatR.read();
                                morceauAEnvoyer.add(new KVS(kv.k, kv.v));
                            }
                            oos.writeObject(morceauAEnvoyer);
                            System.out.println("Morceau envoye");
                        }
                        Message messageFin = new Message(Commande.CMD_WRITE, "FIN");
                        oos.writeObject(messageFin);
                        formatR.close();
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