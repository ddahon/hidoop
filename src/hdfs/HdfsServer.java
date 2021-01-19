
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

import config.Project;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.KVS;
import formats.Format.OpenMode;
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

                        // On envoie le chunk par morceaux
                        long nbLignes = Utilities.countLines(formatR.getFname());
                        int nbEnvoi = (int) Math.max(1, nbLignes/tailleMaxEnvoi); 
                        long tailleEnvoi = Math.min(nbLignes, tailleMaxEnvoi); 
                        int reste = (int) nbLignes % (int) tailleEnvoi;    // Dernières lignes à envoyer

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

                        // Envoi des dernières lignes si la taille du fichier n'était pas divisible par la taille d'un envoi
                        if (reste>0) {
                            Chunk lignesRestantes = new Chunk();
                            for (int i = 0; i<reste; i++) {
                                KV kv = formatR.read();
                                lignesRestantes.add(new KVS(kv.k, kv.v));
                            }
                            oos.writeObject(messageContinue);
                            oos.writeObject(lignesRestantes);
                            System.out.println("Lignes restantes envoyées");
                        }

                        Message messageFin = new Message(Commande.CMD_WRITE, "FIN");
                        oos.writeObject(messageFin);
                        formatR.close();
                        s.close();
                        break;
                    case CMD_WRITE:
                        Format format = new KVFormat(message.getPremierNomFichier());
                        format.open(OpenMode.W);
                        Utilities.recevoirFichier(ois, format);
                        format.close();
                        s.close();
                        break;
                    case CMD_DELETE:
                        
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