package hdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.EOFException;

import java.io.File;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.naming.CommunicationException;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.KVS;
import formats.LineFormat;
import formats.Format.Type;
import hdfs.Message.Commande;
import hdfs.Message.*;
public class Utilities {

    final static String[] hosts = {"localhost", "localhost"};
    final static int[] ports = { 8081, 8082 };
    final static int nbChunks = 2;
    final static int tailleMaxEnvoi = 10;
    
    public static long countLines(String fname) throws IOException {
        BufferedReader reader;
            reader = new BufferedReader(new FileReader(fname));
            long n = 0;
            while (reader.readLine() != null) {
                n++;
            }
            reader.close();
            return n;

   }

   public static void envoyer_fichier( Format format, ObjectOutputStream oos) {
        format.open(Format.OpenMode.R);
        String fname = format.getFname();

        // Lecture du fichier
        long nbLignes;
        try {
            nbLignes = Utilities.countLines(fname);
            long tailleChunk = nbLignes / nbChunks;
            int nbLignesRestantes = (int) nbLignes % nbChunks;
            int nbEnvoi = (int) Math.max(1, tailleChunk/tailleMaxEnvoi);
            long tailleEnvoi = Math.min(tailleChunk, tailleMaxEnvoi);
            System.out.println("Nombre de lignes : " + nbLignes);

            // On traite les chunks l'un après l'autre
            for (int numeroChunk = 0; numeroChunk < nbChunks; numeroChunk++) {

                // On préfixe le nom du fichier par le numéro de chunk
                String[] cheminDecoupe = fname.split("/");
                String HdfsFname = numeroChunk + cheminDecoupe[cheminDecoupe.length - 1];

                // Message pour initialiser la communication
                Message messageDebut = new Message(Commande.CMD_WRITE, HdfsFname);
                oos.writeObject(messageDebut);
                // On envoie le chunk par morceaux
                for (int envoi = 0; envoi<nbEnvoi; envoi++) {
                    Message messageContinue = new Message(Commande.CMD_WRITE, "continue");

                    Chunk morceauAEnvoyer = new Chunk();
                    for (long i = 0; i<tailleEnvoi; i++) {
                        KV kv = format.read();
                        morceauAEnvoyer.add(new KVS(kv.k, kv.v));
                    }
                    oos.writeObject(messageContinue);
                    oos.writeObject(morceauAEnvoyer);
                    System.out.println("Morceau envoye");
                }
                // On met les éventuelles lignes restantes dans le dernier chunk
                if (numeroChunk == nbChunks-1 && nbLignesRestantes > 0) {
                    Chunk lignesRestantes = new Chunk();
                    for (int i = 0; i<nbLignesRestantes; i++) {
                        KV kv = format.read();
                        lignesRestantes.add(new KVS(kv.k, kv.v));
                    }
                    oos.writeObject(messageDebut);
                    oos.writeObject(lignesRestantes);
                    System.out.println("Lignes restantes envoyées");
                }
                // Message de fin
                Message messageContinue = new Message(Commande.CMD_WRITE, "fin");
                oos.writeObject(messageContinue);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
   }

   public static void recevoirFichier(Socket s, ObjectOutputStream oos, String localFSDestFname) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(s.getInputStream());

        Format format = new KVFormat(localFSDestFname);
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
            oos.close();
            ois.close();
        }
    }
}