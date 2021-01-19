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

import config.Project;
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
    
    /**
     * Compte les lignes d'un fichier
     * @param fname le fichier
     * @return nombre de lignes du fichier
    */
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

   /**
     * Reçoit un fichier. Peut être utilisé par le serveur et par le client.
     * Le programme écrit tous les Chunks recus tant qu'il recoit des messages "continue"
     * @param ois stream par lequel recevoir les données
     * @param format le format correspondant au fichier à recevoir
    */
   public static void recevoirFichier(ObjectInputStream ois, Format format) throws IOException, ClassNotFoundException {
        while (((Message) ois.readObject()).getPremierNomFichier().equals("continue")) {
            // Réception du chunk
            Chunk chunk = new Chunk();
            try {
                chunk = (Chunk) ois.readObject();
            } catch (EOFException e) {
            System.out.println("Morceau reçu");
            }
            System.out.println("Nombre de lignes du fichier reçu : " + chunk.size());

            for (KVS kvs : chunk) {
                format.write(new KV(kvs.k, kvs.v));
            }
        }
    }

    /**
     * Envoie un fichier local aux serveurs Hdfs
     * @param fname le fichier local à envoyer
     * @param format le format crrespondant au fichier à envoyer
    */
    public static void envoyerFichierAuServeur(String fname, Format format) throws IOException {
        long nbLignes = countLines(fname);
        long tailleChunk = nbLignes / Project.nbNodes;
        long nbLignesRestantes = nbLignes % Project.nbNodes;
        int nbEnvoi = (int) Math.max(1, tailleChunk/tailleMaxEnvoi); // Nombre d'envois pour 1 chunk
        long tailleEnvoi = Math.min(tailleChunk, tailleMaxEnvoi);   // Taille d'un envoi pour 1 chunk
        long resteChunk = tailleChunk % tailleEnvoi;    // Dernières lignes du chunk à envoyer
        System.out.println("Nombre de lignes à envoyer : " + nbLignes);

        // On traite les chunks l'un après l'autre
        for (int numeroChunk = 0; numeroChunk < Project.nbNodes; numeroChunk++) {

            // On préfixe le nom du fichier par le numéro de chunk
            String[] cheminDecoupe = fname.split("/");
            String hdfsFname = numeroChunk + cheminDecoupe[cheminDecoupe.length - 1];

            Socket s = new Socket(Project.hosts[numeroChunk], Integer.parseInt(Project.ports[numeroChunk]));
            ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());

            Message messageDebut = new Message(Commande.CMD_WRITE, hdfsFname);
            oos.writeObject(messageDebut);
            Message messageContinue = new Message(Commande.CMD_WRITE, "continue");

            // On envoie le chunk par morceaux
            for (int envoi = 0; envoi<nbEnvoi; envoi++) {
                Chunk morceauAEnvoyer = new Chunk();
                for (long i = 0; i<tailleEnvoi; i++) {
                    KV kv = format.read();
                    morceauAEnvoyer.add(new KVS(kv.k, kv.v));
                }
                oos.writeObject(messageContinue);
                oos.writeObject(morceauAEnvoyer);
                System.out.println("Morceau envoyé");
            }

            // Envoi des dernières lignes du chunk si la taille d'un chunk n'était pas divisible par la taille des envois 
            if (resteChunk>0) {
                Chunk lignesRestantes = new Chunk();
                for (int i = 0; i<resteChunk; i++) {
                    KV kv = format.read();
                    lignesRestantes.add(new KVS(kv.k, kv.v));
                }
                oos.writeObject(messageContinue);
                oos.writeObject(lignesRestantes);
                System.out.println("Lignes restantes du chunk envoyées");
            }

            // On met les éventuelles lignes restantes dans le dernier chunk
            if (numeroChunk == Project.nbNodes-1 && nbLignesRestantes > 0) {
                Chunk lignesRestantes = new Chunk();
                for (int i = 0; i<nbLignesRestantes; i++) {
                    KV kv = format.read();
                    lignesRestantes.add(new KVS(kv.k, kv.v));
                }
                oos.writeObject(messageContinue);
                oos.writeObject(lignesRestantes);
                System.out.println("Lignes restantes envoyées");
            }

            Message messageFin = new Message(Commande.CMD_WRITE, "fin");
            oos.writeObject(messageFin);

            s.close();
            oos.close();
        }
    }

    /**
     * Envoie un fichier local du serveur au client
     * @param fname le fichier local à envoyer
     * @param format le format correspondant au fichier à envoyer
     * @param oos stream par lequel envoyer le fichier
    */
    public static void envoyerFichierAuClient(String fname, Format format, ObjectOutputStream oos) throws IOException {
        // On envoie le chunk par morceaux
        long nbLignes = Utilities.countLines(format.getFname());
        int nbEnvoi = (int) Math.max(1, nbLignes/tailleMaxEnvoi); 
        long tailleEnvoi = Math.min(nbLignes, tailleMaxEnvoi); 
        int reste = (int) nbLignes % (int) tailleEnvoi;    // Dernières lignes à envoyer

        Message messageContinue = new Message(Commande.CMD_WRITE, "continue");
        for (int envoi = 0; envoi<nbEnvoi; envoi++) {
            oos.writeObject(messageContinue);
            Chunk morceauAEnvoyer = new Chunk();
            for (int j = 0; j<tailleEnvoi; j++) {
                KV kv = format.read();
                morceauAEnvoyer.add(new KVS(kv.k, kv.v));
            }
            oos.writeObject(morceauAEnvoyer);
            System.out.println("Morceau envoye");
        }

        // Envoi des dernières lignes si la taille du fichier n'était pas divisible par la taille d'un envoi
        if (reste>0) {
            Chunk lignesRestantes = new Chunk();
            for (int i = 0; i<reste; i++) {
                KV kv = format.read();
                lignesRestantes.add(new KVS(kv.k, kv.v));
            }
            oos.writeObject(messageContinue);
            oos.writeObject(lignesRestantes);
            System.out.println("Lignes restantes envoyées");
        }

        Message messageFin = new Message(Commande.CMD_WRITE, "FIN");
        oos.writeObject(messageFin);
    }
}