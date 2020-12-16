/* 
 */

package hdfs;

import java.io.File;
import java.io.IOException;
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

public class HdfsClient {

    final static String[] hosts = {"localhost", "localhost"};
    final static int[] ports = { 8081, 8082 };
    final static int nbChunks = 2;
    final static int tailleMaxEnvoi = 10;

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

    //TODO 
    public static void HdfsDelete(String hdfsFname) {
    }

    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor)
            throws CommunicationException {
        try {
            Format format;
            String fname = localFSSourceFname;
            if (fmt == Type.KV) {
                format = new KVFormat(fname);
            } else {
                format = new LineFormat(fname);
            }
            format.open(Format.OpenMode.R);

            // Lecture du fichier
            long nbLignes = Utilities.countLines(fname);
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

                // Ouverture des sockets
                Socket s = new Socket(hosts[numeroChunk], ports[numeroChunk]);
                OutputStream os = s.getOutputStream();
                InputStream is = s.getInputStream();
                ObjectOutputStream oos = new ObjectOutputStream(os);
                ObjectInputStream ois = new ObjectInputStream(is);

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

                // Fermeture des sockets
                s.close();
                os.close();
                is.close();
                oos.close();
                ois.close();
            }

            format.close();

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
      }

      // TODO
      public static void HdfsRead(String hdfsFname, String localFSDestFname) {
        try {
            Format format = new KVFormat(localFSDestFname);
            format.open(Format.OpenMode.W);

            // On traite les chunks l'un après l'autre
            for (int numeroChunk = 0; numeroChunk < nbChunks; numeroChunk++) {

                 // Ouverture des sockets
                 Socket s = new Socket(hosts[numeroChunk], ports[numeroChunk]);
                 OutputStream os = s.getOutputStream();
                 InputStream is = s.getInputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(os);
                 ObjectInputStream ois = new ObjectInputStream(is);

                // Envoi du message pour initialiser la communication
                Message messageDebut = new Message(Commande.CMD_READ, numeroChunk + hdfsFname);
                oos.writeObject(messageDebut);
                
                // Fermeture des sockets
                s.close();
                os.close();
                is.close();
                oos.close();
                ois.close();
            }

            format.close();

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1],null); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}