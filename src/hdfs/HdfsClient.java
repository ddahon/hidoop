/*
@author Doryan Dahon @ddahon
Cette classe permet de communiquer avec les HdfsServer
Ses méthodes peuvent être appelées directement ou bien via l'interface en ligne de commande
*/

package hdfs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import javax.naming.CommunicationException;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.KVS;
import formats.LineFormat;
import formats.Format.Type;
import hdfs.Message.Commande;
import config.Project;

public class HdfsClient {

    final static int tailleMaxEnvoi = Project.tailleMaxEnvoi;

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <hdfsFname> <localFSDestFname>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

    /* Supprime le fichier hdfsFname du système HDFS 
    */
    public static void HdfsDelete(String hdfsFname) {
        for (int numeroChunk = 0; numeroChunk<Project.nbNodes; numeroChunk++) {
            try {
                Socket s = new Socket(Project.hosts[numeroChunk], Integer.parseInt(Project.ports[numeroChunk]));
                ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                Message messageDelete = new Message(Commande.CMD_DELETE, numeroChunk+hdfsFname);
                oos.writeObject(messageDelete);
                s.close();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /* 
    Ecrit un fichier dans le système HDFS 
    Le fichier est lu localement depuis localFSSourceFname au format fmt
    */
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
            long nbLignes = Utilities.countLines(fname);
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

        format.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
      }

    /*
    Lit un fichier stocké dans le système HDFS
    Le nom du fichier à lire est hdfsFname
    Après lecture, il est stocké localement dans le fichier localFSDestFname
    */
    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
        try {
            Format format = new KVFormat(localFSDestFname);
            format.open(Format.OpenMode.W);

            for (int numeroChunk = 0; numeroChunk < Project.nbNodes; numeroChunk++) {
                Socket s = new Socket(Project.hosts[numeroChunk], Integer.parseInt(Project.ports[numeroChunk]));
                ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());

                Message messageDebut = new Message(Commande.CMD_READ, numeroChunk + hdfsFname);
                oos.writeObject(messageDebut);
                
                // Réception et écriture du fichier dans le FS local
                Utilities.recevoirFichier(s, format);
                
                s.close();
            }
            format.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
	
    public static void main(String[] args) {
        // java HdfsClient write <line|kv> <file>
        // java HdfsClient read <hdfsFname> <localFSDestFname>

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1], args[2]); break;
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
