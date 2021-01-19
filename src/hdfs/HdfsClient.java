/*
@author Doryan Dahon @ddahon
Cette classe permet de communiquer avec les HdfsServer
Ses méthodes peuvent être appelées directement ou bien via l'interface en ligne de commande
*/

package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
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

    /** 
     * Supprime un fichier du système HDFS 
     * @param hdfsFname le fichier à supprimer
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

    /** 
     * Ecrit un fichier dans le système HDFS 
     * Le fichier est lu localement depuis localFSSourceFname au format fmt
     * Il est découpé en chunks puis envoyé aux différents serveurs par morceaux de chunk
     * Le fichier est stocké dans le FS local des serveurs par la convention numeroDeChunk+localFSSourceFname
     * @param fmt le type (LINE ou KV) du fichier à envoyer
     * @param localFSSourceFname le fichier du FS local à envoyer
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
            Utilities.envoyerFichierAuServeur(localFSSourceFname, format);
            format.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Lit un fichier stocké dans le système HDFS
     * @param hdfsFname nom du fichier à lire
     * @param localFSDestFname destination du fichier dans le FS local
    */
    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
        try {
            Format format = new KVFormat(localFSDestFname);
            format.open(Format.OpenMode.W);

            for (int numeroChunk = 0; numeroChunk < Project.nbNodes; numeroChunk++) {
                Socket s = new Socket(Project.hosts[numeroChunk], Integer.parseInt(Project.ports[numeroChunk]));
                ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
                Message messageDebut = new Message(Commande.CMD_READ, numeroChunk + hdfsFname);
                oos.writeObject(messageDebut);
                
                // Réception et écriture du fichier dans le FS local
                Utilities.recevoirFichier(ois, format);
                
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
