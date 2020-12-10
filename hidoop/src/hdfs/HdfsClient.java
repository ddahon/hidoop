/* 
 */

package hdfs;

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
import formats.LineFormat;
import formats.Format.Type;

public class HdfsClient {

    final static String host = "localhost";
    final static int[] ports = { 8081, 8082 };
    final static int nbChunks = 2;

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

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

            // Lecture du fichier par fragments
            LinkedList<KV> fragments = new LinkedList<KV>();
            KV kv = format.read();
            int nbFragments = 0;

            while (kv != null) {
                nbFragments++;
                fragments.add(kv);
                kv = format.read();
            }

            int tailleChunk = nbFragments / nbChunks;
            LinkedList<LinkedList<KV>> chunks = new LinkedList<LinkedList<KV>>();

            // Création de chaque chunk
            for (int numeroChunk = 0; numeroChunk < nbChunks; numeroChunk++) {
                LinkedList<KV> chunk = new LinkedList<KV>();
                // Remplissage des chunks
                for (int i = 0; i < tailleChunk; i++) {
                    chunk.add(fragments.remove());
                }
                chunks.add(chunk);
            }

            // On rajoute les éventuels fragments restants dans le dernier chunk
            while (!fragments.isEmpty()) {
                chunks.getLast().add(fragments.remove());
            }

            // Envoi des chunks sur les serveurs
            for (int numeroChunk = 0; numeroChunk < nbChunks; numeroChunk++) {
                // Ouverture des sockets
                Socket s = new Socket(host, ports[numeroChunk]);
                OutputStream os = s.getOutputStream();
                InputStream is = s.getInputStream();
                ObjectOutputStream oos = new ObjectOutputStream(os);
                ObjectInputStream ois = new ObjectInputStream(is);

                // Envoi du message
                Message message = new Message("write", fname + Integer.toString(numeroChunk));
                oos.writeObject(message);

                // Attente de l'accusé de réception
                String reponse = (String) ois.readObject();
                try {
                    if (reponse != "ok") { 
                        throw new CommunicationException("Communication entre HdfsClient et HdfsServer échouée"); 
                    }
            
                // Envoi du chunk
                oos.writeObject(chunks.get(numeroChunk));

                } catch (CommunicationException e) {
                    System.out.println(e.getExplanation());
                } finally {
                    // Fermeture des sockets
                    s.close();
                    os.close();
                    is.close();
                    oos.close();
                    ois.close();
                }
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } 
      }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) {

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
