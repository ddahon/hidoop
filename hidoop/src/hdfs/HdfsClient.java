/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.Type;

public class HdfsClient {

    final static String host = "localhost";
    final static int[] ports = {8081, 8082};
    final static int nbChunks = 2;

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

    public static void HdfsDelete(String hdfsFname) {
    }

    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {
        try {
            Format format;
            String fname = ""; // TODO : choisir la convention de nommage

            if (fmt == Type.KV) {
                format = new KVFormat(fname);
            } else {
                format = new LineFormat(fname);
            }

            // TODO fragmentation du fichier
            List<KV> fragments = new LinkedList<KV>();
            KV kv = format.read();
            while(kv != null) {
                fragments.add(kv);
                kv = format.read();
            } 

            // Socket utilisé pour la communication avec le serveur 1
            Socket s1 = new Socket(host, ports[0]);
            OutputStream os1 = s1.getOutputStream();
            InputStream is1 = s1.getInputStream();
            ObjectOutputStream oos1 = new ObjectOutputStream(os1);
            ObjectInputStream ois1 = new ObjectInputStream(is1);

            // Socket utilisé pour la communication avec le serveur 2
            Socket s2 = new Socket(host, ports[1]);
            OutputStream os2 = s2.getOutputStream();
            InputStream is2 = s2.getInputStream();
            ObjectOutputStream oos2 = new ObjectOutputStream(os2);
            ObjectInputStream ois2 = new ObjectInputStream(is2);

            // Fermeture des sockets et des streams
            os1.close();
            is1.close();
            os2.close();
            is2.close();
            s1.close();
            s2.close();

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
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
