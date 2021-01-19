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

   public static void recevoirFichier(Socket s, Format format) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
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
}