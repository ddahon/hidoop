package outils;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class CreerFichier {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage : java CreerFichier <taille>");
        } else {
            long taille = Long.parseLong(args[0]);
            try {
                BufferedWriter bw = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream("data/fichier_genere_" + taille)));
                for (int i = 0; i < taille; i++) {
                    bw.write(Integer.toString(i), 0, 1);
                    bw.newLine();
                    bw.flush();
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
