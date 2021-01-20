package config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class LanceurServeurs {

    private static void usage() {
        System.out.println("Usage: java Lanceur");
        System.out.println("La config se trouve dans config/Project.java");
    }

    public static boolean serveursNonTermines(Process[] serveurs) {
        boolean nonTermine = false;
        for (Process process : serveurs) {
            if (process.isAlive()){ nonTermine = true; }
        }
        return nonTermine;
    }

    public static void main(String[] args) {
        try {
            // Lancer les serveurs sur les machines distantes
            Process[] serveurs = new Process[Project.nbNodes];
            BufferedReader readers[];
            for (int i=0; i<Project.nbNodes; i++) {
                /*String sshArgs = "ddahon@" + Project.hosts[i] + ".enseeiht.fr cd "+Project.serverPathToHidoop+";java -cp bin/ hdfs.HdfsServer " + Project.ports[i];
                ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", "ssh", sshArgs);
                Process process = processBuilder.start();*/
                ProcessBuilder pb = new ProcessBuilder("ssh", 
                                       Project.username+"@"+Project.hosts[i]+".enseeiht.fr", 
                                       "cd "+Project.serverPathToHidoop+";java -cp bin/ hdfs.HdfsServer "+Project.ports[i]);
                pb.redirectErrorStream(); //redirect stderr to stdout
                pb.inheritIO();
                Process process = pb.start();
                serveurs[i] = process;
            }
            // Attente de la terminaison des serveurs
            for (int i = 0; i<Project.nbNodes; i++) {
                serveurs[i].waitFor();
            }
        } catch (IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
