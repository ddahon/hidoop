package config;

import java.io.File;
import java.io.IOException;

public class Lanceur {

    private static void usage() {
        System.out.println("Usage: java Lanceur");
        System.out.println("La config se trouve dans config/Project.java");
    }

    public static void main(String[] args) {
        try {
            // Lancer les serveurs sur les machines distantes
            for (int i=0; i<Project.nbNodes; i++) {
                String sshArgs = "ddahon@" + Project.hosts[i] + ".enseeiht.fr cd "+Project.serverPathToHidoop+";java -cp bin/ hdfs.HdfsServer " + Project.ports[i];
                ProcessBuilder processBuilder = new ProcessBuilder("ssh", sshArgs);
                Process process = processBuilder.start();
                int ret = process.waitFor();
                System.out.println(ret);
            }
        } catch (IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
