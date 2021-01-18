package ordo;

import config.*;

public class Tester {
    
    public static void main(String[] Args) {
        int nbNodes = Project.nbNodes;
        String[] hosts = Project.hosts;
        String[] ports = Project.ports;
        int t = 5;

        try {
            long start = System.currentTimeMillis();

            System.out.println("\nDEPART DU TEST\n ...");

            CallBack[] cb = new CallBack[nbNodes];

            // Ensemble des threads qui vont lancer les Workers
    		Thread[] tasks = new Thread[nbNodes]; 
    		for (int i=0; i < nbNodes; i++) {
                cb[i] = new Report();
    			tasks[i] = new Thread(new TaskTest(t, hosts[i], ports[i], cb[i]));
            }
            
            System.out.println("Fin des préparatifs - t = " + chrono(start) + " ms");
    		
            // On lance les threads pour le Mappage
            System.out.println("\nEXECUTION DES DIFFERENTS THREADS");

            long opStart = System.currentTimeMillis();
    		for (int i=0; i < nbNodes; i++) {
                System.out.println("Lancement du thread [" + (i+1) + "] - t = " + chrono(start) + " ms");
    			tasks[i].start();
    		}
    		
            // on attend le callback de tous les workers pour mettre fin au Mappage
    		for (int i=0; i < nbNodes; i++) {
                cb[i].isDone();
                System.out.println("Terminaison du thread [" + (i+1) + "] - t = " + chrono(start) + " ms");
            }
            long opTime = chrono(opStart);
            
            // On affiche les callbacks
            System.out.println("\nRESULTATS DES DIFFERENTS WORKERS :");
            for (int i=0; i < nbNodes; i++) {
                System.out.println(cb[i].getInfo().getMessage());
    		}
            
            // Résumé
            System.out.println("\nRESUME DU TEST :");
            System.out.println("Temps d'exécution des threads : " + opTime + " ms (temps d'exécution d'un thread : " + t*1000 + " ms)");
            System.out.println("Temps d'exécution TOTAL : " + chrono(start) + " ms");

        } catch(Exception e){
            e.printStackTrace();
        }
    }

    private static long chrono(long start) {
        return System.currentTimeMillis() - start;
    }
}
