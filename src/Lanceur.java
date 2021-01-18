import hdfs.HdfsServer;

public class Lanceur {
    
    private static void usage() {
        System.out.println("Usage: java Lanceur <nbServeurs> <hosts> <ports>");
    }
    
    public void main(String[] args) {
        if (args.length < 1) {
            usage();
            return;
        } else {
            try {
                int nbServeurs = Integer.parseInt(args[0]);
                if (args.length-1 != 2*nbServeurs) {
                    usage();
                    return;
                } else {
                    // Lecture des hosts et ports
                    String[] hosts = new String[nbServeurs];
                    int[] ports = new int[nbServeurs];
                    for (int i=0; i<nbServeurs; i++) {
                        hosts[i] = args[i];
                        ports[i] = Integer.parseInt(args[nbServeurs + i]);
                    }
                    // Lancement des serveurs
                    HdfsServer[] serveurs = new HdfsServer[nbServeurs];
                    for (int i=0; i<nbServeurs; i++) {
                        serveurs[i] = new HdfsServer(hosts[i], ports[i]);
                        serveurs[i].launch();
                    }
                }
            } catch(NumberFormatException e) {
                usage();
            }
        }


    }
}
