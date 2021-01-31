package ordo;

import java.rmi.Naming;

public class TestRMI {

    public static void main(String[] Args) {
        String host = "nickel";
        String port = "8080";
        try {
            CallBack cb = new Report();
            String localTesteur = "//" + host + ":" + port + "/testeur";
            WorkerInt testeur = (WorkerInt) Naming.lookup(localTesteur);
            testeur.runTest(0, cb);
            System.out.println(cb.getInfo().getMessage());
            System.out.println("C'EST O-KAY");
        } catch (Exception e) {}
    }
}
