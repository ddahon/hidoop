package hdfs;

import java.io.Serializable;
import java.security.InvalidParameterException;

public class Message implements Serializable {
    
    public static enum Commande {CMD_READ , CMD_WRITE , CMD_DELETE, CMD_SHUTDOWN};

    private Commande commande;
    private String premierNomFichier;

    public Message(Commande commande, String premierNomFichier) {
        this.premierNomFichier = premierNomFichier;
        this.commande = commande;
    }

    public Commande getCommande() {
        return commande;
    }

    public String getPremierNomFichier() {
        return premierNomFichier;
    }
}
