package hdfs;

import java.io.Serializable;
import java.security.InvalidParameterException;

public class Message implements Serializable {
    
    public static enum Commande {CMD_READ , CMD_WRITE , CMD_DELETE};

    private Commande commande;
    private int taillePremierNomFichier;
    private String premierNomFichier;

    public Message(Commande commande, String premierNomFichier) {
        this.taillePremierNomFichier = premierNomFichier.length();
        this.premierNomFichier = premierNomFichier;
        this.commande = commande;
    }

    public Commande getCommande() {
        return commande;
    }

    public int getTaillePremierNomFichier() {
        return taillePremierNomFichier;
    }

    public String getPremierNomFichier() {
        return premierNomFichier;
    }
}
