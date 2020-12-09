package hdfs;

import java.io.Serializable;
import java.security.InvalidParameterException;

public class Message implements Serializable {
    
    public static enum Commande {CMD_READ , CMD_WRITE , CMD_DELETE};

    private Commande commande;
    private int taillePremierNomFichier;
    private String premierNomFichier;

    public Message(String commande, String premierNomFichier) {
        this.taillePremierNomFichier = premierNomFichier.length();
        this.premierNomFichier = premierNomFichier;
        switch (commande) {
            case "read" : 
            this.commande = Commande.CMD_READ;
            break;
            case "write" : 
            this.commande = Commande.CMD_WRITE; 
            break;
            case "delete" : 
            this.commande = Commande.CMD_DELETE;
            break;
            default:
            throw(new InvalidParameterException("Commande invalide"));
        }
        
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
