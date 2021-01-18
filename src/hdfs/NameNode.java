package hdfs;

import java.util.HashMap;

// Serveur permettant la liaison entre les noms des fichiers du FS local et ceux sur HDFS
// La liaison est effectuée avec un système de catalogue
public class NameNode {
    // Permet d'obtenir la ChunkMap d'un fichier à partir de son nom
    private HashMap<String, ChunkMap> catalogue = new HashMap<String, ChunkMap>();

    public NameNode() {}
}
