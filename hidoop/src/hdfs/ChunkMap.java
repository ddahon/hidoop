package hdfs;

import java.util.HashMap;

// Sert à stocker l'emplacement des différents chunks d'un fichier
public class ChunkMap extends HashMap<Integer, String> {

     public ChunkMap() {
        super();
     }
}
