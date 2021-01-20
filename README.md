# hidoop
Simplified version of Hadoop

## Utilisation
0. Se placer à la racine du projet
1. Lancer la commande 'make' pour compiler le projet dans le dossier bin
2. Changer le fichier config/Project.java
3. Executer le script authenticate ('./authenticate <userNameN7>') pour pouvoir lancer les serveurs automatiquement sans avoir à rentrer le mot de passe de ssh
4. Lancer les serveurs avec 'java -cp bin/ config.LanceurServeurs'
5. Executer les commandes avec 'java -cp bin/ package.Classe <args>' (ex: 'java -cp bin/ hdfs.HdfsClient write line data/filesample.txt')