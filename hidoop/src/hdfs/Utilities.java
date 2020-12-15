package hdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Utilities {
    public static long countLines(String fname) throws IOException {
        BufferedReader reader;
            reader = new BufferedReader(new FileReader(fname));
            long n = 0;
            while (reader.readLine() != null) {
                n++;
            }
            reader.close();
            return n;

   } 
}