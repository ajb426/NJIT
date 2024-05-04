import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class FindTopThree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        Path inFile = new Path(args[0]);
        Path outFile = new Path(args[1]);

        TreeMap<Double, String> map = new TreeMap<>(Collections.reverseOrder());
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\t");
                if (parts.length == 2) {
                    double probability = Double.parseDouble(parts[1]);
                    map.put(probability, parts[0]);
                    if (map.size() > 3) {
                        map.remove(map.lastKey());
                    }
                }
            }
        }

        // Output the top 3 to a file
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outFile, true)))) {
            for (Map.Entry<Double, String> entry : map.entrySet()) {
                bw.write(entry.getValue() + "\t" + entry.getKey() + "\n");
            }
        }
    }
}
