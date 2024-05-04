import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class FindBottomThreeOnTime {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        Path inFile = new Path(args[0]);
        Path outFile = new Path(args[1]);

        List<Map.Entry<String, Double>> entries = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length == 2) {
                    double probability = Double.parseDouble(parts[1]);
                    entries.add(new AbstractMap.SimpleEntry<>(parts[0], probability));
                    System.out.println("Read entry: " + parts[0] + " with probability " + parts[1]);
                }
            }
        }

        // Sort entries by probability values
        entries.sort(Comparator.comparingDouble(Map.Entry::getValue));

        // Debug: print all entries to check correct ordering
        System.out.println("All sorted entries:");
        for (Map.Entry<String, Double> entry : entries) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }

        // Write the bottom 3 to a file
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outFile, true)))) {
            int count = 0;
            for (Map.Entry<String, Double> entry : entries) {
                if (count < 3) {
                    bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
                    System.out.println("Writing to file: " + entry.getKey() + "\t" + entry.getValue());
                    count++;
                } else {
                    break;
                }
            }
        }
    }
}
