import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class FindBottomThreeTaxiTimes {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FindBottomThreeTaxiTimes <input path> <output path>");
            System.exit(-1);
        }

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
                    String airportCode = parts[0];
                    double taxiTime = Double.parseDouble(parts[1]);
                    entries.add(new AbstractMap.SimpleEntry<>(airportCode, taxiTime));
                }
            }
        }

        // Sort entries by taxi time values in ascending order
        entries.sort(Comparator.comparingDouble(Map.Entry::getValue));

        // Write the bottom 3 entries to the output file
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outFile, true)))) {
            int count = 0;
            for (Map.Entry<String, Double> entry : entries) {
                if (count < 3) {
                    bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
                    count++;
                } else {
                    break;
                }
            }
        }
    }
}
