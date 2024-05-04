import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.Map;
import java.util.AbstractMap;

public class FindTopThree {

    static class EntryComparator implements Comparator<Map.Entry<String, Double>> {
        public int compare(Map.Entry<String, Double> a, Map.Entry<String, Double> b) {
            return Double.compare(b.getValue(), a.getValue()); // Reversed to maintain the largest values
        }
    }

    public static void main(String[] args) throws Exception {
        // Configuration and file system
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputFile = new Path(args[0]);
        Path outputFile = new Path(args[1]);

        // Priority Queue to store top 3 values
        PriorityQueue<Map.Entry<String, Double>> pq = new PriorityQueue<>(3, new EntryComparator());

        // Reading the file
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputFile)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length == 2) {
                    String airportCode = parts[0];
                    double value = Double.parseDouble(parts[1]);
                    Map.Entry<String, Double> entry = new AbstractMap.SimpleEntry<>(airportCode, value);
                    if (pq.size() < 3) {
                        pq.offer(entry);
                    } else {
                        if (pq.peek().getValue() < value) {
                            pq.poll();
                            pq.offer(entry);
                        }
                    }
                }
            }
        }

        // Writing the output
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outputFile, true)))) {
            while (!pq.isEmpty()) {
                Map.Entry<String, Double> entry = pq.poll();
                bw.write(entry.getKey() + "\t" + entry.getValue());
                bw.newLine();
            }
        }

        System.out.println("Process completed successfully");
    }
}
