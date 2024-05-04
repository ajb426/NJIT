import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class MaxCancellationCode {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: java MaxCancellationCode <input file> <output file>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputFile = args[1];
        Map<String, Integer> countMap = new HashMap<>();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(inputFile);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(inputPath)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length >= 2) {
                    String code = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    countMap.put(code, countMap.getOrDefault(code, 0) + count);
                }
            }
        } catch (IOException e) {
            System.err.println("IO error reading file: " + e.getMessage());
            System.exit(1);
        }

        Path outputPath = new Path(outputFile);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, true)))) {
            Optional<Map.Entry<String, Integer>> maxEntry = countMap.entrySet()
                .stream()
                .max(Map.Entry.comparingByValue());

            if (maxEntry.isPresent()) {
                int maxValue = maxEntry.get().getValue();
                List<String> maxCodes = countMap.entrySet().stream()
                    .filter(entry -> entry.getValue() == maxValue)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

                writer.write("Code(s) with the most occurrences: " + String.join(", ", maxCodes));
            } else {
                writer.write("No entries found.");
            }
        }
    }
}
