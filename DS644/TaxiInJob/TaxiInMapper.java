import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.DoubleWritable;

public class TaxiInMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text airport = new Text();
    private DoubleWritable taxiInTime = new DoubleWritable();

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String[] parts = value.toString().split(",");

        // Check to avoid headers and lines without enough columns
        if (parts.length > 19 && !parts[19].equals("NA") && !parts[19].equals("")) {
            try {
                double time = Double.parseDouble(parts[19].trim()); // Taxi In time
                airport.set(parts[17]); // Destination airport code
                taxiInTime.set(time);
                output.collect(airport, taxiInTime);
            } catch (NumberFormatException e) {
                // Log and ignore malformed records
                System.err.println("Skipping record with invalid number: " + parts[19]);
            }
        }
    }
}

