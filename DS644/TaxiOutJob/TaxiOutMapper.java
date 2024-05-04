import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.DoubleWritable;

public class TaxiOutMapper extends MapReduceBase implements Mapper<Object, Text, Text, DoubleWritable> {
    private Text airport = new Text();
    private DoubleWritable taxiOutTime = new DoubleWritable();

    @Override
    public void map(Object key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String[] parts = value.toString().split(",");

        // Assuming index 16 is the airport and index 20 is the taxi out time
        if (parts.length > 20) {
            try {
                double time = Double.parseDouble(parts[20].trim()); // Trim to remove any leading/trailing whitespaces
                airport.set(parts[16]); // Set airport code
                taxiOutTime.set(time); // Set taxi out time
                output.collect(airport, taxiOutTime);
            } catch (NumberFormatException e) {
                // Optionally log the error or increment a counter
                System.err.println("Skipping record with invalid number: " + parts[20]);
                reporter.incrCounter("Map", "InvalidNumberFormat", 1);
            }
        }
    }
}

