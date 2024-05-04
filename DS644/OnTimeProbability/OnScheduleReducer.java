import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.IntWritable;

public class OnScheduleReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        
        int totalFlights = 0;
        int onTimeFlights = 0;
        
        while (values.hasNext()) {
            int flightStatus = values.next().get();
            totalFlights++;
            if (flightStatus == 1) {
                onTimeFlights++;
            }
        }
        
        double probability = 0;
        if (totalFlights > 0) {
            probability = (double) onTimeFlights / totalFlights;
        }
        
        // Format the probability as a percentage to two decimal places
        String probabilityFormatted = String.format("%.2f", probability * 100);
        
        output.collect(key, new Text(probabilityFormatted));
    }
}
