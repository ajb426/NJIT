import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class OnScheduleMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Pattern COMMA = Pattern.compile(",");

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        
        // Convert Text value to string
        String line = value.toString();

        // Split CSV by commas not within quotes
        String[] infos = COMMA.split(line);
        
        //exclude the first line (headers)
        if (!"Year".equals(infos[0])) {
            int normal = 0;
            if (!"NA".equals(infos[14])) {
                try {
                    int delay = Integer.parseInt(infos[14].trim());
                    if (delay <= 10) {
                        normal = 1;
                    }
                } catch (NumberFormatException e) {
                    // If not an integer, will be caught and normal remains 0
                }
            }
            output.collect(new Text(infos[8]), new IntWritable(normal));
        }
    }
}
