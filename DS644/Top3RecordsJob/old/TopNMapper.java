import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TopNMapper extends MapReduceBase implements Mapper<LongWritable, Text, CompositeKey, Text> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<CompositeKey, Text> output, Reporter reporter) throws IOException {
        String[] parts = value.toString().split("\\s+");
        if (parts.length == 2) {
            try {
                double averageTime = Double.parseDouble(parts[1]);
                CompositeKey compositeKey = new CompositeKey(parts[0], averageTime);
                output.collect(compositeKey, value);
            } catch (NumberFormatException e) {
                reporter.incrCounter("MapperErrors", "NumberFormatErrors", 1);
            }
        }
    }
}

