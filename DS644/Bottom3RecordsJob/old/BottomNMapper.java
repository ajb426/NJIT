import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BottomNMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\s+");
        if (parts.length == 2) {
            String airportCode = parts[0];
            double averageTime = Double.parseDouble(parts[1]);
            CompositeKey compositeKey = new CompositeKey(airportCode, averageTime);
            context.write(compositeKey, new Text(parts[1]));
        }
    }
}
