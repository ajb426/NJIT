import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CancellationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] infos = line.split(",");

        // exclude the first line
        if (!"Year".equals(infos[0])) {
            if ("1".equals(infos[21]) && !"NA".equals(infos[22]) && infos[22].trim().length() > 0) {
                output.collect(new Text(infos[22]), new IntWritable(1));
            }
        }
    }
}
