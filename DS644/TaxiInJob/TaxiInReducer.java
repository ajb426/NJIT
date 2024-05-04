import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.DoubleWritable;

public class TaxiInReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        double sum = 0;
        int count = 0;

        while (values.hasNext()) {
            DoubleWritable val = values.next();
            sum += val.get();
            count++;
        }

        double average = (count > 0) ? (sum / count) : 0;
        result.set(average);
        output.collect(key, result);
    }
}

