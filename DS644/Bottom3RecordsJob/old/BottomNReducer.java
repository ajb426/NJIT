import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class BottomNReducer extends Reducer<CompositeKey, Text, Text, Text> {
    private int count = 0;

    @Override
    protected void setup(Context context) {
        count = 0; // reset count for every new reduce task
    }

    @Override
    public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (count < 3) {
            for (Text value : values) {
                if (count < 3) {
                    context.write(new Text(key.getAirportCode()), value);
                    count++;
                } else {
                    break;
                }
            }
        }
    }
}
