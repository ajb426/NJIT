import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {
    private String airportCode;
    private double averageTaxiTime;

    public CompositeKey() {
    }

    public CompositeKey(String airportCode, double averageTaxiTime) {
        this.airportCode = airportCode;
        this.averageTaxiTime = averageTaxiTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, airportCode);
        out.writeDouble(averageTaxiTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airportCode = WritableUtils.readString(in);
        averageTaxiTime = in.readDouble();
    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = Double.compare(this.averageTaxiTime, o.averageTaxiTime);
        if (result != 0) {
            return result;
        }
        return this.airportCode.compareTo(o.airportCode);
    }

    public String getAirportCode() {
        return airportCode;
    }

    public double getAverageTaxiTime() {
        return averageTaxiTime;
    }
}
