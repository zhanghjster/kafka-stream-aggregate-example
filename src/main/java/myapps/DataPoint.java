package myapps;

import java.util.HashMap;

public class DataPoint {
    public void setValue(double value) {
        this.value = value;
    }

    private double value;

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    private long timestamp;
    private String metric;
    private HashMap tags;

    public String toString() {
        return "[value=" + value + ",timestamp="+timestamp+",metric="+metric+",tags="+tags+"]";
    }

    public double getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }


    public String getMetric() {
        return metric;
    }

    public HashMap getTags() {
        return tags;
    }
}
