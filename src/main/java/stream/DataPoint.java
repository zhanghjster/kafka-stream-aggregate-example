package stream;

import java.util.TreeMap;

public class DataPoint {
    private double value;
    private long timestamp;
    private String metric;
    private TreeMap<String, String> tags;
    private Stats stats;

    public static DataPoint from(AggregateKey key) {
        DataPoint dataPoint = new DataPoint();
        dataPoint.setTags(key.getTags());
        dataPoint.setMetric(key.metric);
        return dataPoint;
    }

    public DataPoint withStats(Stats stats) {
        this.setStats(stats);
        return this;
    }
    public DataPoint withTimestamp(long timestamp) {
        this.setTimestamp(timestamp);
        return this;
    }

    public void setTags(TreeMap<String, String> tags) {
        this.tags = tags;
    }

    public void setStats(Stats stats) {
        this.stats = stats;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public void setValue(double value) {
        this.value = value;
    }


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

    public TreeMap<String, String> getTags() {
        return tags;
    }
}
