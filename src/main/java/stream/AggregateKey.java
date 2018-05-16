package stream;

import java.util.TreeMap;

public class AggregateKey {
    String metric;
    TreeMap<String, String> tags;

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public TreeMap<String, String> getTags() {
        return tags;
    }

    public void setTags(TreeMap<String, String> tags) {
        this.tags = tags;
    }

    public AggregateKey() {}
    public static AggregateKey with(DataPoint dataPoint) {
        AggregateKey aggregateKey = new AggregateKey();
        aggregateKey.setMetric(dataPoint.getMetric());
        aggregateKey.setTags(dataPoint.getTags());
        return aggregateKey;
    }
}
