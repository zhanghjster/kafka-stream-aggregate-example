package stream;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.TreeMap;

@JsonPropertyOrder(alphabetic=true)
public class AggregateKey {
    String metric;
    TreeMap<String, String> tags;
    Window window;

    public AggregateKey() {}
    public static AggregateKey with(DataPoint dataPoint) {
        AggregateKey aggregateKey = new AggregateKey();
        aggregateKey.setMetric(dataPoint.getMetric());
        aggregateKey.setTags(dataPoint.getTags());
        return aggregateKey;
    }

    public Window getWindow() {
        return window;
    }

    public void setWindow(Window window) {
        this.window = window;
    }

    public AggregateKey with(Window window) {
        this.setWindow(window);
        return this;
    }

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

}
