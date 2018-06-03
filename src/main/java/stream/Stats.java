package stream;

import java.util.TreeMap;

public class Stats {
    private String metric;
    private TreeMap<String, String> tags;
    private Window window;
    private long count;
    private double sum;
    private double max;
    private double min;
    private double last;
    private double avg;
    private transient boolean minSetted;

    public long getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }

    public double getLast() {
        return last;
    }

    public double getAvg() {
        return avg;
    }

    public Stats update(DataPoint value) {
        count++;
        double v = value.getValue();
        sum += v;
        last = v;
        max = (max < v) ? v : max;
        if (!minSetted) {
            minSetted = true;
            min = v;
        } else {
            min = (min > v) ? v : min;
        }
        if (count > 0) {
            avg = sum/count;
        }

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

    public Window getWindow() {
        return window;
    }

    public void setWindow(Window window) {
        this.window = window;
    }

}
