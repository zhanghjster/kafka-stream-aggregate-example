package myapps;

public class Stats {
    private long count;
    private double sum;
    private double max;
    private double min;
    private double last;
    private double avg;
    private Window window;
    private DataPoint dp;
    private boolean minSetted;

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

    public void caculateAgv() {
        if (count > 0) {
            avg = sum/count;
        }
    }

    public Window getWindow() {
        return window;
    }


    public DataPoint getDp() {
        return dp;
    }

    public Stats() {
        this.window = new Window();
    }

    public Stats update(DataPoint value) {
        this.dp = value;
        this.count++;
        double v = value.getValue();
        this.sum += v;
        this.last = v;
        this.max = (this.max < v) ? v : this.max;
        if (!minSetted) {
            minSetted = true;
            this.min = (this.min > v) ? v : this.min;
        }
        return this;
    }

    private class Window {
        private long startMs;
        private long endMs;
    }
}
