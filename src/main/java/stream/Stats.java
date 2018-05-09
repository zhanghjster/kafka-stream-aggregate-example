package stream;

public class Stats {
    private long count;
    private double sum;
    private double max;
    private double min;
    private double last;
    private double avg;
    private Window window;

    private transient boolean minInit;

    Stats() {
        this.window = new Window();
        this.minInit = false;
    }

    void setWindow(Stats.Window window) {
        this.window = window;
    }

    public void update(double value) {
        count++;
        sum += value;
        max = (max < value) ? value : max;
        if (!minInit) {
            min = value;
            minInit = true;
        }
        min = (min > value) ? value : min;

        last = value;
    }

    public double getAvg() {
        return avg;
    }

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

    public void caculateAvg() {
        if (count>0) {
            avg = sum/count;
        }
    }

    public Window getWindow() {
        return window;
    }

    static class Window {
        private Long startMs;
        private Long endMs;

        Window() {

        }
        Window(long start, long end) {
            this.startMs = start;
            this.endMs = end;
        }

        public String toString() {
            return "window[start=" + startMs + ", end=" + endMs + "]";
        }
    }

    public String toString() {
        return "stats[" +
                "count=" + count +
                ", sum=" + sum +
                ", max=" + max +
                ", min=" + min +
                ", last=" + last +
                ", avg=" + avg +
                ", widow=" + window +
                "]";
    }

}
