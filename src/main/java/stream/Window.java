package stream;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder(alphabetic=true)
public class Window {
    private final long startMs;
    private final long endMs;

    public Window(long startMs, long endMs) {
        this.startMs = startMs;
        this.endMs = endMs;
    }

    public long getStartMs() {
        return startMs;
    }

    public long getEndMs() {
        return endMs;
    }

    public String toString() {
        return "Window[start=" + startMs + ", end=" + endMs + "]";
    }
}
