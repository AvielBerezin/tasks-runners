package aviel.task_runners;

import java.time.Instant;

public record Timestamped<Value>(Instant timestamp, Value get) {
    public static <Value> Timestamped<Value> create(Value value) {
        return new Timestamped<>(Instant.now(), value);
    }
}
