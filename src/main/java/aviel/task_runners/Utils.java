package aviel.task_runners;

import java.time.Duration;
import java.time.Instant;

public class Utils {
    public static Duration instantMinus(Instant subtrahend, Instant minuend) {
        return Duration.ofSeconds(subtrahend.getEpochSecond() - minuend.getEpochSecond())
                       .plusNanos(subtrahend.getNano() - minuend.getNano());
    }
}
