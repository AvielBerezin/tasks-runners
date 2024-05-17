package aviel.task_runners;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public record DurationForScheduler(long delay, TimeUnit unit) {
    public static DurationForScheduler from(Duration duration) {
        long amount;
        TimeUnit unit;
        try {
            amount = duration.toNanos();
            unit = TimeUnit.NANOSECONDS;
        } catch (ArithmeticException e) {
            amount = duration.toSeconds();
            unit = TimeUnit.SECONDS;
        }
        return new DurationForScheduler(amount, unit);
    }

    public void schedule(ScheduledExecutorService scheduler, Runnable task) {
        scheduler.schedule(task, delay, unit);
    }
}
