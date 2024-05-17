package aviel.task_runners.rate_limiters;

import aviel.task_runners.DurationForScheduler;
import aviel.task_runners.pending_tasks.PendingTasks;
import aviel.task_runners.Utils;

import java.time.Duration;
import java.time.Instant;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * RateLimiter for which no more than limit tasks are executed at any duration time-duration.
 * Any tasks that is submitted on an instant for each an execution would break the limit property promised the task would be pended.
 * A pended task will be fetched on the next instant that is possible for its execution without breaking the limit property promised.
 * The pending and fetching of tasks is managed by the PendingTasks created by the provided pendingTasksCreator at construction.
 */
public class ExactRateLimiter<MetaData> implements RateLimiter<MetaData> {
    private final Deque<Instant> executed;
    private final PendingTasks<MetaData> pending;
    private final ScheduledExecutorService pendingScheduler;
    private final AtomicBoolean isScheduled;
    private final Duration duration;
    private final int limit;

    public ExactRateLimiter(Supplier<PendingTasks<MetaData>> pendingTasksCreator,
                            ScheduledExecutorService pendingScheduler,
                            Duration duration, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be strictly positive");
        }
        if (!duration.isPositive()) {
            throw new IllegalArgumentException("duration must be strictly positive");
        }
        executed = new LinkedList<>();
        pending = pendingTasksCreator.get();
        this.pendingScheduler = pendingScheduler;
        isScheduled = new AtomicBoolean(false);
        this.duration = duration;
        this.limit = limit;
    }

    @Override
    public synchronized void submitTask(MetaData metaData, Runnable task) {
        Instant now = Instant.now();
        while (!executed.isEmpty() &&
               now.minus(duration).isAfter(executed.getFirst())) {
            executed.removeFirst();
        }
        if (executed.size() < limit) {
            executed.addLast(now);
            task.run();
        } else {
            pending.insert(metaData, task);
            schedulePending();
        }
    }

    private void schedulePending() {
        if (isScheduled.compareAndSet(false, true)) {
            schedulePendingUnsafe();
        }
    }

    private void schedulePendingUnsafe() {
        Duration untilNextPending = duration.minus(Utils.instantMinus(Instant.now(), executed.getFirst()));
        DurationForScheduler.from(untilNextPending).schedule(pendingScheduler, this::pendingExecutorTask);
    }

    private synchronized void pendingExecutorTask() {
        while (true) {
            while (Instant.now().minus(duration).isAfter(executed.getFirst())) {
                executed.removeFirst();
            }
            if (executed.size() >= limit) {
                break;
            }
            Optional<PendingTasks.Task<MetaData>> nextPending = pending.remove();
            if (nextPending.isEmpty()) {
                break;
            }
            Instant now = Instant.now();
            executed.addLast(now);
            nextPending.get().get().run();
        }
        if (executed.size() >= limit) {
            schedulePendingUnsafe();
        } else {
            isScheduled.set(false);
        }
    }
}
