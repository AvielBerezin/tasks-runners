package aviel.task_runners.rate_limiters;

import aviel.task_runners.DurationForScheduler;
import aviel.task_runners.Utils;
import aviel.task_runners.pending_tasks.Storage;

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
public class ExactUniformingRateLimiter<Task extends Runnable> implements RateLimiter<Task> {
    private final Deque<Instant> executed;
    private final Storage<Task> pending;
    private final ScheduledExecutorService pendingScheduler;
    private final AtomicBoolean isScheduled;
    private final Duration duration;
    private final int limit;
    private final double uniformingRate;
    private final Object completionLock;

    public ExactUniformingRateLimiter(Supplier<Storage<Task>> pendingTasksCreator,
                                      ScheduledExecutorService pendingScheduler,
                                      double uniformingRate,
                                      Duration duration, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be strictly positive");
        }
        if (!duration.isPositive()) {
            throw new IllegalArgumentException("duration must be strictly positive");
        }
        if (uniformingRate < 0 || uniformingRate > 1) {
            throw new IllegalArgumentException("uniformingRate must be a value between 0 and 1");
        }
        executed = new LinkedList<>();
        pending = pendingTasksCreator.get();
        this.uniformingRate = uniformingRate;
        this.pendingScheduler = pendingScheduler;
        isScheduled = new AtomicBoolean(false);
        this.duration = duration;
        this.limit = limit;
        completionLock = new Object();
    }

    @Override
    public void submitTask(Task task) {
        synchronized (completionLock) {
            Instant now = Instant.now();
            cleanOldExecutedRecords();
            if (executed.size() < limit) {
                executed.addLast(now);
                task.run();
            } else {
                pending.store(task);
                schedulePending();
            }
            if (pending.isEmpty()) {
                completionLock.notifyAll();
            }
        }
    }

    private void schedulePending() {
        if (isScheduled.compareAndSet(false, true)) {
            schedulePendingUnsafe();
        }
    }

    private void schedulePendingUnsafe() {
        Duration uniformInterval = duration.dividedBy(limit);
        Duration untilNextPending = executed.size() >= limit
                                    ? duration.minus(Utils.instantMinus(Instant.now(), executed.getFirst()))
                                    : Duration.ZERO;
        if (untilNextPending.compareTo(uniformInterval) < 0) {
            untilNextPending = untilNextPending.plus(uniformInterval.minus(untilNextPending)
                                                                    .dividedBy(1_000_000L)
                                                                    .multipliedBy((long) (1_000_000d * uniformingRate)));
        }
        DurationForScheduler.from(untilNextPending).schedule(pendingScheduler, this::pendingExecutorTask);
    }

    private void pendingExecutorTask() {
        synchronized (completionLock) {
            try {
                cleanOldExecutedRecords();
                if (executed.size() < limit) {
                    Optional<Task> nextPending = pending.fetch();
                    if (nextPending.isPresent()) {
                        Instant now = Instant.now();
                        executed.addLast(now);
                        nextPending.get().run();
                    }
                }
                if (!pending.isEmpty()) {
                    schedulePendingUnsafe();
                } else {
                    isScheduled.set(false);
                    completionLock.notifyAll();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void cleanOldExecutedRecords() {
        while (!executed.isEmpty() &&
               Instant.now().minus(duration).isAfter(executed.getFirst())) {
            executed.removeFirst();
        }
    }

    public void awaitCurrentTasks() throws InterruptedException {
        synchronized (completionLock) {
            while (!pending.isEmpty()) {
                completionLock.wait();
            }
        }
    }
}
