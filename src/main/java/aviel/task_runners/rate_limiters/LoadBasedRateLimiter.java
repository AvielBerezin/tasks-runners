package aviel.task_runners.rate_limiters;

import aviel.task_runners.pending_tasks.PendingTasks;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static aviel.task_runners.DurationForScheduler.from;

/**
 * A task manager that limits task execution rate based on load as to the following definition of load:
 * Every time duration that is second / loadRate the load is decremented by 1, upon every execution of a task the load is incremented by 1.
 * That is within bounds of 0 to maxLoad, meaning, a load would not be decremented below 0 and no task is executed when the load is at maxLoad.
 * Tasks that are submitted when the load is at maxLoad are pended for future execution and are fetched when load is just below loadRate.
 * The pending and fetching of tasks at a maxLoad load is managed by PendingTasks that is provided by the pendingTasksCreator provided at construction.
 */
public class LoadBasedRateLimiter<MetaData> implements RateLimiter<MetaData> {
    private final AtomicInteger load;
    private final int maxLoad;
    private final PendingTasks<MetaData> pending;

    /**
     * @param pendingTasksCreator a creator for the collector of tasks for pending.
     * @param loadDecrementer     scheduler that is used for load decremental.
     * @param loadRate            the rate of which the load decreases involuntarily. Units: Hz (times per second)
     * @param maxLoad             the maximal load that is allowed to be reached. Units: Hz (times per second)
     */
    public LoadBasedRateLimiter(Supplier<PendingTasks<MetaData>> pendingTasksCreator,
                                ScheduledExecutorService loadDecrementer,
                                int loadRate,
                                int maxLoad) {
        this.load = new AtomicInteger(0);
        this.maxLoad = maxLoad;
        pending = pendingTasksCreator.get();
        from(Duration.ofSeconds(1).dividedBy(loadRate)).schedule(loadDecrementer, () -> {
            Optional<PendingTasks.Task<MetaData>> removed = pending.remove();
            if (removed.isPresent()) {
                removed.get().run();
            } else {
                decrementLoad();
            }
        });
    }

    @Override
    public void submitTask(MetaData metaData, Runnable task) {
        if (incrementLoad()) {
            task.run();
        } else {
            pending.insert(metaData, task);
        }
    }

    /**
     * @return true if incrementation succeeded or false if load has reached maximally allowed load value
     */
    private boolean incrementLoad() {
        int currentLoad;
        do {
            currentLoad = load.get();
            if (currentLoad >= maxLoad) {
                return false;
            }
        }
        while (!load.compareAndSet(currentLoad, currentLoad + 1));
        return true;
    }

    /**
     * @return true if decremental succeeded or false if load has reached minimally allowed load value 0
     */
    private boolean decrementLoad() {
        int currentLoad;
        do {
            currentLoad = load.get();
            if (currentLoad <= 0) {
                return false;
            }
        }
        while (!load.compareAndSet(currentLoad, currentLoad - 1));
        return true;
    }
}
