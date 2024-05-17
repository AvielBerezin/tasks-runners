package aviel.task_runners.rate_limiters;

public interface RateLimiter<MetaData> {
    void submitTask(MetaData metaData, Runnable task);
}
