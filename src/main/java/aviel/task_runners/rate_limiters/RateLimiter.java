package aviel.task_runners.rate_limiters;

public interface RateLimiter<Task> {
    void submitTask(Task task);
}
