package aviel.task_runners.rate_limiters;

import aviel.task_runners.pending_tasks.RandomPendingTasks;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ExactUniformingRateLimiterTest {
    public static void main(String[] args) throws InterruptedException {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ExactUniformingRateLimiter<Integer> rateLimiter = new ExactUniformingRateLimiter<>(() -> new RandomPendingTasks<>(new Random(), 20),
                                                                                           scheduler,
                                                                                           0.3,
                                                                                           Duration.ofSeconds(1),
                                                                                           3);
        for (int i = 0; i < 100; i++) {
            int jobId = i % 4;
            rateLimiter.submitTask(jobId, () -> System.out.println("job " + jobId));
        }
        Thread.sleep(20_000);
        rateLimiter.awaitCurrentTasks();
        scheduler.shutdown();
    }
}