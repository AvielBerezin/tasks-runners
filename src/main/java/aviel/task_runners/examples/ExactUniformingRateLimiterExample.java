package aviel.task_runners.examples;

import aviel.task_runners.KeyedTask;
import aviel.task_runners.Utils;
import aviel.task_runners.pending_tasks.PendingTasks;
import aviel.task_runners.pending_tasks.RandomPendingTasks;
import aviel.task_runners.rate_limiters.ExactUniformingRateLimiter;
import aviel.task_runners.rate_limiters.RateLimiter;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.reactivex.rxjava3.internal.schedulers.ExecutorScheduler;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

public class ExactUniformingRateLimiterExample {
    public static void main(String[] args) throws IOException {
        String bucket = "rate limiter";
        String org = "aviel";
        AtomicBoolean stopped = new AtomicBoolean(false);
        try (InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", System.getenv("INFLUX_TOKEN").toCharArray());
             ExecutorService influxWriterExecutor = Executors.newFixedThreadPool(2)) {
            WriteApi writeApi = client.makeWriteApi(WriteOptions.builder()
                                                                .bufferLimit(1 << 14)
                                                                .writeScheduler(new ExecutorScheduler(influxWriterExecutor, false, false))
                                                                .build());
            try (ExecutorService threadPool = Executors.newFixedThreadPool(6)) {
                Duration duration = Duration.ofSeconds(1).multipliedBy(5);
                int limit = 30 * 5;
                int maxStoredTasks = 20;
                threadPool.execute(() -> {
                    try {
                        testExactUniformRateLimiterWithRandomPendingTasks(stopped, bucket, org, writeApi, 0, duration, limit, maxStoredTasks);
                    } catch (InterruptedException e) {
                        new Exception("testExactUniformRateLimiterWithRandomPendingTasks", e).printStackTrace();
                    }
                });
                threadPool.execute(() -> {
                    try {
                        testExactUniformRateLimiterWithRandomPendingTasks(stopped, bucket, org, writeApi, 0.1, duration, limit, maxStoredTasks);
                    } catch (InterruptedException e) {
                        new Exception("testExactUniformRateLimiterWithRandomPendingTasks", e).printStackTrace();
                    }
                });
                threadPool.execute(() -> {
                    try {
                        testExactUniformRateLimiterWithRandomPendingTasks(stopped, bucket, org, writeApi, 0.3, duration, limit, maxStoredTasks);
                    } catch (InterruptedException e) {
                        new Exception("testExactUniformRateLimiterWithRandomPendingTasks", e).printStackTrace();
                    }
                });
                threadPool.execute(() -> {
                    try {
                        testExactUniformRateLimiterWithRandomPendingTasks(stopped, bucket, org, writeApi, 0.6, duration, limit, maxStoredTasks);
                    } catch (InterruptedException e) {
                        new Exception("testExactUniformRateLimiterWithRandomPendingTasks", e).printStackTrace();
                    }
                });
                threadPool.execute(() -> {
                    try {
                        testExactUniformRateLimiterWithRandomPendingTasks(stopped, bucket, org, writeApi, 0.9, duration, limit, maxStoredTasks);
                    } catch (InterruptedException e) {
                        new Exception("testExactUniformRateLimiterWithRandomPendingTasks", e).printStackTrace();
                    }
                });
                threadPool.execute(() -> {
                    try {
                        testExactUniformRateLimiterWithRandomPendingTasks(stopped, bucket, org, writeApi, 1, duration, limit, maxStoredTasks);
                    } catch (InterruptedException e) {
                        new Exception("testExactUniformRateLimiterWithRandomPendingTasks", e).printStackTrace();
                    }
                });
                System.out.println("waiting for any input to stop");
                System.in.read();
                System.out.println("stopping");
                stopped.set(true);
            }
        }
    }

    private static void testExactUniformRateLimiterWithRandomPendingTasks(AtomicBoolean stopped, String bucket, String org, WriteApi writeApi, double uniformingRate, Duration duration, int limit, int maxStoredTasks) throws InterruptedException {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        UnaryOperator<Point> setPointAlgorithmicParameters = setPointAlgorithmicParameters(uniformingRate, duration, limit, maxStoredTasks);
        BiFunction<Integer, Integer, Function<Point, Point>> setPointBasicData = (key, id) ->
                ExactUniformingRateLimiterExample.setPointTaskInfo(key, id)
                                                 .compose(ExactUniformingRateLimiterExample::setPointCurrentTime)
                                                 .compose(setPointAlgorithmicParameters);
        AtomicLong disposalCount = new AtomicLong(0);
        AtomicLong executedCount = new AtomicLong(0);
        AtomicLong submittedCount = new AtomicLong(0);
        ExactUniformingRateLimiter<IndexedTask> rateLimiter =
                new ExactUniformingRateLimiter<>(() -> new RandomPendingTasks<>(new Random(), maxStoredTasks,
                                                                                disposedTask -> {
                                                                                    writeApi.writePoint(bucket, org,
                                                                                                        setPointBasicData.apply(disposedTask.key(), disposedTask.id())
                                                                                                                         .apply(Point.measurement("disposed")
                                                                                                                                     .addField("count", disposalCount.incrementAndGet())));
                                                                                }),
                                                 scheduler,
                                                 uniformingRate,
                                                 duration,
                                                 limit);
        IndexedTask.Creator tasksCreator = IndexedTask.creator(id -> id % 4);
        AtomicReference<Optional<Instant>> lastExecutionInstantOpt = new AtomicReference<>(Optional.empty());
        for(int i = 0; i < Integer.MAX_VALUE && !stopped.get(); i++) {
            int jobId = i;
            Instant start = Instant.now();
            IndexedTask task = tasksCreator.create(jobId, jobCategory -> {
                Instant executionInstant = Instant.now();
                Function<Point, Point> setPointBasicDataConcrete =
                        setPointBasicData.apply(jobCategory, jobId);
                writeApi.writePoint(bucket, org,
                                    setPointBasicDataConcrete.compose(setFieldDurationInMillis(Utils.instantMinus(executionInstant, start)))
                                                             .apply(Point.measurement("execution delay")));
                lastExecutionInstantOpt.get().ifPresent(lastExecutionInstant -> {
                    writeApi.writePoint(bucket, org,
                                        setPointBasicDataConcrete.compose(setFieldDurationInMillis(Utils.instantMinus(executionInstant, lastExecutionInstant)))
                                                                 .apply(Point.measurement("adjacent interval")));
                });
                lastExecutionInstantOpt.set(Optional.of(executionInstant));
                writeApi.writePoint(bucket, org,
                                    setPointBasicDataConcrete.apply(Point.measurement("executed")
                                                                         .addField("count", executedCount.incrementAndGet())));
            });
            rateLimiter.submitTask(task);
            writeApi.writePoint(bucket, org,
                                setPointBasicData.apply(task.key(), task.id())
                                                 .apply(Point.measurement("submitted")
                                                             .addField("count", submittedCount.incrementAndGet())));
            Thread.sleep(limit / duration.dividedBy(Duration.ofSeconds(1)) / 3);
        }
        rateLimiter.awaitCurrentTasks();
        scheduler.shutdown();
    }

    @NotNull
    private static UnaryOperator<Point> setFieldDurationInMillis(Duration interval) {
        return point -> point.addField("durationInMillis", interval.toMillis());
    }

    @NotNull
    private static UnaryOperator<Point> setPointAlgorithmicParameters(double uniformingRate, Duration duration, int limit, int maxStoredTasks) {
        return point ->
                point.addTag(RateLimiter.class.getSimpleName(),
                             ExactUniformingRateLimiter.class.getSimpleName())
                     .addTag(ExactUniformingRateLimiter.class.getSimpleName() + ".uniformingRate",
                             String.valueOf(uniformingRate))
                     .addTag(ExactUniformingRateLimiter.class.getSimpleName() + ".duration",
                             duration.toString())
                     .addTag(ExactUniformingRateLimiter.class.getSimpleName() + ".limit",
                             String.valueOf(limit))
                     .addTag(PendingTasks.class.getSimpleName(),
                             RandomPendingTasks.class.getSimpleName())
                     .addTag(RandomPendingTasks.class.getSimpleName() + ".maxStoredTasks",
                             String.valueOf(maxStoredTasks));
    }

    private static UnaryOperator<Point> setPointTaskInfo(Integer key, Integer id) {
        return point ->
                point.addTag("task.key", String.valueOf(key))
                     .addTag("task.id", String.valueOf(id));
    }

    private static Point setPointCurrentTime(Point point) {return point.time(Instant.now(), WritePrecision.MS);}

    private static class IndexedTask implements KeyedTask<Integer> {
        private final int id;
        private final int key;
        private final Runnable runnable;

        public IndexedTask(IntUnaryOperator categorizer, int id, IntConsumer taskOnCategory) {
            this.id = id;
            this.key = categorizer.applyAsInt(id);
            this.runnable = () -> taskOnCategory.accept(key);
        }

        public int id() {
            return id;
        }

        @Override
        public Integer key() {
            return key;
        }

        @Override
        public void run() {
            runnable.run();
        }

        private static class Creator {
            private final IntUnaryOperator categorizer;

            private Creator(IntUnaryOperator categorizer) {
                this.categorizer = categorizer;
            }

            public IndexedTask create(int id, IntConsumer taskOnCategory) {
                return new IndexedTask(categorizer, id, taskOnCategory);
            }
        }

        static Creator creator(IntUnaryOperator categorizer) {
            return new Creator(categorizer);
        }
    }
}