package aviel.task_runners;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class CyclicQueueTest {
    @Test
    public void multiPut() {
        CyclicQueue<Integer> queue = new CyclicQueue<>(10);
        int threadsCount = Runtime.getRuntime().availableProcessors();
        try (ExecutorService executor = Executors.newFixedThreadPool(threadsCount)) {
            for (int i = 0; i < threadsCount; i++) {
                int threadId = i;
                executor.execute(() -> {
                    for (int j = 0; j < 1000; j++) {
                        queue.put(threadId * 1000 + j);
                    }
                });
            }
        }
        List<Integer> results = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            Optional<Integer> popped = queue.pop();
            assert popped.isPresent();
            results.add(popped.get());
        }
        assert queue.pop().isEmpty();
        assert new HashSet<>(results).size() == 10;
        for (int i = 0; i < results.size(); i++) {
            assert 990 <= results.get(i) % 1000;
        }
    }

    @Test
    public void multiPutWithPops() {
        CyclicQueue<Integer> queue = new CyclicQueue<>(10);
        AtomicInteger deletions = new AtomicInteger(0);
        int operationsPerThread = 1000000;
        try (ExecutorService executor = Executors.newFixedThreadPool(20)) {
            for (int i = 0; i < 10; i++) {
                int threadId = i;
                executor.execute(() -> {
                    for (int j = 0; j < operationsPerThread; j++) {
                        queue.put(threadId * operationsPerThread + j).ifPresent(__ -> deletions.incrementAndGet());
                    }
                });
            }
            for (int i = 10; i < 20; i++) {
                executor.execute(() -> {
                    for (int j = 0; j < operationsPerThread; j++) {
                        queue.pop().ifPresent(__ -> deletions.incrementAndGet());
                    }
                });
            }
        }
        int expectedSize = operationsPerThread * 10 - deletions.get();
        List<Integer> results = new ArrayList<>(expectedSize);
        for (int i = 0; i < expectedSize; i++) {
            Optional<Integer> popped = queue.pop();
            assert popped.isPresent();
            results.add(popped.get());
        }
        assert queue.pop().isEmpty();
        assert new HashSet<>(results).size() == expectedSize;
        for (int i = 0; i < results.size(); i++) {
            assert operationsPerThread - 10 <= results.get(i) % operationsPerThread;
        }
    }

    @Test
    public void orderTest() {
        CyclicQueue<Integer> queue = new CyclicQueue<>(5);
        for (int i = 0; i < 7; i++) {
            queue.put(i);
        }
        List<Integer> results = new ArrayList<>(5);
        for (int i = 0; i < 5; i++) {
            Optional<Integer> popped = queue.pop();
            assert popped.isPresent();
            results.add(popped.get());
        }
        assert results.equals(List.of(2, 3, 4, 5, 6));
    }
}