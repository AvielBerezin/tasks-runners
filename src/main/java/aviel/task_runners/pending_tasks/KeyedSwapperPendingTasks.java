package aviel.task_runners.pending_tasks;

import aviel.task_runners.CyclicQueue;
import aviel.task_runners.Timestamped;

import java.util.*;
import java.util.function.Function;

import static java.util.Comparator.comparing;

/**
 * When the amount of queues has reached queuesCountMax, upon receiving a task of a new key,
 * all entries of a key which hold the oldest entry are disposed.
 * When fetching an element, the last element to be recorded is fetched.
 */
public class KeyedSwapperPendingTasks<Key> implements PendingTasks<Key> {
    private final SortedSet<Key> sortedKeys;
    private final Map<Key, CyclicQueue<Timestamped<Runnable>>> queuesMap;

    private final int queuesCountMax;
    private final int queueSizeMax;

    public KeyedSwapperPendingTasks(int queuesCountMax, int queueSizeMax) {
        queuesMap = new HashMap<>();
        Function<Optional<Timestamped<Runnable>>, Timestamped<Runnable>> optionalGet = runnableTimestamped ->
                runnableTimestamped.orElseThrow(() -> new RuntimeException("should not have empty queues"));
        Comparator<Key> byLatestTimestamp =
                comparing(queuesMap::get, comparing(CyclicQueue::peak, comparing(optionalGet, comparing(Timestamped::timestamp))));
        sortedKeys = new TreeSet<>(byLatestTimestamp);
        this.queuesCountMax = queuesCountMax;
        this.queueSizeMax = queueSizeMax;
    }

    @Override
    public synchronized void insert(Key key, Runnable task) {
        if (queuesMap.containsKey(key)) {
            CyclicQueue<Timestamped<Runnable>> queue = queuesMap.get(key);
            sortedKeys.remove(key);
            queue.put(Timestamped.create(task));
            sortedKeys.add(key);
        } else {
            if (sortedKeys.size() == queuesCountMax) {
                sortedKeys.remove(sortedKeys.last());
            }
            CyclicQueue<Timestamped<Runnable>> newQueue = new CyclicQueue<>(queueSizeMax);
            newQueue.put(Timestamped.create(task));
            queuesMap.put(key, newQueue);
            sortedKeys.add(key);
        }
    }

    @Override
    public synchronized Optional<Task<Key>> remove() {
        if (sortedKeys.isEmpty()) {
            return Optional.empty();
        }
        Key last = sortedKeys.last();
        sortedKeys.remove(last);
        CyclicQueue<Timestamped<Runnable>> queue = queuesMap.get(last);
        Timestamped<Runnable> task = queue.pop().orElseThrow(() -> new RuntimeException("recorded queue should not be empty"));
        if (queue.size() > 0) {
            sortedKeys.add(last);
        }
        return Optional.of(new Task<>(last, task.get()));
    }

    @Override
    public boolean isEmpty() {
        return sortedKeys.isEmpty();
    }
}
