package aviel.task_runners.pending_tasks;

import aviel.task_runners.CyclicQueue;
import aviel.task_runners.KeyedTask;
import aviel.task_runners.Timestamped;

import java.util.*;
import java.util.function.Function;

import static java.util.Comparator.comparing;

/**
 * When the amount of queues has reached queuesCountMax, upon receiving a task of a new key,
 * all entries of a key which hold the oldest entry are disposed.
 * When fetching an element, the last element to be recorded is fetched.
 */
public class KeyedSwapperByOldestStorage<Key, Task extends KeyedTask<Key>> implements Storage<Task> {
    private final SortedSet<Key> sortedKeys;
    private final Map<Key, CyclicQueue<Timestamped<Task>>> queuesMap;

    private final int queuesCountMax;
    private final int queueSizeMax;

    public KeyedSwapperByOldestStorage(int queuesCountMax, int queueSizeMax) {
        queuesMap = new HashMap<>();
        Function<Optional<Timestamped<Task>>, Timestamped<Task>> optionalGet = runnableTimestamped ->
                runnableTimestamped.orElseThrow(() -> new RuntimeException("should not have empty queues"));
        Comparator<Key> byLatestTimestamp =
                comparing(queuesMap::get, comparing(CyclicQueue::peak, comparing(optionalGet, comparing(Timestamped::timestamp))));
        sortedKeys = new TreeSet<>(byLatestTimestamp);
        this.queuesCountMax = queuesCountMax;
        this.queueSizeMax = queueSizeMax;
    }

    @Override
    public synchronized void store(Task task) {
        if (queuesMap.containsKey(task.key())) {
            CyclicQueue<Timestamped<Task>> queue = queuesMap.get(task.key());
            sortedKeys.remove(task.key());
            queue.put(Timestamped.create(task));
            sortedKeys.add(task.key());
        } else {
            if (sortedKeys.size() == queuesCountMax) {
                sortedKeys.remove(sortedKeys.last());
            }
            CyclicQueue<Timestamped<Task>> newQueue = new CyclicQueue<>(queueSizeMax);
            newQueue.put(Timestamped.create(task));
            queuesMap.put(task.key(), newQueue);
            sortedKeys.add(task.key());
        }
    }

    @Override
    public synchronized Optional<Task> fetch() {
        if (sortedKeys.isEmpty()) {
            return Optional.empty();
        }
        Key last = sortedKeys.last();
        sortedKeys.remove(last);
        CyclicQueue<Timestamped<Task>> queue = queuesMap.get(last);
        Timestamped<Task> task = queue.pop().orElseThrow(() -> new RuntimeException("recorded queue should not be empty"));
        if (queue.size() > 0) {
            sortedKeys.add(last);
        }
        return Optional.of(task.get());
    }

    @Override
    public boolean isEmpty() {
        return sortedKeys.isEmpty();
    }
}
