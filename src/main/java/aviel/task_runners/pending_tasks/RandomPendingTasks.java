package aviel.task_runners.pending_tasks;

import aviel.task_runners.DLNode;
import aviel.task_runners.ExposedDLList;

import java.util.*;

/**
 * Tasks are disposed weightedly, that is, to dispose a task a key is chosen by a chance proportional to the amount of tasks on that key.
 * Tasks are fetched indiscriminately, that is, a key for whom its task will be fetched is chosen with uniform distribution.
 */
public class RandomPendingTasks<Key> implements PendingTasks<Key> {
    private final Undiscriminating<Key> undiscriminating;
    private final Weighted<Key> weighted;
    private final Queues<Key> queues;
    private final int maxStoredTasks;
    private final Random random;

    public RandomPendingTasks(Random random, int maxStoredTasks) {
        if (maxStoredTasks < 1) {
            throw new IllegalArgumentException("maxStoredTasks must have a strictly positive value");
        }
        undiscriminating = new Undiscriminating<>(maxStoredTasks);
        weighted = new Weighted<>(maxStoredTasks);
        queues = new Queues<>(maxStoredTasks);
        this.maxStoredTasks = maxStoredTasks;
        this.random = random;
    }

    @Override
    public synchronized void insert(Key key, Runnable task) {
        if (weighted.size() == maxStoredTasks) {
            disposeEntryWeightedly();
        }
        weighted.insert(key);
        undiscriminating.insert(key);
        queues.insert(key, task);
    }

    private void disposeEntryWeightedly() {
        Optional<Key> removedOpt = weighted.remove(random);
        if (removedOpt.isEmpty()) {
            return;
        }
        Key removed = removedOpt.get();
        queues.removeOneOn(removed);
        if (queues.isEmpty(removed)) {
            undiscriminating.remove(removed);
        }
    }

    @Override
    public synchronized Optional<Task<Key>> remove() {
        Optional<Key> chosenOpt = undiscriminating.get(random);
        if (chosenOpt.isEmpty()) {
            return Optional.empty();
        }
        Key chosen = chosenOpt.get();
        weighted.removeOneOn(chosen);
        if (weighted.isEmpty(chosen)) {
            undiscriminating.remove(chosen);
        }
        Runnable runnable = queues.removeOneOn(chosen)
                                  .orElseThrow(() -> new RuntimeException("this should not happen," +
                                                                          " for every entry on undiscriminating" +
                                                                          " there must be a corresponding entry on queues"));
        return Optional.of(new Task<>(chosen, runnable));
    }

    @Override
    public boolean isEmpty() {
        return queues.isEmpty();
    }

    private static class Queues<Key> {
        private final Map<Key, Queue<Runnable>> queues;

        public Queues(int maxStoredTasks) {
            this.queues = new HashMap<>(maxStoredTasks);
        }

        public Optional<Runnable> removeOneOn(Key key) {
            Queue<Runnable> queue = queues.get(key);
            if (queue == null) {
                return Optional.empty();
            }
            Runnable removed = queue.remove();
            if (queue.isEmpty()) {
                queues.remove(key);
            }
            return Optional.of(removed);
        }

        public void insert(Key key, Runnable task) {
            queues.computeIfAbsent(key, __ -> new LinkedList<>())
                  .add(task);
        }

        public boolean isEmpty(Key key) {
            return !queues.containsKey(key);
        }

        public boolean isEmpty() {
            return queues.isEmpty();
        }
    }

    private static class Weighted<Key> {
        /*
         * implementation notes:
         * 1) Foreach index in data:
         *    locator[data[index].key] contains node data[index].locatorNode
         * 2) Foreach key, list in locator:
         *    Foreach value index in list:
         *    data[index].key = key
         */
        private final ArrayList<Entry<Key>> data;
        private final Map<Key, ExposedDLList<Integer>> locator;

        public Weighted(int maxStoredTasks) {
            data = new ArrayList<>(maxStoredTasks);
            locator = new HashMap<>(maxStoredTasks);
        }

        public Optional<Key> remove(Random random) {
            if (data.isEmpty()) {
                return Optional.empty();
            }
            int index = random.nextInt(data.size());
            Entry<Key> chosen = data.get(index);
            Collections.swap(data, index, data.size() - 1);
            data.remove(data.size() - 1);
            chosen.locatorNode.unlink();
            if (index < data.size()) {
                relocate(index);
            }
            if (locator.get(chosen.key).size() == 0) {
                locator.remove(chosen.key);
            }
            return Optional.of(chosen.key);
        }

        private void relocate(int index) {
            Entry<Key> oldEntry = data.get(index);
            oldEntry.locatorNode.unlink();
            data.set(index,
                     new Entry<>(oldEntry.key,
                                 locator.get(oldEntry.key).addLast(index)));
        }

        public void removeOneOn(Key key) {
            if (!locator.containsKey(key)) {
                return;
            }
            ExposedDLList<Integer> locatorEntry = locator.get(key);
            int index = locatorEntry.removeLast()
                                    .orElseThrow(() -> new RuntimeException("this should not happen," +
                                                                            " data map should not contain empty entries" +
                                                                            " at Weighted"));
            Collections.swap(data, index, data.size() - 1);
            data.remove(data.size() - 1);
            if (index < data.size()) {
                relocate(index);
            }
            if (locatorEntry.size() == 0) {
                locator.remove(key);
            }
        }

        public boolean isEmpty(Key key) {
            return !locator.containsKey(key);
        }

        public void insert(Key key) {
            data.add(new Entry<>(key,
                                 locator.computeIfAbsent(key, __ -> new ExposedDLList<>())
                                        .addLast(data.size())));
        }

        public int size() {
            return data.size();
        }

        private record Entry<Key>(Key key, DLNode<Integer> locatorNode) {}
    }

    private static class Undiscriminating<Key> {
        private final ArrayList<Key> data;
        private final Map<Key, Integer> locator;

        public Undiscriminating(int maxStoredTasks) {
            data = new ArrayList<>(maxStoredTasks);
            locator = new HashMap<>(maxStoredTasks);
        }

        public Optional<Key> get(Random random) {
            if (data.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(data.get(random.nextInt(data.size())));
        }

        public void remove(Key key) {
            Integer index = locator.get(key);
            Collections.swap(data, index, data.size() - 1);
            data.remove(data.size() - 1);
            if (index < data.size()) {
                locator.put(data.get(index), index);
            }
            locator.remove(key);
        }

        public void insert(Key key) {
            if (!locator.containsKey(key)) {
                locator.put(key, data.size());
                data.add(key);
            }
        }
    }
}
