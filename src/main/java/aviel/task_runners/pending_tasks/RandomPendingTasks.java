package aviel.task_runners.pending_tasks;

import aviel.task_runners.DLNode;
import aviel.task_runners.ExposedDLList;
import aviel.task_runners.KeyedTask;
import aviel.task_runners.ThisShouldNotHappen;

import java.util.*;
import java.util.function.Consumer;

/**
 * Tasks are disposed weightedly, that is, to dispose a task a key is chosen by a chance proportional to the amount of tasks on that key.
 * Tasks are fetched indiscriminately, that is, a key for whom its task will be fetched is chosen with uniform distribution.
 */
public class RandomPendingTasks<Key, Task extends KeyedTask<Key>> implements PendingTasks<Task> {
    private final Consumer<Task> onDispose;
    private final Undiscriminating<Key> undiscriminating;
    private final Weighted<Key> weighted;
    private final Queues<Key, Task> queues;
    private final int maxStoredTasks;
    private final Random random;

    public RandomPendingTasks(Random random, int maxStoredTasks, Consumer<Task> onDispose) {
        if (maxStoredTasks < 1) {
            throw new IllegalArgumentException("maxStoredTasks must have a strictly positive value");
        }
        this.onDispose = onDispose;
        undiscriminating = new Undiscriminating<>(maxStoredTasks);
        weighted = new Weighted<>(maxStoredTasks);
        queues = new Queues<>(maxStoredTasks);
        this.maxStoredTasks = maxStoredTasks;
        this.random = random;
    }

    @Override
    public synchronized void store(Task task) {
        if (weighted.size() == maxStoredTasks) {
            disposeEntryWeightedly();
        }
        weighted.insert(task.key());
        undiscriminating.insert(task.key());
        queues.insert(task);
    }

    private void disposeEntryWeightedly() {
        Optional<Key> removedOpt = weighted.remove(random);
        if (removedOpt.isEmpty()) {
            return;
        }
        Key removed = removedOpt.get();
        Task task = queues.removeOneOn(removed)
                          .orElseThrow(() -> new ThisShouldNotHappen("For every entry on weighted" +
                                                                     " there must be a corresponding entry on queues"));
        onDispose.accept(task);
        if (queues.isEmpty(removed)) {
            undiscriminating.remove(removed);
        }
    }

    @Override
    public synchronized Optional<Task> fetch() {
        Optional<Key> chosenOpt = undiscriminating.get(random);
        if (chosenOpt.isEmpty()) {
            return Optional.empty();
        }
        Key chosen = chosenOpt.get();
        weighted.removeOneOn(chosen);
        if (weighted.isEmpty(chosen)) {
            undiscriminating.remove(chosen);
        }
        Task task = queues.removeOneOn(chosen)
                          .orElseThrow(() -> new ThisShouldNotHappen("For every entry on undiscriminating" +
                                                                     " there must be a corresponding entry on queues"));
        return Optional.of(task);
    }

    @Override
    public boolean isEmpty() {
        return queues.isEmpty();
    }

    private static class Queues<Key, Task extends KeyedTask<Key>> {
        private final Map<Key, Queue<Task>> queues;

        public Queues(int maxStoredTasks) {
            this.queues = new HashMap<>(maxStoredTasks);
        }

        public Optional<Task> removeOneOn(Key key) {
            Queue<Task> queue = queues.get(key);
            if (queue == null) {
                return Optional.empty();
            }
            Task removed = queue.remove();
            if (queue.isEmpty()) {
                queues.remove(key);
            }
            return Optional.of(removed);
        }

        public void insert(Task task) {
            queues.computeIfAbsent(task.key(), __ -> new LinkedList<>())
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
                                    .orElseThrow(() -> new ThisShouldNotHappen("Data map should not contain empty entries" +
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
