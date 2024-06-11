package aviel.task_runners.pending_tasks;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueStorage<Task> implements Storage<Task> {
    private final BlockingQueue<Task> queue;

    public QueueStorage() {
        queue = new LinkedBlockingQueue<>();
    }

    @Override
    public void store(Task task) {
        queue.add(task);
    }

    @Override
    public Optional<Task> fetch() {
        return Optional.ofNullable(this.queue.remove());
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
