package aviel.task_runners.pending_tasks;

import java.util.Optional;

public interface PendingTasks<Task> {
    void insert(Task task);
    Optional<Task> remove();
    boolean isEmpty();
}
