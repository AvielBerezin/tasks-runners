package aviel.task_runners.pending_tasks;

import java.util.Optional;

public interface PendingTasks<Task> {
    void store(Task task);
    Optional<Task> fetch();
    boolean isEmpty();
}
