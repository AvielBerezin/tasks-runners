package aviel.task_runners.pending_tasks;

import java.util.Optional;

public interface PendingTasks<MetaData> {
    void insert(MetaData metaData, Runnable task);
    Optional<Task<MetaData>> remove();
    boolean isEmpty();

    record Task<MetaData>(MetaData data,
                          Runnable get) {
        public void run() {get.run();}
    }
}
