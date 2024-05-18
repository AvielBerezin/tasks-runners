package aviel.task_runners;

public interface KeyedTask<Key> extends Runnable {
    Key key();

    static <Key> KeyedTask<Key> of(Key key, Runnable runnable) {
        return new KeyedTask<>() {
            @Override
            public Key key() {
                return key;
            }

            @Override
            public void run() {
                runnable.run();
            }
        };
    }
}
