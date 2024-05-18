package aviel.task_runners;

/**
 * {@link ThisShouldNotHappen} is thrown when on algorithmic point of view some given state that is technically possible but logically should not be reached.
 * If this is thrown it means that there is a bug in the library itself.
 * This can mean that there is some invariant that is essential for correctness that the library failed to ensure.
 */
public class ThisShouldNotHappen extends RuntimeException {
    public ThisShouldNotHappen() {
    }

    public ThisShouldNotHappen(String message) {
        super(message);
    }

    public ThisShouldNotHappen(String message, Throwable cause) {
        super(message, cause);
    }

    public ThisShouldNotHappen(Throwable cause) {
        super(cause);
    }
}
