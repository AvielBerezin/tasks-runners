package aviel.task_runners;

import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * a doubly linked node
 */
public class DLNode<Value> {
    private final UnaryOperator<DLNode<Value>> transformer;

    private final Value value;
    private DLNode<Value> prev;
    private DLNode<Value> next;

    public DLNode(DLNode<Value> other) {
        transformer = other.transformer;

        value = other.value;
        prev = other.prev;
        next = other.next;
    }

    private DLNode(UnaryOperator<DLNode<Value>> transformer,
                   Value value, DLNode<Value> prev, DLNode<Value> next) {
        this.transformer = transformer;

        this.value = value;
        this.prev = prev;
        this.next = next;
    }

    private static <Value> DLNode<Value> middle(UnaryOperator<DLNode<Value>> transformer,
                                                Value value, DLNode<Value> prev, DLNode<Value> next) {
        DLNode<Value> middle = transformer.apply(new DLNode<>(transformer, value, prev, next));
        prev.next = middle;
        next.prev = middle;
        return middle;
    }

    private static <Value> DLNode<Value> first(UnaryOperator<DLNode<Value>> transformer,
                                               Value value, DLNode<Value> next) {
        DLNode<Value> first = transformer.apply(new DLNode<>(transformer, value, null, next));
        next.prev = first;
        return first;
    }

    private static <Value> DLNode<Value> last(UnaryOperator<DLNode<Value>> transformer,
                                              Value value, DLNode<Value> prev) {
        DLNode<Value> last = transformer.apply(new DLNode<>(transformer, value, prev, null));
        prev.next = last;
        return last;
    }

    private static <Value> DLNode<Value> only(UnaryOperator<DLNode<Value>> transformer,
                                              Value value) {
        return transformer.apply(new DLNode<>(transformer, value, null, null));
    }

    public static class Generator<Value> {
        private final UnaryOperator<DLNode<Value>> transformer;

        public Generator(UnaryOperator<DLNode<Value>> transformer) {
            this.transformer = transformer;
        }

        public DLNode<Value> middle(Value value, DLNode<Value> prev, DLNode<Value> next) {
            return DLNode.middle(transformer, value, prev, next);
        }

        public DLNode<Value> first(Value value, DLNode<Value> next) {
            return DLNode.first(transformer, value, next);
        }

        public DLNode<Value> last(Value value, DLNode<Value> prev) {
            return DLNode.last(transformer, value, prev);
        }

        public DLNode<Value> only(Value value) {
            return DLNode.only(transformer, value);
        }
    }

    public final Optional<DLNode<Value>> prev() {
        return Optional.ofNullable(prev);
    }

    public final Optional<DLNode<Value>> next() {
        return Optional.ofNullable(next);
    }

    public final Value value() {
        return value;
    }

    public void unlink() {
        if (prev != null) {
            prev.next = next;
        }
        if (next != null) {
            next.prev = prev;
        }
        prev = next = null;
    }

    public DLNode<Value> pushNext(Value value) {
        if (next != null) {
            return middle(transformer,
                          value, this, next);
        }
        return last(transformer,
                    value, this);
    }

    public DLNode<Value> pushPrev(Value value) {
        if (prev != null) {
            return middle(transformer,
                          value, prev, this);
        }
        return first(transformer,
                     value, this);
    }
}
