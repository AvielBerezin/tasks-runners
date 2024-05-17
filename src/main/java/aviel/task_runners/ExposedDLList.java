package aviel.task_runners;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A linked list implementation that exposes its double linked nodes
 */
public class ExposedDLList<Value> {
    private final DLNode.Generator<Value> nodeGenerator;

    private DLNode<Value> first;
    private DLNode<Value> last;
    private int size;

    public ExposedDLList() {
        size = 0;
        nodeGenerator = new DLNode.Generator<>(valueDLNode -> new DLNode<>(valueDLNode) {
            boolean linked = true;

            @Override
            public void unlink() {
                if (linked) {
                    linked = false;
                    if (first == this) {
                        first = first.next().orElse(null);
                    }
                    if (last == this) {
                        last = last.prev().orElse(null);
                    }
                    super.unlink();
                    size--;
                }
            }

            @Override
            public DLNode<Value> pushNext(Value value) {
                DLNode<Value> next = super.pushNext(value);
                if (linked) {
                    size++;
                }
                return next;
            }

            @Override
            public DLNode<Value> pushPrev(Value value) {
                DLNode<Value> prev = super.pushPrev(value);
                if (linked) {
                    size++;
                }
                return prev;
            }
        });
    }

    public DLNode<Value> addLast(Value value) {
        if (last == null) {
            first = last = nodeGenerator.only(value);
        } else {
            last = nodeGenerator.last(value, last);
        }
        size++;
        return last;
    }

    public Optional<DLNode<Value>> getLast() {
        return Optional.ofNullable(last);
    }

    public Optional<Value> removeLast() {
        if (last == null) {
            return Optional.empty();
        } else {
            DLNode<Value> prevLast = last;
            prevLast.prev().ifPresentOrElse(node -> last = node,
                                            () -> last = first = null);
            prevLast.unlink();
            return Optional.of(prevLast.value());
        }
    }

    public DLNode<Value> addFirst(Value value) {
        if (first == null) {
            first = last = nodeGenerator.only(value);
        } else {
            first = nodeGenerator.first(value, first);
        }
        size++;
        return first;
    }

    public Optional<DLNode<Value>> getFirst() {
        return Optional.ofNullable(first);
    }

    public Optional<Value> removeFirst() {
        if (first == null) {
            return Optional.empty();
        } else {
            DLNode<Value> prevFirst = first;
            prevFirst.next().ifPresentOrElse(node -> first = node,
                                             () -> first = last = null);
            prevFirst.unlink();
            return Optional.of(prevFirst.value());
        }
    }

    public int size() {
        return size;
    }

    public List<DLNode<Value>> toList() {
        ArrayList<DLNode<Value>> nodes = new ArrayList<>(size);
        Optional<DLNode<Value>> node = Optional.of(first);
        while (node.isPresent()) {
            nodes.add(node.get());
            node = node.get().next();
        }
        return nodes;
    }
}
