package aviel.task_runners;

import org.junit.Test;

import java.util.List;

public class ExposedDLListTest {
    @Test
    public void directSizingTest() {
        ExposedDLList<Integer> ints = new ExposedDLList<>();
        ints.addLast(10);
        ints.addLast(20);
        ints.addLast(30);
        assert ints.size() == 3;
        ints.removeLast();
        assert ints.size() == 2;
        assert ints.getLast().isPresent();
        assert ints.getLast().get().value().equals(20);
    }

    @Test
    public void unlinkSizingTest() {
        ExposedDLList<Integer> ints = new ExposedDLList<>();
        ints.addLast(10);
        DLNode<Integer> node = ints.addLast(20);
        ints.addLast(30);
        assert ints.size() == 3;
        node.unlink();
        assert ints.size() == 2;
        node.unlink();
        assert ints.size() == 2;
        assert ints.toList()
                   .stream()
                   .map(DLNode::value)
                   .toList()
                   .equals(List.of(10, 30));
    }

    @Test
    public void pushSizingTest() {
        ExposedDLList<Integer> ints = new ExposedDLList<>();
        ints.addLast(10);
        DLNode<Integer> node = ints.addLast(20);
        ints.addLast(30);
        assert ints.size() == 3;
        node.pushNext(40).pushPrev(50);
        assert ints.size() == 5;
        assert ints.toList()
                   .stream()
                   .map(DLNode::value)
                   .toList()
                   .equals(List.of(10, 20, 50, 40, 30));
    }
}