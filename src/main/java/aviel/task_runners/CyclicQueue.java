package aviel.task_runners;

import java.util.Optional;

public class CyclicQueue<Element> {
    private final int maxSize;
    private final Element[] data;
    private Orientation orientation;

    private record Orientation(int maxSize, int start, int size) {
        public Orientation incrementStart() {
            return new Orientation(maxSize, (start + 1) % maxSize, size);
        }

        public Orientation incrementSize() {
            return new Orientation(maxSize, start, size + 1);
        }

        public Orientation decrementSizeIfPossible() {
            if (size == 0) {
                return this;
            }
            return new Orientation(maxSize, start, size - 1);
        }
    }

    public CyclicQueue(int maxSize) {
        this.maxSize = maxSize;
        @SuppressWarnings("unchecked")
        Element[] data = (Element[]) new Object[maxSize];
        this.data = data;
        orientation = new Orientation(maxSize, 0, 0);
    }

    public int size() {
        return orientation.size;
    }

    public synchronized Optional<Element> put(Element element) {
        int position = (orientation.start + orientation.size) % maxSize;
        Element datum = data[position];
        data[position] = element;
        orientation = (orientation.size < maxSize
                       ? orientation.incrementSize()
                       : orientation.incrementStart());
        return Optional.ofNullable(datum);
    }

    public synchronized Optional<Element> pop() {
        int position = orientation.start;
        Element datum = data[position];
        data[position] = null;
        orientation = orientation.incrementStart().decrementSizeIfPossible();
        return Optional.ofNullable(datum);
    }

    public synchronized Optional<Element> peak() {
        return Optional.ofNullable(data[orientation.start]);
    }
}
