package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.state.StateEvent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by gobinath on 5/24/17.
 */
public class EventStore implements Iterable<StateEvent> {

    private final List<StateEvent>[] lists;
    private final int sublistSize;

    private EventRemovalListener eventRemovalListener;

    public EventStore() {
        this(new LinkedList<>());
    }

    public EventStore(List<StateEvent>... lists) {
        this.lists = lists;
        this.sublistSize = lists.length;
    }

    public void setEventRemovalListener(EventRemovalListener eventRemovalListener) {
        this.eventRemovalListener = eventRemovalListener;
    }

    public boolean isEmpty() {
        for (List<StateEvent> list : lists) {
            if (!list.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Iterator<StateEvent> iterator() {
        return null;
    }

    @FunctionalInterface
    public interface EventRemovalListener {
        void onEventRemoved(StateEvent event);
    }

    private class EventStoreIterator implements Iterator<StateEvent> {
        private final Iterator<StateEvent>[] iterators;
        private final int size;
        private int index = 0;
        private StateEvent currentEvent;

        public EventStoreIterator(Iterator<StateEvent>... iterators) {
            this.iterators = iterators;
            this.size = iterators.length;
        }

        @Override
        public boolean hasNext() {
            boolean has = iterators[index].hasNext();
            while (!has) {
                index++;
                if (index == size) {
                    index--;
                    break;
                }
                has = iterators[index].hasNext();
            }
            return has;
        }

        @Override
        public StateEvent next() {
            this.currentEvent = iterators[index].next();
            return currentEvent;
        }

        @Override
        public void remove() {
            iterators[index].remove();
            if (eventRemovalListener != null) {
                eventRemovalListener.onEventRemoved(this.currentEvent);
            }
        }
    }

}
