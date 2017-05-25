package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.state.StateEvent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by gobinath on 5/24/17.
 */
public class StateEventStore implements Iterable<StateEvent> {

    private List<StateEvent> list;
    private EventRemovalListener eventRemovalListener;

    public StateEventStore() {
        this.list = new LinkedList<>();
    }

    public void setEventRemovalListener(EventRemovalListener eventRemovalListener) {
        this.eventRemovalListener = eventRemovalListener;
    }

    public boolean add(StateEvent event) {
        this.list.add(event);
        return true;
    }

    public List<StateEvent> toList() {
        return this.list;
    }

    @Override
    public Iterator<StateEvent> iterator() {
        return new StateEventStoreIterator(this.list.iterator());
    }

    @FunctionalInterface
    public interface EventRemovalListener {
        void onEventRemoved(StateEvent event);
    }

    private class StateEventStoreIterator implements Iterator<StateEvent> {

        private final Iterator<StateEvent> iterator;
        private StateEvent currentEvent;

        public StateEventStoreIterator(Iterator<StateEvent> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public StateEvent next() {
            this.currentEvent = this.iterator.next();
            return this.currentEvent;
        }

        @Override
        public void remove() {
            this.iterator.remove();
            if (eventRemovalListener != null) {
                eventRemovalListener.onEventRemoved(this.currentEvent);
            }
        }
    }

}
