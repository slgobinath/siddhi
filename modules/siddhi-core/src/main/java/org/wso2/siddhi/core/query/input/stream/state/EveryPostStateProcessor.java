package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.state.StateEvent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by gobinath on 5/21/17.
 */
public class EveryPostStateProcessor extends StreamPostStateProcessor {


    public EveryPostStateProcessor() {

    }

    @Override
    public void setNextStatePreProcessor(PreStateProcessor preStateProcessor) {
        super.setNextStatePreProcessor(preStateProcessor);
    }

    public void setStreamPostStateProcessor(StreamPostStateProcessor processor) {

    }


    @Override
    public boolean isEventReturned() {
        if (nextStatePerProcessor != null) {
            return false;
        }
        return true;

    }


    /**
     * Returns a {@link List} of {@link StateEvent}s stored in the processor.
     *
     * @return a {@link List} of {@link StateEvent}s
     */
    @Override
    public List<StateEvent> events() {
        return super.events();
    }


    private class MyList extends LinkedList<StateEvent> {

        private final int size;
        private List<StateEvent>[] sublists;

        public MyList(List<StateEvent>... lists) {
            this.sublists = lists;
            this.size = lists.length;
        }

        @Override
        public Iterator<StateEvent> iterator() {
            Iterator<StateEvent>[] iterators = new Iterator[size];
            for (int i = 0; i < size; i++) {
                iterators[i] = sublists[i].iterator();
            }
            return new MyIterator(iterators);
        }
    }

    private class MyIterator implements Iterator<StateEvent> {
        private final Iterator<StateEvent>[] iterators;
        private final int size;
        private int index = 0;

        public MyIterator(Iterator<StateEvent>... iterators) {
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
            return iterators[index].next();
        }

        @Override
        public void remove() {
            iterators[index].remove();
        }
    }
}
