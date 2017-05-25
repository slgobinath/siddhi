package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.query.api.execution.query.input.state.LogicalStateElement;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by gobinath on 5/21/17.
 */
public class LogicalPostStateProcessor extends StreamPostStateProcessor {

    private LogicalStateElement.Type logicalType;

    private List<StreamPostStateProcessor> streamPostStateProcessors = new LinkedList<>();

    public LogicalPostStateProcessor(LogicalStateElement.Type logicalType) {
        this.logicalType = logicalType;
    }

    @Override
    public void setNextStatePreProcessor(PreStateProcessor preStateProcessor) {
        super.setNextStatePreProcessor(preStateProcessor);
    }

    @Override
    public void setEndOfEvery(boolean endOfEvery) {
        super.setEndOfEvery(endOfEvery);
        for (StreamPostStateProcessor processor : this.streamPostStateProcessors) {
            processor.setEndOfEvery(endOfEvery);
        }
    }

    public void addStreamPostStateProcessor(StreamPostStateProcessor processor) {
        this.streamPostStateProcessors.add(processor);
//        if(logicalType == LogicalStateElement.Type.OR) {
//            processor.newAndEveryStateEventList = this.newAndEveryStateEventList;
//            processor.pendingStateEventList = this.pendingStateEventList;
//        }
    }


    @Override
    public boolean isEventReturned() {
        if (nextStatePerProcessor != null) {
            return false;
        }
        if (logicalType == LogicalStateElement.Type.OR) {
            for (StreamPostStateProcessor processor : streamPostStateProcessors) {
                if (!processor.newAndEveryStateEventList.isEmpty()) {
                    return true;
                }
            }
            return false;
//            return !newAndEveryStateEventList.isEmpty();
        } else if (logicalType == LogicalStateElement.Type.AND) {
            boolean consume = true;
            for (StreamPostStateProcessor processor : streamPostStateProcessors) {
                if (processor.newAndEveryStateEventList.isEmpty() && processor.pendingStateEventList.isEmpty()) {
                    consume = false;
                    break;
                }
            }
            return consume;
        }
        return false;
    }


    /**
     * Returns a {@link List} of {@link StateEvent}s stored in the processor.
     *
     * @return a {@link List} of {@link StateEvent}s
     */
    @Override
    public List<StateEvent> events() {
        List<StateEvent> events = new LinkedList<>();
        if (logicalType == LogicalStateElement.Type.OR) {
            /*if (false) {
                for (StreamPostStateProcessor processor : streamPostStateProcessors) {
                    if (!processor.newAndEveryStateEventList.isEmpty()) {
                        events.addAll(processor.newAndEveryStateEventList);
                    }
                }
            } else {*/
            for (StreamPostStateProcessor processor : streamPostStateProcessors) {
                if (!processor.newAndEveryStateEventList.isEmpty()) {
                    events = processor.newAndEveryStateEventList;
                    break;
                }
            }
            //}
//            return this.pendingStateEventList;
        } else {
            // AND
            events = streamPostStateProcessors.get(streamPostStateProcessors.size() - 1).newAndEveryStateEventList;
        }
        return events;
    }


//    @Override
//    public List<StateEvent> events() {
//        List<StateEvent> events = new LinkedList<>();
//        if (logicalType == LogicalStateElement.Type.OR) {
//            if (endOfEvery) {
//                List<StateEvent>[] lists = new List[streamPostStateProcessors.size()];
//                for (int i = 0; i < lists.length; i++) {
//                    lists[i] = streamPostStateProcessors.get(i).newAndEveryStateEventList;
//                }
//                events = new MyList(lists);
//            } else {
//                for (StreamPostStateProcessor processor : streamPostStateProcessors) {
//                    if (!processor.newAndEveryStateEventList.isEmpty()) {
//                        events = processor.newAndEveryStateEventList;
//                        break;
//                    }
//                }
//            }
//        } else {
//            // AND
//            events = streamPostStateProcessors.get(streamPostStateProcessors.size() - 1).newAndEveryStateEventList;
//        }
//        return events;
//    }

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
