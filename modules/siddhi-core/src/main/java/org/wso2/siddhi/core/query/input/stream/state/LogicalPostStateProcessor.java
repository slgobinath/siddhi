package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.query.api.execution.query.input.state.LogicalStateElement;

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
            for (StreamPostStateProcessor processor : streamPostStateProcessors) {
                if (!processor.newAndEveryStateEventList.isEmpty()) {
                    events = processor.newAndEveryStateEventList;
                    break;
                }
            }
        } else {
            // AND
            events = streamPostStateProcessors.get(streamPostStateProcessors.size() - 1).newAndEveryStateEventList;
        }
        return events;
    }

}
