package im.dashen;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class StressSource extends AbstractPollableSource implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(StressSource.class);

    private CounterGroup counterGroup;
    private byte[] content;
    private long maxTotalEvents;
    private long maxSuccessfulEvents;
    private int batchSize;
    private long lastSent = 0;
    private Event event;
    private List<Event> eventBatchList;
    private List<Event> eventBatchListToProcess;

    private Long startTime;
    private Boolean hasStopSent = true;

    public StressSource() {
        counterGroup = new CounterGroup();
        startTime = System.currentTimeMillis();
    }

    @Override
    protected void doConfigure(Context context) throws FlumeException {
        maxTotalEvents = context.getLong("maxTotalEvents", -1L);
        maxSuccessfulEvents = context.getLong("maxSuccessfulEvents", -1L);
        batchSize = context.getInteger("batchSize", 1);
        String eventContent = context.getString("eventContent");
        Preconditions.checkState(eventContent != null, "Missing params: eventContent");
        prepEventData(eventContent);
    }

    private void prepEventData(String eventContent) {
        try {
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(eventContent));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int size = 0;
            while ((size = in.read(buf)) != -1) {
                bos.write(buf, 0, size);
            }
            content = bos.toByteArray();
            in.close();
            bos.close();
        } catch (Exception e) {
            logger.error("Can not read file: {}", eventContent);
        }

        if (batchSize > 1) {
            eventBatchList = new ArrayList<>();

            for (int i = 0; i < batchSize; ++i) {
                eventBatchList.add(EventBuilder.withBody(content));
            }
        } else {
            event = EventBuilder.withBody(content);
        }
    }

    @Override
    protected Status doProcess() throws EventDeliveryException {
        long totalEventSent = counterGroup.addAndGet("events.total", lastSent);
        if ((maxTotalEvents >= 0 && totalEventSent >= maxTotalEvents) ||
                (maxSuccessfulEvents >= 0 && counterGroup.get("events.successful") >= maxSuccessfulEvents)) {
            if (hasStopSent) {
                logger.info("Elapsed time: {} ms", System.currentTimeMillis() - startTime);
                hasStopSent = false;
            }
            return Status.BACKOFF;
        }
        try {
            lastSent = batchSize;

            if (batchSize == 1) {
                getChannelProcessor().processEvent(event);
            } else {
                long eventsLeft = maxTotalEvents - totalEventSent;

                if (maxTotalEvents >= 0 && eventsLeft < batchSize) {
                    eventBatchListToProcess = eventBatchList.subList(0, (int) eventsLeft);
                } else {
                    eventBatchListToProcess = eventBatchList;
                }
                lastSent = eventBatchListToProcess.size();
                getChannelProcessor().processEventBatch(eventBatchListToProcess);
            }

            counterGroup.addAndGet("events.successful", lastSent);
        } catch (ChannelException ex) {
            counterGroup.addAndGet("events.failed", lastSent);
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override
    protected void doStart() throws FlumeException {
        logger.info("Stress source doStart finished");
    }

    @Override
    protected void doStop() throws FlumeException {
        logger.info("Stress source do stop. Metrics:{}", counterGroup);
    }
}
