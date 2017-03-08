package im.dashen;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.source.StressSource;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.fest.reflect.core.Reflection.field;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StressSourceWithReportTest {
    private ChannelProcessor mockProcessor;

    @Before
    public void setUp() {
        mockProcessor = mock(ChannelProcessor.class);
    }

    private Event getEvent(StressSource source) {
        return field("event").ofType(Event.class)
                .in(source)
                .get();
    }

    @SuppressWarnings("unchecked")
    private List<Event> getLastProcessedEventList(StressSource source) {
        return field("eventBatchListToProcess").ofType(List.class).in(source).get();
    }

    private CounterGroup getCounterGroup(StressSource source) {
        return field("counterGroup").ofType(CounterGroup.class).in(source).get();
    }


    @Test
    public void testMaxTotalEvents() throws InterruptedException,
            EventDeliveryException {
        StressSource source = new StressSource();
        source.setChannelProcessor(mockProcessor);
        Context context = new Context();
        context.put("maxTotalEvents", "35");
        source.configure(context);
        source.start();

        for (int i = 0; i < 50; i++) {
            source.process();
        }
        verify(mockProcessor, times(35)).processEvent(getEvent(source));
    }

    @Test
    public void testBatchEvents() throws InterruptedException,
            EventDeliveryException {
        StressSource source = new StressSource();
        source.setChannelProcessor(mockProcessor);
        Context context = new Context();
        context.put("maxTotalEvents", "35");
        context.put("batchSize", "10");
        source.configure(context);
        source.start();

        for (int i = 0; i < 50; i++) {
            if (source.process() == PollableSource.Status.BACKOFF) {
                TestCase.assertTrue("Source should have sent all events in 4 batches", i == 4);
                break;
            }
            if (i < 3) {
                verify(mockProcessor,
                        times(i + 1)).processEventBatch(getLastProcessedEventList(source));
            } else {
                verify(mockProcessor,
                        times(1)).processEventBatch(getLastProcessedEventList(source));
            }
        }
        long successfulEvents = getCounterGroup(source).get("events.successful");
        TestCase.assertTrue("Number of successful events should be 35 but was " +
                successfulEvents, successfulEvents == 35);
        long failedEvents = getCounterGroup(source).get("events.failed");
        TestCase.assertTrue("Number of failure events should be 0 but was " +
                failedEvents, failedEvents == 0);
    }

    @Test
    public void testBatchEventsWithoutMatTotalEvents() throws InterruptedException,
            EventDeliveryException {
        StressSource source = new StressSource();
        source.setChannelProcessor(mockProcessor);
        Context context = new Context();
        context.put("batchSize", "10");
        source.configure(context);
        source.start();

        for (int i = 0; i < 10; i++) {
            Assert.assertFalse("StressSource with no maxTotalEvents should not return " +
                    PollableSource.Status.BACKOFF, source.process() == PollableSource.Status.BACKOFF);
        }
        verify(mockProcessor,
                times(10)).processEventBatch(getLastProcessedEventList(source));

        long successfulEvents = getCounterGroup(source).get("events.successful");
        TestCase.assertTrue("Number of successful events should be 100 but was " +
                successfulEvents, successfulEvents == 100);

        long failedEvents = getCounterGroup(source).get("events.failed");
        TestCase.assertTrue("Number of failure events should be 0 but was " +
                failedEvents, failedEvents == 0);
    }

    @Test
    public void testMaxSuccessfulEvents() throws InterruptedException,
            EventDeliveryException {
        StressSource source = new StressSource();
        source.setChannelProcessor(mockProcessor);
        Context context = new Context();
        context.put("maxSuccessfulEvents", "35");
        source.configure(context);
        source.start();

        for (int i = 0; i < 10; i++) {
            source.process();
        }

        // 1 failed call, 10 successful
        doThrow(new ChannelException("stub")).when(
                mockProcessor).processEvent(getEvent(source));
        source.process();
        doNothing().when(mockProcessor).processEvent(getEvent(source));
        for (int i = 0; i < 10; i++) {
            source.process();
        }

        // 1 failed call, 50 successful
        doThrow(new ChannelException("stub")).when(
                mockProcessor).processEvent(getEvent(source));
        source.process();
        doNothing().when(mockProcessor).processEvent(getEvent(source));
        for (int i = 0; i < 50; i++) {
            source.process();
        }

        // We should have called processEvent(evt) 37 times, twice for failures
        // and twice for successful events.
        verify(mockProcessor, times(37)).processEvent(getEvent(source));
    }
}