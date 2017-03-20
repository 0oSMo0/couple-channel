package im.dashen;

import junit.framework.TestCase;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.fest.reflect.core.Reflection.field;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CustomizeStressSourceTest {

    private ChannelProcessor mockProcessor;

    @Before
    public void setUp() {
        mockProcessor = mock(ChannelProcessor.class);
    }

    private Event getEvent(CustomizeStressSource source) {
        return field("event").ofType(Event.class).in(source).get();

    }

    @SuppressWarnings("unchecked")
    private List<Event> getLastProcessedEventList(CustomizeStressSource source) {
        return field("eventBatchListToProcess").ofType(List.class).in(source).get();
    }

    private CounterGroup getCounterGroup(CustomizeStressSource source) {
        return field("counterGroup").ofType(CounterGroup.class).in(source).get();
    }


    @Test
    public void testStressSources() throws InterruptedException, EventDeliveryException {
        CustomizeStressSource source = new CustomizeStressSource();
        source.setChannelProcessor(mockProcessor);
        Context context = new Context();
        context.put("eventContent", "target/test-classes/sources/event-content.txt");
        context.put("maxTotalEvents", "40");
        context.put("batchSize", "10");
        source.configure(context);
        source.start();
    }
}