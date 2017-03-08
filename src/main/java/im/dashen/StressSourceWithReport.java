package im.dashen;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractPollableSource;
import org.apache.flume.source.StressSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StressSourceWithReport extends AbstractPollableSource implements Configurable{
    private static final Logger logger = LoggerFactory.getLogger(StressSourceWithReport.class);

    private AbstractPollableSource stressProcess;

    private Long startTime;

    public StressSourceWithReport() {
        stressProcess = new StressSource();
        startTime = System.currentTimeMillis();
    }

    protected Status doProcess() throws EventDeliveryException {
        PollableSource.Status status = stressProcess.process();
        if (status == Status.BACKOFF) {
            logger.info("Elapsed: {}", System.currentTimeMillis() - startTime);
        }
        return status;
    }

    protected void doConfigure(Context context) throws FlumeException {
        stressProcess.configure(context);
    }

    protected void doStart() throws FlumeException {
        stressProcess.start();
    }

    protected void doStop() throws FlumeException {
        stressProcess.stop();
    }
}
