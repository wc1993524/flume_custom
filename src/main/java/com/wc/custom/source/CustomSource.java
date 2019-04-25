package com.wc.custom.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Random;

public class CustomSource extends AbstractSource implements Configurable, PollableSource {

    public Status process() throws EventDeliveryException {
        try {
            while (true) {
                int max = 20;
                int min = 10;
                Random random = new Random();

                int s = random.nextInt(max) % (max - min + 1) + min;
                HashMap<String, String> header = new HashMap<String, String>();
                header.put("id", "自定义source=>"+s);
                this.getChannelProcessor()
                        .processEvent(EventBuilder.withBody("哈喽=>"+s, Charset.forName("UTF-8"), header));

                Thread.sleep(1000);
            }
        }  catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    public void configure(Context context) {

    }
}
