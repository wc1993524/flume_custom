package com.wc.custom.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class CustomSink extends AbstractSink implements Configurable {
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Transaction trans = null;
        try {
            Channel channel = getChannel();
            trans = channel.getTransaction();
            trans.begin();
            for (int i = 0; i < 100; i++) {
                Event event = channel.take();
                if(event == null){
                    status = status.BACKOFF;
                    break;
                }else {
                    String body = new String(event.getBody());
                    System.out.println("自定义Sink==>"+body);
                }
            }
            trans.commit();
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            if(trans!=null){
                trans.close();
            }
        }
        return status;
    }

    public void configure(Context context) {

    }
}
