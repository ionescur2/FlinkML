package consumers;

import consumer_builder.Consumer;

/**
 * -----------------------
 *
 * @author: Radu Ionescu
 * 28 iulie 2023
 * -----------------------
 */
public class ThreatMatrix {

    Consumer tmxConsumer = new Consumer.ConsumerBuilder("topic1", "servers.2").build();
    public Consumer getConsumer(){
        return tmxConsumer;
    }
}
