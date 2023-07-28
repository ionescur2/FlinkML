package consumer_builder;

/**
 * -----------------------
 *
 * @author: Radu Ionescu
 * 28 iulie 2023
 * -----------------------
 */
public class Consumer {
    private String topic;
    private String bootStrapServers;

    private Consumer(ConsumerBuilder consumerBuilder){
        this.topic = consumerBuilder.topic;
        this.bootStrapServers = consumerBuilder.bootStrapServers;
    }

    @Override
    public String toString() {
        return "Consumer{" +
                "topic='" + topic + '\'' +
                ", bootStrapServers='" + bootStrapServers + '\'' +
                '}';
    }

    public static class ConsumerBuilder {
        private String topic;
        private String bootStrapServers;

        public ConsumerBuilder(String topic, String bootStrapServers){
            this.topic = topic;
            this.bootStrapServers = bootStrapServers;
        }

        public ConsumerBuilder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public ConsumerBuilder setBootStrapServers(String bootStrapServers){
            this.bootStrapServers = bootStrapServers;
            return this;
        }

        public Consumer build(){
            return new Consumer(this);
        }

    }
}
