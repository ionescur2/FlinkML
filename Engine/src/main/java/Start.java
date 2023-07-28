import consumer_builder.Consumer;
import consumers.ThreatMatrix;

/**
 * -----------------------
 *
 * @author: Radu Ionescu
 * 28 iulie 2023
 * -----------------------
 */
public class Start {
    public static void main(String[] args) {
        System.out.println("hello");
        Consumer tmx = new ThreatMatrix().getConsumer();
        System.out.println(tmx);
    }
}
