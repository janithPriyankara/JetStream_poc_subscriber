import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

//import io.nats.client.ConnectionFactory;


public class Main {

  public static void main(String[] args)
      throws IOException, InterruptedException, JetStreamApiException, IllegalStateException, TimeoutException {

    Connection nc = Nats.connect("nats://local:vvNGFu3HRk3PMjZHsztlsCZ9MoUwTGzR@0.0.0.0:37733");
    JetStream js = nc.jetStream();

    PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
//          .durable("configurator_service_8")  this is optional
        .stream("test_stream")
        .build();

    JetStreamSubscription sub = js.subscribe("test_subject_1", pullOptions);
    List<Message> messages = sub.fetch(10, Duration.ofMillis(1000));
    for (Message m : messages) {
      // process message
      System.out.println(m);
      m.ack();
    }

    System.out.println("Message count is " + messages.size());
    nc.flush(Duration.ofSeconds(1));
    nc.close();


  }


}
