import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import netscape.javascript.JSObject;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Main {

  public static void main(String[] args)
      throws IOException, InterruptedException, JetStreamApiException, IllegalStateException, TimeoutException, ParseException {

    String userName = "local";
    String password = "vvNGFu3HRk3PMjZHsztlsCZ9MoUwTGzR";
    String host = "0.0.0.0";
    String port = "37733";
    String streamName = "test_stream_1";
    String subjectName = "nextgen_2";
    int BATCH_SIZE = 10;

    Connection nc = Nats.connect("nats://" + userName + ":" + password + "@" + host + ":" + port);
    JetStream js = nc.jetStream();

    ConsumerConfiguration conf = ConsumerConfiguration.builder()
//        .startSequence(885)
        .deliverPolicy(DeliverPolicy.All)
        .build();

    PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
//          .durable("configurator_service_9")
        .stream(streamName)
        .configuration(conf)
        .build();

    boolean status = false;

    int count_msg = 0;
    JetStreamSubscription sub = js.subscribe(subjectName, pullOptions);
    Message msg_mock = null;

    outerloop:
    while (true) {
      List<Message> messages = sub.fetch(BATCH_SIZE, Duration.ofMillis(1000));


      if(messages.size()==0){
        nc.flush(Duration.ofSeconds(1));
        nc.close();
        break;}

      for (Message m : messages) {
        String data_1 = new String(m.getData(), StandardCharsets.UTF_8);

        System.out.println(data_1);        m.ack();
        count_msg++;

        msg_mock =(m==null) ? msg_mock:m;

      }


      String data = new String(msg_mock.getData(), StandardCharsets.UTF_8);
//      JSONObject natMessageJSON = (JSONObject) new JSONParser().parse(data);


      System.out.println("The last  message : "+data);
      System.out.println("Message count is " + messages.size());
      System.out.println("Message count_msg is " + count_msg);

    }

//    JetStreamSubscription sub = js.subscribe(subjectName, pullOptions);
//
////      sub.pullNoWait(BATCH_SIZE);
//
//    int count = 0;
//    Message m = sub.nextMessage(Duration.ofSeconds(1)); // first message
//    while (m != null && count < BATCH_SIZE) {
//      if (m.isJetStream()) {
//        // process message
//        count++;
//        System.out.println("" + count + ". " + m);
//        m.ack();
//        m = sub.nextMessage(
//            Duration.ofMillis(100)); // other messages should already be on the client
//      } else if (m.isStatusMessage()) {
//        // m.getStatus().getCode should == 404
//        // m.getStatus().getCode should be "No Messages"
//        m = null; // drop out of the while (m != null) loop because we are done.
//        // you could also m = sub.nextMessage(Duration.ofMillis(100)) because it will return null
//      }
//    }
//
//    // here, count will be the number of jetstream messages received

//    JetStreamSubscription sub = js.subscribe(subjectName, pullOptions);
//    nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server
//
//    while (true) {
//      // get the next message waiting a maximum of 1 second for it to arrive
//      handleMessage(sub.nextMessage(Duration.ofSeconds(1)));
//    }

//    List<Message> list = new ArrayList<Message>();
//
//    for (int i = 0; i <1000;i++) {
//      JetStreamSubscription sub = js.subscribe(subjectName,pullOptions);
//      Message msg = sub.nextMessage(Duration.ofSeconds(1));
//      list.add(msg);
//      System.out.println(msg.metaData());
//    }
//
//    System.out.println(list.size());
//    nc.close();
  }

  static void handleMessage(Message msg) {
    if (msg == null) {
      // the server had no message for us.
      // Maybe sleep here or do some housekeeping
      System.out.println("Enough");
    } else {
      if (msg.isJetStream()) {
        // do something with the message
        // don't forget to ack based on your consumer AckPolicy configuration
        // or async auto ack setting
        System.out.println(msg);
        msg.ack();
      } else if (msg.isStatusMessage()) {
        // status messages include heartbeat and flow control depending on
        // your consumer configuration
        System.out.println("Status " + msg.getStatus());
      }
    }
  }
}
