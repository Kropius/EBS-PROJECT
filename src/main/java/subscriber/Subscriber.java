package subscriber;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import subscriber.model.SubscriptionContainer;

import java.io.InputStream;
import java.util.Map;

public class Subscriber extends BaseRichBolt {

    private OutputCollector collector;
    private SubscriptionContainer subscriptionContainer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        init("subscriptions.json");
        collector.emit(new Values(subscriptionContainer.getSubscriptions().get(0)));
    }

    @Override
    public void execute(Tuple input) {

        System.out.println("Received publication " + input.getValueByField("PUBLICATION"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("SUBSCRIPTION"));
    }

    private void init(String sourceFile) {
        System.out.println(String.format("Received filename: %s", sourceFile));

        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(sourceFile)) {
            ObjectMapper mapper = new ObjectMapper();
            subscriptionContainer = mapper.readValue(in,
                    SubscriptionContainer.class);
            System.out.println(String.format("Read %d subscriptions", subscriptionContainer.getSubscriptions().size()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
