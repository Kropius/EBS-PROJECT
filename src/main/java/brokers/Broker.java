package brokers;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import publishers.model.Publication;
import subscriber.model.Subscription;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Broker extends BaseRichBolt {

    private OutputCollector collector;
    private List<Publication> publications;
    private List<Subscription> subscriptions;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.publications = new LinkedList<>();
    }

    public void execute(Tuple input) {
        Publication receivedPublication = managePublication(input);
        publications.add(receivedPublication);
        collector.emit(new Values(receivedPublication));

        Subscription receivedSubscription = manageSubscriptions(input);
        if (receivedSubscription != null) {
            System.out.println(receivedSubscription.getConstraints());
            subscriptions.add(receivedSubscription);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("PUBLICATION"));
    }

    private Publication createPublication(LinkedHashMap tuple) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        return Publication.builder()
                .stationId((Long) tuple.get("statotionId"))
                .city((String) tuple.get("city"))
                .date(LocalDate.parse((String) tuple.get("date"), formatter))
                .rainChance((Double) tuple.get("rainChance"))
                .temperature(((Integer) tuple.get("temperature")).longValue())
                .windSpeed(((Integer) tuple.get("windSpeed")).longValue())
                .build();

    }

    private Publication managePublication(Tuple input) {
        try {
            LinkedHashMap receivedPublication = ((LinkedHashMap) input.getValueByField("PUBLICATION"));
            Publication publication = createPublication(receivedPublication);
            System.out.println(publication);
            return publication;
        } catch (RuntimeException e) {

        }
        return null;
    }

    private Subscription manageSubscriptions(Tuple input) {
        try {
            Subscription receivedSubscription = (Subscription) input.getValueByField("SUBSCRIPTION");
            return receivedSubscription;
        } catch (RuntimeException e) {

        }
        return null;
    }

}