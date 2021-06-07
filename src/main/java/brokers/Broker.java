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
import java.util.*;

public class Broker extends BaseRichBolt {

    private OutputCollector collector;
    private List<Publication> publications;
    private Map<Integer,List<Subscription>> taskAndSubscription;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.publications = new LinkedList<>();
        this.taskAndSubscription = new HashMap<>();
    }

    public void execute(Tuple input) {

        Publication receivedPublication = managePublication(input);

        if(receivedPublication != null){
            publications.add(receivedPublication);
        }

        Subscription receivedSubscription = manageSubscriptions(input);

        if (receivedSubscription != null) {
            System.out.println(input.getSourceTask());
            taskAndSubscription.putIfAbsent(input.getSourceTask(),new ArrayList<>());
            taskAndSubscription.get(input.getSourceTask()).add(receivedSubscription);
            collector.emitDirect(input.getSourceTask(),"result",new Values(publications.get(0)));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("PUBLICATION"));
        declarer.declareStream("result", true, new Fields("PUBLICATION"));

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