package brokers;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import publishers.model.Publication;

public class Broker extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    public void execute(Tuple input) {
        LinkedHashMap tuple = ((LinkedHashMap) input.getValue(0));
        System.out.println(createPublication(tuple));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("PUBLICATION"));
    }

    private Publication createPublication(LinkedHashMap tuple) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        Publication publication = Publication.builder()
                .stationId((Long) tuple.get("statotionId"))
                .city((String) tuple.get("city"))
                .date(LocalDate.parse((String) tuple.get("date"), formatter))
                .rainChance((Double) tuple.get("rainChance"))
                .temperature(((Integer)tuple.get("temperature")).longValue())
                .windSpeed(((Integer)tuple.get("windSpeed")).longValue())
                .build();
        return publication;
    }

}