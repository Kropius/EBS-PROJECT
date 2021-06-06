package publishers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import publishers.model.Publication;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Publisher extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private List<Publication> publications = new LinkedList<>();
    private int publicationNumber = 0;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        init((String) map.get("publications"));
    }

    public void nextTuple() {

        publicationNumber++;


        this.collector.emit(new Values(publications.get(publicationNumber)));

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("PUBLICATION"));
    }

    private void init(String sourceFile) {
        System.out.println(String.format("Received filename: %s", sourceFile));
        Thread.currentThread().getContextClassLoader().getResourceAsStream(sourceFile);
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(sourceFile)) {
            ObjectMapper mapper = new ObjectMapper();
            publications = mapper.readValue(in,
                    List.class);
            System.out.println(String.format("Read %d publications", publications.size()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}