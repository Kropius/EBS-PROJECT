
import brokers.Broker;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import publishers.Publisher;

public class App {
    private static final String SPOUT_ID = "source_text_spout";
    private static final String BROKER_ID = "broker_bolt";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        Publisher spout = new Publisher();
        Broker splitbolt = new Broker();

        builder.setSpout(SPOUT_ID, spout);
        builder.setBolt(BROKER_ID, splitbolt).shuffleGrouping(SPOUT_ID);

        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();

        // fine tuning
        Config configuration = init();

        cluster.submitTopology("count_topology", configuration, topology);

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        cluster.killTopology("count_topology");
        cluster.shutdown();

    }

    private static Config init() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
        config.put(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE, 1);
        config.put("publications","publications.json");
        return config;

    }


}