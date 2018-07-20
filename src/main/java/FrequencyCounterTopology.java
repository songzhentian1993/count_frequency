import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by James on 2018/7/18.
 */
public class FrequencyCounterTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new LogParserSpout());
        builder.setSpout("timetaskspout", new TimeTaskSpout());
        builder.setBolt("bolt1", new NginxLogSplitter())
                .shuffleGrouping("spout")
                .shuffleGrouping("timetaskspout");
        builder.setBolt("bolt2", new FrequencyCounterBolt())
                .fieldsGrouping("bolt1", "udidValStream", new Fields("udidVal"))
                .fieldsGrouping("bolt1", "ipValStream", new Fields("ipVal"))
                .shuffleGrouping("timetaskspout");
        Config conf = new Config();
        conf.setDebug(false);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("countfreq", conf, builder.createTopology());
            //Thread.sleep(60000);
            //cluster.shutdown();
        }
    }
}
