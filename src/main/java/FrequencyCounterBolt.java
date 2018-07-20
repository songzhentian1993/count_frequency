import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by James on 2018/7/18.
 */
public class FrequencyCounterBolt extends BaseBasicBolt {
    private Map<String, Integer> frequencyCountMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        frequencyCountMap = new HashMap<>();//初始容量设置为多少
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(isTickTuple(tuple)) {
            DumpMemToFile("frequency_data.txt");
        } else {
            if(tuple.getSourceStreamId().equals("udidValStream")) {
                String udidVal = tuple.getStringByField("udidVal");
                int udidValCounter = (frequencyCountMap.get(udidVal) == null)?0:frequencyCountMap.get(udidVal);
                frequencyCountMap.put(udidVal, udidValCounter + 1);
            }
            if(tuple.getSourceStreamId().equals("ipValStream")) {
                String ipVal = tuple.getStringByField("ipVal");
                int ipValCounter = (frequencyCountMap.get(ipVal) == null)?0:frequencyCountMap.get(ipVal);
                frequencyCountMap.put(ipVal, ipValCounter + 1);
            }
            if(tuple.getSourceComponent().equals("timetaskspout")) {
                String signal = tuple.getStringByField("signal");
                String data = tuple.getStringByField("date");
                if(signal.equals("Clear")) {
                    String filename = "frequency_data.txt." + data;
                    DumpMemToFile(filename);
                    frequencyCountMap.clear();
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        return conf;
    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreadId = tuple.getSourceStreamId();
        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreadId.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void DumpMemToFile(String filename) {
        try(BufferedWriter out = new BufferedWriter(new FileWriter(filename))) {
            for(Map.Entry<String, Integer> entry : frequencyCountMap.entrySet()) {
                out.write(String.format("%s\t%d\r\n", entry.getKey(), entry.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
