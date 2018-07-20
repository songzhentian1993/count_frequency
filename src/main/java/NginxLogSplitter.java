import org.apache.storm.shade.org.apache.commons.codec.binary.Base64;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by James on 2018/7/17.
 */
public class NginxLogSplitter extends BaseBasicBolt {
    private Set<String> filterKeySet;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        filterKeySet = new HashSet<>();//初始化容量应设置为多少
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(tuple.getSourceComponent().equals("spout")) {
            String log = tuple.getStringByField("log");
            Map<String, String> filterMap = logSplitter(log);
            String adx = filterMap.get("adx");
            String ad = filterMap.get("ad");
            String bid = filterMap.get("bid");
            String imp = filterMap.get("imp");
            String campId = filterMap.get("campaign_id");
            String udid = new String(Base64.decodeBase64(filterMap.get("udid_encode")));
            String ip = new String(Base64.decodeBase64(filterMap.get("ip_encode")));
            String filterKey = String.format("%s\\x01%s\\x01%s\\x01%s", adx, ad, bid, imp);
            String udidVal = String.format("%d\t%s\t%s", 1, udid, campId);
            String ipVal = String.format("%d\t%s\t%s", 2, ip, campId);
            if(!filterKeySet.contains(filterKey)) {
                basicOutputCollector.emit("udidValStream", new Values(udidVal));
                basicOutputCollector.emit("ipValStream", new Values(ipVal));
                filterKeySet.add(filterKey);
            }
        }
        if(tuple.getSourceComponent().equals("timetaskspout")) {
            String signal = tuple.getStringByField("signal");
            if(signal.equals("Clear")) {
                filterKeySet.clear();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("udidValStream", new Fields("udidVal"));
        outputFieldsDeclarer.declareStream("ipValStream", new Fields("ipVal"));
    }

    public Map<String, String> logSplitter(String log) {
        String regex = "\\[(\\d{2}/\\w{3}/\\d{4}).*(adx=\\S*)";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(log);
        Map<String, String> filterMap = new HashMap<String, String>();
        if(m.find() && m.groupCount() == 2) {
            String logTime = m.group(1);
            String fieldInfo = m.group(2);
            for(String elem : fieldInfo.split("&")) {
                if(elem.indexOf("=") == -1) {
                    continue;
                }
                String key = elem.split("=", 2)[0];
                String value = elem.split("=", 2)[1];
                filterMap.put(key, value);
            }
        }
        return filterMap;
    }
}
