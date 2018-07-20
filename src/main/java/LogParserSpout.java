import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Created by James on 2018/7/17.
 */
public class LogParserSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private List<String> logs;
    private int nextEmitIndex;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.nextEmitIndex = 0;
        try {
            logs = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("track.test.log"),
                    Charset.defaultCharset().name());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        String log = logs.get(nextEmitIndex);
        spoutOutputCollector.emit(new Values(log));
        nextEmitIndex = (nextEmitIndex + 1) % logs.size();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));
    }
}
