import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

/**
 * Created by James on 2018/7/19.
 */
public class TimeTaskSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private Calendar startDate;
    private SimpleDateFormat sdf;
    private String signal;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        startDate = Calendar.getInstance();
        sdf = new SimpleDateFormat("yyyy-MM-dd");
        signal = "Clear";
    }

    @Override
    public void nextTuple() {
        Calendar currentDate = Calendar.getInstance();
        String startDateStr = sdf.format(startDate.getTime());
        String currentDateStr = sdf.format(currentDate.getTime());
        int hour = currentDate.get(Calendar.HOUR_OF_DAY);
        if(!startDateStr.equals(currentDateStr)) {
            if(hour == 1) {
                spoutOutputCollector.emit(new Values(signal, startDateStr));
                startDate = currentDate;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("signal", "date"));
    }
}
