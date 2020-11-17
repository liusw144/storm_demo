package bigdata;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Map;

/**
 * @ClassName：SpiltBolt
 * @Description:ff
 * @Author: liusw
 * @Date: 2020/11/17 4:04
 */
public class SpiltBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }


    /**
     * @methodName:
     * @Description:取上游数据
     * @Author: liusw
     * @Date: 2020/11/17 4:46
     */
    public void execute(Tuple tuple) {
        String juzi = tuple.getStringByField("juzi");
        String[] s = juzi.split(" ");
        for (String s1 : s) {
            outputCollector.emit(Arrays.asList(s1, "1"));

        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "num"));
    }
}
