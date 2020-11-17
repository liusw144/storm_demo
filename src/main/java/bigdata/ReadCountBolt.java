package bigdata;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName：ReadCountBolt
 * @Description:
 * @Author: liusw
 * @Date: 2020/11/17 4:04
 */
public class ReadCountBolt extends BaseRichBolt {
    private Map<String, Integer> map =new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {

        String word = tuple.getStringByField("word");
        String num = tuple.getStringByField("num");

        if (map.containsKey(word)) {
            map.put(word, Integer.parseInt(num) + 1);
        } else {
            map.put(word, Integer.parseInt(num));
        }
        System.out.println(map);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }


}
