package bigdata;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.io.*;
import java.util.Arrays;
import java.util.Map;

/**
 * @ClassName：ReadFileSpout
 * @Description:
 * @Author: liusw
 * @Date: 2020/11/17 4:04
 */
public class ReadFileSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader bufferedReader;

    @Override
    /**
     * @methodName:open
     * @Description:收集器
     * @Author: liusw
     * @Date: 2020/11/17 4:31
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        try {
            bufferedReader = new BufferedReader(new FileReader(new File("d://readcount.txt")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        this.spoutOutputCollector = spoutOutputCollector;
    }


    /**
     * @methodName:nextTuple
     * @Description:下一步 没读取一行数据就发送给下一步
     * @Author: liusw
     * @Date: 2020/11/17 4:32
     */
    @Override
    public void nextTuple() {
        String line = null;
        try {
            line = bufferedReader.readLine();
            if (line != null) {
                spoutOutputCollector.emit(Arrays.asList(line));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("juzi"));
    }
}
