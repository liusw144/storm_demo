package bigdata;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @ClassName：WordCountTopology
 * @Description:
 * @Author: liusw
 * @Date: 2020/11/17 5:33
 */
public class ReadCountTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("readfilespout",new ReadFileSpout());
        topologyBuilder.setBolt("spiltbolt",new SpiltBolt()).shuffleGrouping("readfilespout");
        topologyBuilder.setBolt("readcountBolt",new ReadCountBolt()).shuffleGrouping("spiltbolt");
        LocalCluster localCluster = new LocalCluster();
        Config config = new Config();
        localCluster.submitTopology("readCount",config,topologyBuilder.createTopology());
       /* BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("d://a.txt")));

        String s = bufferedReader.readLine();*/
    }
}


