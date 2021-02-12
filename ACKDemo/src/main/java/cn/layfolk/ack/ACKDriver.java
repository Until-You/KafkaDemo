package cn.layfolk.ack;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class ACKDriver {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ack_spout", new ACKSpout());
        builder.setBolt("a_bolt", new ABolt()).shuffleGrouping("ack_spout");
        builder.setBolt("b_bolt", new BBolt()).shuffleGrouping("a_bolt");
        builder.setBolt("c_bolt", new CBolt()).shuffleGrouping("b_bolt");

        StormTopology topology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("acktopology", new Config(), topology);

    }


}
