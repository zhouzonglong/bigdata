package org.apache.storm.fourth;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * 
 * 
 * builder.setBolt(“2”, new Trans()).shuffleGrouping(“1”);
 * shuffleGrouping(“1”)在这里1记录bolt数据的来源是哪个Spout或者Bolt builder.setBolt(“3”, new
 * wordcount(), 5).fieldsGrouping(“2”, new Fields(“word”)); 第二个参数new
 * Fields(“word”) 表示按照名为word的分组来分发数据
 * 
 * @author ZZL
 *
 */
public class StartStorm {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("1", new MySpout());
		builder.setBolt("2", new FirstBult()).shuffleGrouping("1");
		builder.setBolt("3", new SecondBult()).shuffleGrouping("2");

		Config config = new Config();
		config.setDebug(false);
		if (args != null && args.length > 0) {
			config.setNumWorkers(2);
			StormSubmitter.submitTopology("mysql", config, builder.createTopology());
		} else {
			LocalCluster local = new LocalCluster();
			local.submitTopology("topo", config, builder.createTopology());

		}

	}
}
