package wordcount3;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class MainToplogy {

	/**
	 * 构建拓扑，组装Spout和Bolt节点，相当于在MapReduce中构建Job
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		// 两份数据源
		builder.setSpout("id_file_spout", new FileSpout(), 2);
//		一个数据源对应两个task   4/2
		builder.setBolt("id_split_bolt", new SplitBolt(), 2).setNumTasks(4).shuffleGrouping("id_file_spout");
		builder.setBolt("id_wc_bolt", new WCBolt()).allGrouping("id_split_bolt");

//		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).setNumTasks(4) .shuffleGrouping(SENTENCE_SPOUT_ID);
//	builder.setBolt(COUNT_BOLT_ID, countBolt, 4).fieldsGrouping(SPLIT_BOLT_ID, newFields("word"));

		LocalCluster cluster = new LocalCluster();
		Config config = new Config();
//		设置两个工作节点
		config.setNumWorkers(2);
		cluster.submitTopology("StormLocalWordCountTopology", config, builder.createTopology());

//
//	        builder.setSpout(SENTENCE_SPOUT_ID, spout);
//
//	        builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
//	  
//	        builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
//
//	        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

	}

}