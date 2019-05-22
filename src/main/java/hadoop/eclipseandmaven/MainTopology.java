package hadoop.eclipseandmaven;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		KafkaSpoutConfig.Builder<String, String> kafkaBuilder = KafkaSpoutConfig.builder("127.0.0.1:9092", "test0811");
//				.builder("127.0.0.1:9092,node-2:9092,node-3:9092", "test0811");
		// 设置kafka属于哪个组
		kafkaBuilder.setGroupId("testgroup");
		// 创建kafkaspoutConfig
		KafkaSpoutConfig<String, String> build = kafkaBuilder.build();
		// 通过kafkaspoutConfig获得kafkaspout
		KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(build);
		// 设置5个线程接收数据
		builder.setSpout("kafkaSpout", kafkaSpout, 5);
		// 设置2个线程处理数据
		builder.setBolt("printBolt", new PrintBolt(), 2).localOrShuffleGrouping("kafkaSpout");
		Config config = new Config();
		if (args.length > 0) {
			// 集群提交模式
			config.setDebug(false);
			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		} else {
			// 本地测试模式
			config.setDebug(true);
			// 设置2个进程
			config.setNumWorkers(2);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafkaSpout", config, builder.createTopology());
		}
	}
}