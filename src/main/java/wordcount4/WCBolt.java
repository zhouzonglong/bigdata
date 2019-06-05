package wordcount4;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WCBolt extends BaseRichBolt {

	private Map conf; // 当前组件配置信息
	private TopologyContext context; // 当前组件上下文对象
	private OutputCollector collector; // 发送tuple的组件

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.conf = conf;
		this.context = context;
		this.collector = collector;
	}

	private Map<String, Integer> map = new HashMap<>();

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Integer count = input.getIntegerByField("count");
		/*
		 * if (map.containsKey(word)) { map.put(word, map.get(word) + 1); } else {
		 * map.put(word, 1); }
		 */
		map.put(word, map.getOrDefault(word, 0) + 1);

		System.out.println("====================================");
		map.forEach((k, v) -> {
			System.out.println(k + ":::" + v);
		});
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
