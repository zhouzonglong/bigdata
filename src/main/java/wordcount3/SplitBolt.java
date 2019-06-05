package wordcount3;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitBolt extends BaseRichBolt {

	private Map conf; // 当前组件配置信息
	private TopologyContext context; // 当前组件上下文对象
	private OutputCollector collector; // 发送tuple的组件

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.conf = conf;
		this.context = context;
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String line = input.getStringByField("line");
		String[] words = line.split(" ");
		for (String word : words) {
			this.collector.emit(new Values(word, 1));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}