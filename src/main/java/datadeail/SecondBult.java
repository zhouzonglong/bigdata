package datadeail;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SecondBult extends BaseBasicBolt {
	OutputCollector collector;

	public void execute(Tuple arg0, BasicOutputCollector outputCollector) {

		Whether w = (Whether) arg0.getValueByField("device");
		System.out.println("名称" + "\t" + "学校" + "\t" + "日期" + "\t" + "PC" + "\t" + "奥" + "\t" + "名称" + "\t" + "名称");
		System.out.println(w.getAddr() + "\t" + w.getDate() + "\t" + w.getType() + "\t" + w.getId());
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}
}
