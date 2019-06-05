package datadeail;

import java.io.IOException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FirstBult extends BaseBasicBolt {

	public void execute(Tuple arg0, BasicOutputCollector output) {
		String str = String.valueOf(arg0.getValueByField("device"));
//		 id,addr,type,date
		String[] arrys = str.split("\t");
		String id = arrys[0];
		String addr = arrys[1];
		String type = arrys[2];
		String date = arrys[3];

		Whether w = new Whether();
		w.setAddr(addr);
		w.setDate(date);
		w.setId(id);
		w.setType(type);

//		保存数据到hbase中
		InsertHbase iHbase = new InsertHbase();
		try {
			iHbase.insertData("stormHbaseTem", w);
			System.out.print("insert one 。。。。。。");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		output.emit(new Values(w));
	}

	public void declareOutputFields(OutputFieldsDeclarer output) {
		output.declare(new Fields("device"));
	}

}