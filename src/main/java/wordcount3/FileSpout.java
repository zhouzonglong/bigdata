package wordcount3;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileSpout extends BaseRichSpout {
	private Map conf; // 当前组件配置信息
	private TopologyContext context; // 当前组件上下文对象
	private SpoutOutputCollector collector; // 发送tuple的组件

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.conf = conf;
		this.context = context;
		this.collector = collector;
	}

	public void nextTuple() {
		File directory = new File("D:\\360downloads\\data");
		// 第二个参数extensions的意思就是，只采集某些后缀名的文件
		Collection<File> files = FileUtils.listFiles(directory, new String[] { "txt" }, true);
		for (File file : files) {
			try {
				List<String> lines = FileUtils.readLines(file, "utf-8");
				for (String line : lines) {
					this.collector.emit(new Values(line));
				}
				// 当前文件被消费之后，需要重命名，同时为了防止相同文件的加入，重命名后的文件加了一个随机的UUID，或者加入时间戳也可以的
				File destFile = new File(file.getAbsolutePath() + "_" + UUID.randomUUID().toString() + ".completed");
				FileUtils.moveFile(file, destFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
