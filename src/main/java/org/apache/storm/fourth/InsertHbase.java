package org.apache.storm.fourth;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class InsertHbase {
	public static Connection connection;
	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("zookeeper.znode.parent", "/hbase-unsecure"); // 与 hbase-site-xml里面的配置信息
																		// zookeeper.znode.parent 一致
		configuration.set("hbase.zookeeper.quorum", "10.10.10.241"); // hbase 服务地址
		configuration.set("hbase.zookeeper.property.clientPort", "2181"); // 端口号
		// 这里使用的是接口Admin 该接口有一个实现类HBaseAdmin 也可以直接使用这个实现类
	}

	public void insertData(String tableName, Whether w) throws IOException {
		System.out.println("start insert data ......");
		Connection connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf(tableName));

		Put put = new Put(w.getId().toString().getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
		put.add("addr".getBytes(), null, w.getAddr().getBytes());// 本行数据的第一列
		put.add("date".getBytes(), null, w.getType().getBytes());// 本行数据的第二列
		put.add("type".getBytes(), null, w.getDate().getBytes());// 本行数据的第三列

		try {
			table.put(put);
//			table.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end insert data ......");
	}
//	public static whether getWhether( ) {
//		whether w = new whether();
////	 b = Integer.parseInt(new java.text.DecimalFormat("0").format(Math.random() * 1000));
//		w.setAddr("wew");
////		w.setId(b);
//		w.setType("rain");
//		w.setDate("2019-08-08");
//		return w;
//	}

//	public static void main(String[] args) throws IOException {
//		insertData("stormHbaseTem",new whether());
//	}

}
