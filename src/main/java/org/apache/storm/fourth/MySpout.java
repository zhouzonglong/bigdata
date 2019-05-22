package org.apache.storm.fourth;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MySpout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
//	String url = "jdbc:mysql://localhost:3309/zzldb";
//	String username = "root";
//	String password = "123123";
	String username;
	String password;
	String driver;
	String url;
	private ResultSet res;
	private Statement sta;
	private SpoutOutputCollector collector;
	int id = 0;

	public void nextTuple() {
		String str = "";
		try {
			if (res.next()) {
				String id = res.getString(1);
				String addr = res.getString(2);
				String type = res.getString(3);
				String date = res.getString(4);
				str += id + "\t" + addr + "\t" + type + "\t" + date;
				collector.emit(new Values(str));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void open(Map arg0, TopologyContext topology, SpoutOutputCollector collector) {
		Properties p = new Properties();
		try {
			p.load(new FileInputStream("target/jdbc.properties"));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {

			driver = p.getProperty("driver");
			url = p.getProperty("url");
			username = p.getProperty("username");
			password = p.getProperty("password");
			System.out.println(url + "," + driver);
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, username, password);
//			String driver = "com.mysql.cj.jdbc.Driver";
//			Class.forName(driver);
//			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/zzldb?useSSL=false&user=root&password=root123");
			sta = conn.createStatement();
			res = sta.executeQuery("select id,addr,type,date  from Whether");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		this.collector = collector;

	}

	public void declareOutputFields(OutputFieldsDeclarer output) {
		output.declare(new Fields("device"));
	}

}