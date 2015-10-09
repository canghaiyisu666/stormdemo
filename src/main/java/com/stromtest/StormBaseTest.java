package com.stromtest;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StormBaseTest {
	public static class MySpout extends BaseRichSpout {
		private Map conf;// 配置
		private TopologyContext context;// 上下文环境
		private SpoutOutputCollector collector;// 数据发射器

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}

		int i = 0;

		@Override
		public void nextTuple() {
			System.out.println("i====>" + i);
			this.collector.emit(new Values(i++));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("data"));// 设置数据输出标识（列名）
		}

	}

	public static class MyBolt extends BaseRichBolt {
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}

		int sum = 0;

		@Override
		public void execute(Tuple input) {
			Integer data = input.getIntegerByField("data");// 可以根据列名或者索引获取数据。
			sum += data;
			System.out.println("sum====>" + sum);

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// 不再发射数据，所以不用再声明输出类型。
		}

	}

	public static void main(String[] args) {
		TopologyBuilder tb = new TopologyBuilder();
		tb.setSpout("spout_id", new MySpout());
		tb.setBolt("bolt_id", new MyBolt()).shuffleGrouping("spout_id");

		Config conf = new Config();
		LocalCluster lc = new LocalCluster();// 在本地跑Topology
		lc.submitTopology("My_First_Storm", conf, tb.createTopology());

	}

}
