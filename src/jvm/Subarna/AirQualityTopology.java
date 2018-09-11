package Subarna;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.spout.RandomSentenceSpout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//G5K
//bin/storm jar /root/storm-starter/target/storm-starter-0.10.2.jar Subarna/AirQualityTopology AirQualityTopology 
// "/root/storm-starter/CO" "/root/storm-starter/NO2" "/root/storm-starter/O3" "/root/storm-starter/PM10"

// Local machine
// bin/storm jar ~/workspace/storm-starter/target/storm-starter-0.10.2.jar Subarna/AirQualityTopology AirQualityTopology 
// "/home/suchatte/workspace/storm-starter/CO" "/home/suchatte/workspace/storm-starter/NO2" "/home/suchatte/workspace/storm-starter/O3" "/home/suchatte/workspace/storm-starter/PM10"


public class AirQualityTopology {

	public static class FilterBolt extends BaseRichBolt {

		private OutputCollector collector;
		private String taskName;
		int max = 5;

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
			taskName = context.getThisComponentId() + "_" + context.getThisTaskId();
		}

		public void execute(Tuple tuple) {

			ArrayList raw =new ArrayList((ArrayList)tuple.getValue(0));
			ArrayList<String> t = new ArrayList<String>(); 
			String type = (raw.get(1)).toString();
			//System.out.println(type + " **************");
			if(type.contains("NO2"))
			{
				type="NO2";
			}
			t.add(type);
			t.add(raw.get(4).toString());
			t.add(raw.get(5).toString());

			collector.emit(tuple, new Values(type, t));
			//collector.emit("stream1", tuple, new Values(t));
			//collector.emit("stream2", tuple, new Values(t));
			collector.ack(tuple);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			//declarer.declareStream("stream1", new Fields("type"));
			//declarer.declareStream("stream2", new Fields("group-type"));
			declarer.declare(new Fields("type", "type1"));
		}	

	}

	public static class AssessQualityBolt extends BaseRichBolt {

		private OutputCollector collector;
		private String taskName;
		HashMap<String, Double> chart = new HashMap<String, Double>();

		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
			chart.put("CO", 0.4);
			chart.put("NO2", 0.4);
			chart.put("O3", 0.4);
			chart.put("PM10", 0.4);
			taskName = context.getThisComponentId() + "_" + context.getThisTaskId();
		}

		public void execute(Tuple tuple) {
			ArrayList raw =new ArrayList((ArrayList)tuple.getValue(1));
			Double threshold = chart.get(raw.get(0));
			//System.out.println(raw.get(0) + " Good ******************************************************************");
			if (Double.parseDouble(raw.get(1).toString()) <= threshold)
			{
				System.out.println(raw.get(0) + " Good");
				//System.out.println("Good");
			}
			else
			{
				System.out.println(raw.get(0) + " Bad");
			}
			collector.ack(tuple);
			Long complete_latency = System.nanoTime() - Long.parseLong(raw.get(2).toString()); // in nano second
			//System.out.println(complete_latency);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("quality"));
		}

	}

	public static class AirQualityAssessBolt extends BaseRichBolt {

		private OutputCollector collector;
		private String taskName;
		HashMap<String, Double> chart = new HashMap<String, Double>();
		HashMap<String, ArrayList<Double>> chartReceived;
		int count =0, max = 10;
		ArrayList<Double> COarr = new ArrayList<Double>();
		ArrayList<Double> NO2arr = new ArrayList<Double>();
		ArrayList<Double> O3arr = new ArrayList<Double>();
		ArrayList<Double> PM10arr = new ArrayList<Double>();
		ArrayList<Double> temp = new ArrayList<Double>();

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
			chart.put("CO", 0.4);
			chart.put("NO2", 0.4);
			chart.put("O3", 0.4);
			chart.put("PM10", 0.4);
			taskName = context.getThisComponentId() + "_" + context.getThisTaskId();
		}

		public void execute(Tuple tuple) {
			ArrayList raw =new ArrayList((ArrayList)tuple.getValue(1));
			Double threshold = chart.get(raw.get(0));
			if(raw.get(0).equals("CO"))
			{
				COarr.add(Double.parseDouble(raw.get(1).toString()));
				temp = COarr;
			}
			else if(raw.get(0).equals("NO2"))
			{
				NO2arr.add(Double.parseDouble(raw.get(1).toString()));
				temp = NO2arr;
			}
			else if(raw.get(0).equals("O3"))
			{
				O3arr.add(Double.parseDouble(raw.get(1).toString()));
				temp = O3arr;
			}
			else if(raw.get(0).equals("PM10"))
			{
				PM10arr.add(Double.parseDouble(raw.get(1).toString()));
				temp = PM10arr;
			}

			if(temp.size() >= max)
			{
				double sum = 0, avg;
				for(Double d: temp)
				{
					sum = sum + d;
				}
				avg = sum/temp.size();
				if(avg >= threshold)
					System.out.println("Bad air quality!");
				else
					System.out.println("Good air quality!");
			}

			collector.ack(tuple);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("air-quality"));
		}
	}

	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("CO", new COSpout(), 15);
		builder.setSpout("NO2", new NO2Spout(), 15);
		builder.setSpout("O3", new O3Spout(), 15);
		builder.setSpout("PM10", new PM10Spout(), 15);
		builder.setBolt("filter", new FilterBolt(), 100).shuffleGrouping("CO").shuffleGrouping("NO2").shuffleGrouping("O3").shuffleGrouping("PM10");;
		builder.setBolt("assess", new AssessQualityBolt(), 5).fieldsGrouping("filter", new Fields("type"));
		builder.setBolt("air-quality-assess", new AirQualityAssessBolt(), 5).fieldsGrouping("filter", new Fields("type"));

		//builder.setBolt("assess", new AssessQualityBolt(), 5).fieldsGrouping("filter", "stream1", new Fields("type"));
		//builder.setBolt("air-quality-assess", new AirQualityAssessBolt(), 5).fieldsGrouping("filter", "stream2", new Fields("group-type"));

		Config conf = new Config();
		conf.put("CO",args[1]);
		conf.put("NO2",args[2]);
		conf.put("O3",args[3]);
		conf.put("PM10",args[4]);
		//conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(7);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

			/*conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("air-quality-monitor", conf, builder.createTopology());
			System.out.println("Submitted + " + conf);
			Thread.sleep(10000);*/

			//cluster.shutdown();
		}
		else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("air-quality-monitor", conf, builder.createTopology());
			System.out.println("Submitted + " + conf);
			Thread.sleep(10000);

			cluster.shutdown();
		}
	}

}
