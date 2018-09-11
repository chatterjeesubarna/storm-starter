package AIS;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class AISTopology {

	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("AISDataSpout", new AISDataSpout(), 5);
		builder.setBolt("DecodeBolt", new DecodeBolt(), 2).globalGrouping("AISDataSpout");
		builder.setBolt("AverageBolt", new AverageSpeedBolt(), 2).fieldsGrouping("DecodeBolt", new Fields("mmsi"));
		builder.setBolt("FilterBolt", new SpeedFilterBolt(), 2).shuffleGrouping("DecodeBolt");
        builder.setBolt("CountPort", new CountShipsBolt(), 2).fieldsGrouping("FilterBolt", new Fields("destination"));
		
		Config conf = new Config();
		conf.put("ais_orbcomm_20170301_0000.nm4",args[1]);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			//StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("AIS-data-analysis", conf, builder.createTopology());
			System.out.println("Submitted + " + conf);
			Thread.sleep(10000);

			//cluster.shutdown();
		}
		else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("AIS-data-analysis", conf, builder.createTopology());
			System.out.println("Submitted + " + conf);
			Thread.sleep(10000);

			cluster.shutdown();
		}
	}

}
