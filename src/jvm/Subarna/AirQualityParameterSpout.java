package Subarna;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.*;
import java.util.*;

public class AirQualityParameterSpout extends BaseRichSpout implements IRichSpout{

	public static Logger LOG = LoggerFactory.getLogger(AirQualityParameterSpout.class);
	boolean _isDistributed;
	SpoutOutputCollector _collector;
	boolean completed = false;
	Scanner fileInput;
	String filename;

	public AirQualityParameterSpout() {
	}
	
	public void nextTuple() {
		Utils.sleep(100); 
		try{
			if(fileInput.hasNextLine()){
				String[] oneLine = fileInput.nextLine().split(",");


				final ArrayList<String> line = new ArrayList(Arrays.asList(oneLine));
				_collector.emit(new Values(line));

			}
		}catch(Exception e){
			throw new RuntimeException(e.getMessage());
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		_collector = collector;
		try{
			fileInput = new Scanner(new File(conf.get("CO").toString()));
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tuple"));

	}

}
