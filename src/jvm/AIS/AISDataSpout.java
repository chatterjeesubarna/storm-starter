package AIS;


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

public class AISDataSpout extends BaseRichSpout implements IRichSpout{

	public static Logger LOG = LoggerFactory.getLogger(AISDataSpout.class);
	SpoutOutputCollector scollector;
	Scanner fileInput;
	int count =-1, total =0;
	ArrayList<String> lines = new ArrayList<String>();
	public AISDataSpout() {
	}

	public void nextTuple() {
		Utils.sleep(10); 
		//if(fileInput.hasNextLine()){
		if(total < 4000){
			String obtained = fileInput.nextLine();
			String finalString = obtained.substring(obtained.indexOf("!"));
			//System.out.println(finalString);
			lines.add(finalString);
			total++;
			scollector.emit(new Values(finalString));
			
		}
		else
		{
			count = (count + 1)%lines.size();
			scollector.emit(new Values(lines.get(count)));
		}
		
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		scollector = collector;
		try{
			fileInput = new Scanner(new File(conf.get("ais_orbcomm_20170301_0000.nm4").toString()));
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tuple"));

	}


}
