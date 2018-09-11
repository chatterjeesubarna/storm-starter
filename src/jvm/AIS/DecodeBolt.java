package AIS;

import java.util.ArrayList;
import java.util.Map;
import java.io.*;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import dk.tbsalling.aismessages.AISInputStreamReader;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DecodeBolt extends BaseBasicBolt {

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// here we declare we will be emitting tuples with
		// a single field called "sentence"
		declarer.declare(new Fields("mmsi", "speed", "destination"));
	}

	  public void execute(Tuple input, BasicOutputCollector boc) {
          try {
              String word = input.getString(0);
              if (StringUtils.isBlank(word)) {
                  // ignore blank lines
                  return;
              }
              //System.out.println(word);
              InputStream inputStream = new ByteArrayInputStream(word.getBytes());
              AISInputStreamReader streamReader = new AISInputStreamReader(inputStream, aisMessage
                      -> {
                  //System.out.println(new Values(aisMessage));
                  boc.emit(new Values(aisMessage.getSourceMmsi().getMMSI(), aisMessage.dataFields().get("speedOverGround"), aisMessage.dataFields().get("destination")));
              }
              );
              streamReader.run();
          } catch (Exception ex) {
              //Logger.getLogger(DecodeBolt.class.getName()).log(Level.SEVERE, null, ex);
          }
}
}
