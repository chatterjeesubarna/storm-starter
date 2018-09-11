/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AIS;

import java.util.HashMap;
import java.util.Map;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 *
 * @author Belkhiria
 */
public class CountShipsBolt extends BaseBasicBolt {

    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        String port = tuple.getStringByField("destination");
        if (port == null) {
            port = "unknown";
        }
        Integer count = counts.get(port);
        if (count == null) {
            count = 0;
        }
        count++;
        counts.put(port, count);
        System.err.println(new Values(port, count));
        //boc.emit(new Values(port, count));

    }

}
