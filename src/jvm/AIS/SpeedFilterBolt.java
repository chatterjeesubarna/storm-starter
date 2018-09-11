/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AIS;

import static java.lang.Math.abs;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 *
 * @author Belkhiria
 */
public class SpeedFilterBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("mmsi", "speed", "destination"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        Float speed = tuple.getFloatByField("speed");
        Integer mmsi = tuple.getIntegerByField("mmsi");
        String destination = tuple.getStringByField("destination");
        if (speed != null && abs(speed) <= 0.1F) {
            //System.out.println(new Values(mmsi, speed, destination));
            boc.emit(new Values(mmsi, speed, destination));
        }
    }
}
