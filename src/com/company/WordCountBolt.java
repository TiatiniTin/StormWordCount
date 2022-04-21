package com.company;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, Long> counts;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        try {
            String word = tuple.getStringByField("word");
            counts.put(word, counts.containsKey(word) ? counts.get(word) + 1 : 1);
            collector.emit(tuple, new Values(word, counts.get(word)));
            collector.ack(tuple);
        } catch (Exception e) {
            collector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}