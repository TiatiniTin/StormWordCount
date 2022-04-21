package com.company;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class ReportBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, Long> map;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.map = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        try {
            String word = tuple.getStringByField("word");
            Long count = tuple.getLongByField("count");
            map.put(word, count);
            collector.ack(tuple);
        } catch (Exception e) {
            collector.fail(tuple);
        }
    }

    @Override
    public void cleanup() {
        System.out.println ("------ результат статистики ------");
        List<String> keys = new ArrayList<String>();
        keys.addAll(map.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + map.get(key));
        }
        System.out.println("------------------");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}