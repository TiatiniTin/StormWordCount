package com.company;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LineSpout extends BaseRichSpout {

    private List<String> lines;
    private SpoutOutputCollector collector;
    private int index = 0;

    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            BufferedReader br = new BufferedReader(new FileReader("test.txt"));
            lines = new ArrayList<>();
            String nextLine = "";
            while ((nextLine = br.readLine()) != null) {
                lines.add(nextLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        if (index < lines.size()) {
            collector.emit(new Values(lines.get(index)), index);
            index++;
        } else {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
        System.out.println ("Ошибка передачи данных! Эти данные следует отправить повторно!" + msgId + ":" + lines.get ((int) msgId));
        collector.emit(new Values(lines.get((int) msgId)), (int) msgId);
    }
}
