package com.company;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitLineBolt extends BaseRichBolt {
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        try {
            // 1. Получить упакованную информацию из кортежа
            String sentence = tuple.getStringByField("line");
            // 2, разбить на слова
            String[] words = sentence.split(" ");
            // Преобразуем в кортеж для продолжения отправки
            for (int i = 0; i < words.length; i++) {
                collector.emit(tuple, new Values(words[i]));
            }
            // сообщаем об успехе штурму
            collector.ack(tuple);
        } catch (Exception e) {
            // сообщить о неудаче штурма
            collector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}