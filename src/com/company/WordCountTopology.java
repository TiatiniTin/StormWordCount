package com.company;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {
    public static void main(String[] args) throws Exception {
        // 1. Создаем компонент
        LineSpout spout = new LineSpout();
        SplitLineBolt splitLineBolt = new SplitLineBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        // 2. Создаем построитель топологии
        TopologyBuilder builder = new TopologyBuilder();

        // 3. Топология организации
        builder.setSpout("LineSpout", spout);
        builder.setBolt("SplitLineBolt", splitLineBolt)
                .shuffleGrouping("LineSpout");
        builder.setBolt("WordCountBolt", wordCountBolt)
                .fieldsGrouping("SplitLineBolt", new Fields("word"));
        builder.setBolt("ReportBolt", reportBolt)
                .globalGrouping("WordCountBolt");

        // 4. Создаем топологию
        StormTopology topology = builder.createTopology();

        // 5. Создаем объект конфигурации
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        // 6. Отправляем топологию для работы кластера
        cluster.submitTopology("WordCountTopology", conf, topology);

        // 7. Убить топологию и закрыть кластер после работы в течение 10 секунд
        try {
            Thread.sleep(1000 * 10);
        } catch (InterruptedException e) {
        }
        cluster.killTopology("WordCountTopology");
        cluster.shutdown();
    }
}