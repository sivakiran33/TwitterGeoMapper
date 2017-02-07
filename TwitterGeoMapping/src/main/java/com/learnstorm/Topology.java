package com.learnstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.base.Preconditions;

/**
 * Topology class that sets up the Storm topology for this sample. Please note
 * that Twitter credentials have to be provided as VM args, otherwise you'll get
 * an Unauthorized error.
 * @link http://twitter4j.org/en/configuration.html#systempropertyconfiguration
 */
public class Topology {

    static final String TOPOLOGY_NAME = "storm-twitter-word-count";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
        
        TopologyBuilder b = new TopologyBuilder();
        
        
        b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        
        
        b.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("TwitterSampleSpout");//shuffleGrouping("TwitterSampleSpout");
        
        b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).shuffleGrouping("WordSplitterBolt");
        
        b.setBolt("WordCounterBolt", new GeoWordCounterBolt(10, 5 * 60, 50)).fieldsGrouping("IgnoreWordsBolt",new Fields("x","y"));

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });

    }

}
