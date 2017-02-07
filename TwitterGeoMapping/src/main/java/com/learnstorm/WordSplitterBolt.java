package com.learnstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;

/**
 * Receives tweets and emits its words over a certain length.
 */
public class WordSplitterBolt extends BaseRichBolt {
    private final int minWordLength;

    private OutputCollector collector;

    public WordSplitterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
//        String lang = tweet.getUser().getLang();
        String text = tweet.getText().replaceAll("\\p{Punct}", " ").toLowerCase();
        
        int x= (int)Math.floor((tweet.getGeoLocation().getLongitude()+180)/10);
        int y= (int)Math.floor((tweet.getGeoLocation().getLatitude()+90)/10);
        String[] words = text.split(" ");
        for (String word : words) {
            if (word.length() >= minWordLength) {
                collector.emit(new Values ( x,y, word));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("x","y", "word"));
    }
}
