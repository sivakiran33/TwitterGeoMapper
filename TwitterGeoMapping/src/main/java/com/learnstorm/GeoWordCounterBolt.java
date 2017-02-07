package com.learnstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Keeps stats on word count, calculates and logs top words every X second to
 * stdout and top list every Y seconds,
 *
 * @author raghu
 */
public class GeoWordCounterBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(GeoWordCounterBolt.class);
    /**
     * Number of seconds before the top list will be logged to stdout.
     */
    private final long logIntervalSec;
    /**
     * Number of seconds before the top list will be cleared.
     */
    private final long clearIntervalSec;
    /**
     * Number of top words to store in stats.
     */
    private final int topListSize;

    private HashMap<String, Long> counter;
    private long lastLogTime;
    private long lastClearTime;

    public GeoWordCounterBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
        this.logIntervalSec = logIntervalSec;
        this.clearIntervalSec = clearIntervalSec;
        this.topListSize = topListSize;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

//        HashMap<String,Long>[][] hashMap = new HashMap[20][20];
        counter = new HashMap<String, Long>();

        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        /**
         * Co-ordinate matrix
         */
        Integer x = (Integer) input.getValueByField("x") - 1;
        Integer y = (Integer) input.getValueByField("y") - 1;
//        input.
        Long count = -1l;
        try {
            count = counter.get(word);
        } catch (NullPointerException ex) {
            throw ex;
        }
        count = count == null ? 1L : count + 1;
        counter.put(word, count);

//        logger.info(new StringBuilder(word).append('>').append(count).toString());
        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {
            logger.info("Word count(): " + counter.size() + "-:" + x + "x" + y);

            publishTopList();
            lastLogTime = now;
        }
    }

    private void publishTopList() {
        // calculate top list:
        SortedMap<Long, String> top = new TreeMap<Long, String>();

        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            long count = entry.getValue();
            String word = entry.getKey();

            top.put(count, word);
            if (top.size() > topListSize) {
                top.remove(top.firstKey());
            }
        }

        // Output top list:
        for (Map.Entry<Long, String> entry : top.entrySet()) {
            logger.info(new StringBuilder("top - ").append(entry.getValue()).append('>').append(entry.getKey()).toString());
        }
        
        // Clear top list
        long now = System.currentTimeMillis();
        if (now - lastClearTime > clearIntervalSec * 1000) {
            counter.clear();
            lastClearTime = now;
        }
    }
}
