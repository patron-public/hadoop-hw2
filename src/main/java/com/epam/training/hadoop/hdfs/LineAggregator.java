package com.epam.training.hadoop.hdfs;


import java.util.*;

public class LineAggregator {


    private Map<String, Integer> parseResults = new HashMap<String, Integer>(20000000);

    private IntList intList = new IntList(1000, 1000);

    public void addLine(String line) {

        String id = LineParser.parse(line);

        Integer count = parseResults.get(id);

        if (count == null) {
            count = 0;
        }
        parseResults.put(id, intList.getNextValue(count));

    }

    public List<Map.Entry<Integer, String>> getTop(int topCount) {
        SortedAggregator sortedAggregator = new SortedAggregator(topCount);
        SortedMap<Integer, Set<String>> orderedMap = new TreeMap<Integer, Set<String>>();

        for (Map.Entry<String, Integer> idCountPair : parseResults.entrySet()) {
            sortedAggregator.offerIdCountPair(idCountPair);

        }

        return sortedAggregator.getTopList();


    }

    private class SortedAggregator {
        private TreeMap<Integer, Set<String>> orderedMap = new TreeMap<Integer, Set<String>>();
        private int topCount;
        //set to true when orderedMap.size() reaches topCount
        private boolean isFull = false;

        private SortedAggregator(int topCount) {
            this.topCount = topCount;
        }

        private void offerIdCountPair(Map.Entry<String, Integer> idCountPair) {
            //map is not full -OR- map is full and entry.key >= map.lessKey
            if (!isFull || orderedMap.firstKey() <= idCountPair.getValue())
                addIdCountPair(idCountPair);

        }

        private void addIdCountPair(Map.Entry<String, Integer> idCountPair) {

            Set<String> ids = orderedMap.get(idCountPair.getValue());

            if (ids == null) {
                ids = new HashSet<String>();
                orderedMap.put(idCountPair.getValue(), ids);

                if (isFull)
                    orderedMap.remove(orderedMap.firstKey());
                else
                    isFull = orderedMap.size() == topCount;

            }

            ids.add(idCountPair.getKey());

        }

        public List<Map.Entry<Integer, String>> getTopList() {
            List<Map.Entry<Integer, String>> results = new ArrayList<Map.Entry<Integer, String>>();
            for (Integer count : orderedMap.descendingKeySet()) {
                Set<String> ids = orderedMap.get(count);
                for (String id : ids) {
                    results.add(new AbstractMap.SimpleEntry<Integer, String>(count, id));
                }
            }

            return results;
        }
    }


}
