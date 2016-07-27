package com.epam.training.hadoop.hdfs;


import java.util.ArrayList;
import java.util.List;

/*List with Integer elements
  list.get(0) == 0
  list.get(1) == 1
  list.get(2) == 2
  etc
   */
public class IntList {
    private List<Integer> list;
    private int increaseStep;

    public IntList(int initSize, int increaseStep) {

        this.increaseStep = increaseStep;

        list = new ArrayList<Integer>(initSize);
        for (int i = 0; i <= initSize; i++) {
            list.add(Integer.valueOf(i));
        }

    }

    public Integer getNextValue(Integer valueToIncrease) {

        //list size = last/max list element + 1
        if (valueToIncrease + 1 == list.size())
            performListIncrease();

        return list.get(valueToIncrease + 1);

    }

    private void performListIncrease() {
        int currentSize = list.size();
        for (int i = currentSize + 1; i <= currentSize + increaseStep; i++) {
            list.add(Integer.valueOf(i));
        }
    }

}
