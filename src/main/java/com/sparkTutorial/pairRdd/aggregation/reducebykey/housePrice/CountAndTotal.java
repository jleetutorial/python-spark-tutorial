package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;

import java.io.Serializable;

class CountAndTotal implements Serializable {
    private int count;
    private double total;

    CountAndTotal(int count, double total) {
        this.count = count;
        this.total = total;
    }

    public int getCount() {
        return count;
    }

    public double getTotal() {
        return total;
    }
}