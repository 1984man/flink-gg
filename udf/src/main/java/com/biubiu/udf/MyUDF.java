package com.biubiu.udf;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;
import java.util.Random;

public class MyUDF {

    /**
     * Accumulator for test requiresOver.
     */
    public static class AccumulatorOver extends Tuple2<Long, Integer> {
    }

    /**
     * Test for requiresOver.
     */
    public static class OverAgg0 extends AggregateFunction<Long, AccumulatorOver> {
        @Override
        public AccumulatorOver createAccumulator() {
            return new AccumulatorOver();
        }

        @Override
        public Long getValue(AccumulatorOver accumulator) {
            return 1L;
        }

        @Override
        public boolean requiresOver() {
            return true;
        }

        //Overloaded accumulate method
        public void accumulate(AccumulatorOver accumulator, long iValue, int iWeight) {
        }
    }

    /**
     * Increment input.
     */
    public static class JavaFunc0 extends ScalarFunction {
        public long eval(Long l) {
            return l + 1;
        }
    }

    /**
     * Concatenate inputs as strings.
     */
    public static class JavaFunc1 extends ScalarFunction {
        public String eval(Integer a, int b, Long c) {
            return a + " and " + b + " and " + c;
        }
    }

    /**
     * Append product to string.
     */
    public static class JavaFunc2 extends ScalarFunction {
        public String eval(String s, Integer... a) {
            int m = 1;
            for (int n : a) {
                m *= n;
            }
            return s + m;
        }
    }

    /**
     * Test overloading.
     */
    public static class JavaFunc3 extends ScalarFunction {
        public int eval(String a, int... b) {
            return b.length;
        }

        public String eval(String c) {
            return c;
        }
    }

    /**
     * Concatenate arrays as strings.
     */
    public static class JavaFunc4 extends ScalarFunction {
        public String eval(Integer[] a, String[] b) {
            return Arrays.toString(a) + " and " + Arrays.toString(b);
        }
    }

    /**
     * Testing open method is called.
     */
    public static class UdfWithOpen extends ScalarFunction {

        private boolean isOpened = false;

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            this.isOpened = true;
        }

        public String eval(String c) {
            if (!isOpened) {
                throw new IllegalStateException("Open method is not called!");
            }
            return "$" + c;
        }
    }



}
