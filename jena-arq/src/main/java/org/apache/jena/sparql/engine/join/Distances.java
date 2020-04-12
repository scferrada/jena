package org.apache.jena.sparql.engine.join;

import flann.metric.Metric;
import flann.metric.MetricEuclideanSquared;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.query.DistanceFunction;
import org.apache.jena.query.QueryExecException;
import org.apache.jena.sparql.core.Var;

import java.util.List;
import java.util.Map;

public class Distances {
    public static double compute(List<Node> lvals, List<Node> rvals, DistanceFunction distFunc) {
        switch (distFunc){
            case EUCLIDEAN:
                return euclidean(lvals, rvals, null, null);
            case MANHATTAN:
                return manhattan(lvals, rvals, null, null);
            default:
                throw new IllegalStateException("Unsupported Distance Function: "+distFunc.toString());
        }
    }

    private static double manhattan(List<Node> lvals, List<Node> rvals, List<Var> attrs, Map<String, Pair<Double, Double>> norm) {
        double d = 0;
        for (int i = 0; i < lvals.size(); i++) {
            double factor, min;
            if (norm == null) {
                factor = 1;
                min = 0;
            } else {
                Pair<Double, Double> v = norm.get(attrs.get(i).getVarName());
                factor = v.getRight() - v.getLeft();
                min = v.getLeft();
            }
            double x = ((((Number)lvals.get(i).getLiteralValue()).doubleValue()-min)/factor);
            double y = ((((Number)rvals.get(i).getLiteralValue()).doubleValue()-min)/factor);
            d += Math.abs(x - y);
        }
        return d;
    }

    private static double euclidean(List<Node> lvals, List<Node> rvals, List<Var> attrs, Map<String, Pair<Double, Double>> norm) {
        double d = 0;
        for (int i = 0; i < lvals.size(); i++) {
            double sq = (double)lvals.get(i).getLiteralValue()-(double)rvals.get(i).getLiteralValue();
            d += sq*sq;
        }
        return d;
    }

    public static Metric getMetric(DistanceFunction distFunc) {
        switch (distFunc){
            case EUCLIDEAN:
            case MANHATTAN:    return new MetricEuclideanSquared();
            default: throw new QueryExecException("Unsupported distance for approximated nn");
        }
    }

    public static double compute(List<Node> lvals, List<Node> rvals, DistanceFunction distFunc, List<Var> attrs,  Map<String,Pair<Double, Double>> norm) {
        switch (distFunc){
            case EUCLIDEAN:
                return euclidean(lvals, rvals, attrs, norm);
            case MANHATTAN:
                return manhattan(lvals, rvals, attrs, norm);
            default:
                throw new IllegalStateException("Unsupported Distance Function: "+distFunc.toString());
        }
    }
}
