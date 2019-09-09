package org.apache.jena.sparql.engine.join;

import org.apache.jena.graph.Node;
import org.apache.jena.query.DistanceFunction;

import java.util.List;

public class Distances {
    public static double compute(List<Node> lvals, List<Node> rvals, DistanceFunction distFunc) {
        switch (distFunc){
            case EUCLIDEAN:
                return euclidean(lvals, rvals);
            case MANHATTAN:
                return manhattan(lvals, rvals);
            default:
                throw new IllegalStateException("Unsupported Distance Function: "+distFunc.toString());
        }
    }

    private static double manhattan(List<Node> lvals, List<Node> rvals) {
        double d = 0;
        for (int i = 0; i < lvals.size(); i++) {
            d += Math.abs(Double.parseDouble((String) lvals.get(i).getLiteralValue())-Double.parseDouble((String) rvals.get(i).getLiteralValue()));
        }
        return d;
    }

    private static double euclidean(List<Node> lvals, List<Node> rvals) {
        double d = 0;
        for (int i = 0; i < lvals.size(); i++) {
            double sq = (double)lvals.get(i).getLiteralValue()-(double)rvals.get(i).getLiteralValue();
            d += sq*sq;
        }
        return d;
    }
}
