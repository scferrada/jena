package org.apache.jena.query;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpRangeSimJoin;
import org.apache.jena.sparql.algebra.op.OpSimJoin;

public class RangeSimJoinQuery extends SimJoinQuery{
    private double radius;

    public double getRadius() {
        return radius;
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }

    @Override
    public OpSimJoin createOp(Op left, Op right) {
        OpRangeSimJoin op = new OpRangeSimJoin(left, right);
        op.setRadius(radius);
        op.setLeftAttrs(leftAttrs);
        op.setRightAttrs(rightAttrs);
        op.setDist(dist);
        op.setDistanceFunc(distance);
        return op;
    }
}
