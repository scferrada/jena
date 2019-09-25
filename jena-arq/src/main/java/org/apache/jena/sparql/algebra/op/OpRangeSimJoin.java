package org.apache.jena.sparql.algebra.op;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.join.QueryIterSimilarityRange;

public class OpRangeSimJoin extends OpSimJoin {

    private double radius;

    public double getRadius() {
        return radius;
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }

    public OpRangeSimJoin(Op left, Op right) {
        super(left, right);
    }

    @Override
    public QueryIterator createIterator(QueryIterator left, QueryIterator right) {
        return QueryIterSimilarityRange.create(left, right, this, null);
    }

    @Override
    public Op2 copy(Op left, Op right) {
        OpRangeSimJoin sj = new OpRangeSimJoin(this.left, this.right);
        sj.radius = this.radius;
        sj.distance = this.distance;
        sj.dist = this.dist;
        sj.leftAttrs = this.leftAttrs;
        sj.rightAttrs = this.rightAttrs;
        return sj;
    }
}
