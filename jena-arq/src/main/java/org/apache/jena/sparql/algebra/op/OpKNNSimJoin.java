package org.apache.jena.sparql.algebra.op;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.join.QueryIterFLANNSimilarityJoin;
import org.apache.jena.sparql.engine.join.QueryIterSimilarityJoin;
import org.apache.jena.sparql.engine.join.QueryIterVPTSimilarityJoin;

import java.util.LinkedList;

public class OpKNNSimJoin extends OpSimJoin {

    private int k;

    public OpKNNSimJoin(Op left, Op right) {
        super(left, right);
    }

    @Override
    public QueryIterator createIterator(QueryIterator left, QueryIterator right, ExecutionContext execCxt) {
        return QueryIterFLANNSimilarityJoin.create(left, right, this, execCxt);
    }


    public Op2 copy(Op left, Op right) {
        OpKNNSimJoin sj = new OpKNNSimJoin(left, right);
        sj.k = this.k;
        sj.distance = this.distance;
        sj.dist = this.dist;
        sj.leftAttrs = new LinkedList<>(this.leftAttrs);
        sj.rightAttrs = new LinkedList<>(this.rightAttrs);
        return sj;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }
}
