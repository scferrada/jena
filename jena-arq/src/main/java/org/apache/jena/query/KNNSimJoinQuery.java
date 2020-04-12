package org.apache.jena.query;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpKNNSimJoin;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;

import java.util.LinkedList;
import java.util.List;

public class KNNSimJoinQuery extends SimJoinQuery{
    private int k;

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    @Override
    public Op createOp(Op left, Op right) {
        OpKNNSimJoin op = new OpKNNSimJoin(left, right);
        op.setK(k);
        op.setLeftAttrs(leftAttrs);
        op.setRightAttrs(rightAttrs);
        op.setDist(this.dist);
        op.setDistanceFunc(distance);
        return op;
    }
}
