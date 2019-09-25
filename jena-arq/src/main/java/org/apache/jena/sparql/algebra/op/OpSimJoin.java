package org.apache.jena.sparql.algebra.op;

import org.apache.jena.query.DistanceFunction;
import org.apache.jena.query.SimJoinQuery;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.sse.Tags;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.List;

public abstract class OpSimJoin extends Op2 {

    protected DistanceFunction distance;
    protected Var dist;
    protected List<Var> leftAttrs, rightAttrs;

    public OpSimJoin(Op left, Op right) {
        super(left, right);
    }

    public static OpSimJoin create(Op left, Op right, SimJoinQuery query) {
        return query.createOp(left, right);
    }

    public abstract QueryIterator createIterator(QueryIterator left, QueryIterator right);

    @Override
    public Op apply(Transform transform, Op left, Op right) {
        return transform.transform(this, left, right);
    }

    @Override
    public void visit(OpVisitor opVisitor) {
        opVisitor.visit(this);
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpSimJoin)) return false ;
        return super.sameArgumentsAs((Op2)other, labelMap) ;
    }

    @Override
    public String getName() {
        return Tags.tagSimjoin;
    }

    public DistanceFunction getDistanceFunc(){
        return distance;
    }

    public void setDistanceFunc(DistanceFunction distance) {
        this.distance = distance;
    }

    public Var getDist() {
        return dist;
    }

    public void setDist(Var dist) {
        this.dist = dist;
    }

    public List<Var> getLeftAttrs() {
        return leftAttrs;
    }

    public void setLeftAttrs(List<Var> leftAttrs) {
        this.leftAttrs = leftAttrs;
    }

    public List<Var> getRightAttrs() {
        return rightAttrs;
    }

    public void setRightAttrs(List<Var> rightAttrs) {
        this.rightAttrs = rightAttrs;
    }

    public void setLeft(Op l){
        this.left = l;
    }

    public void setRight(Op r){
        this.right = r;
    }
}
