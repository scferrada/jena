package org.apache.jena.sparql.algebra.op;

import org.apache.jena.query.DistanceFunction;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.sse.Tags;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.LinkedList;
import java.util.List;

public class OpSimJoin extends Op2 {

    private DistanceFunction distance;
    private Var dist;
    private int k;
    private List<Var> attr1, attr2;

    public OpSimJoin(Op left, Op right) {
        super(left, right);
    }

    @Override
    public Op apply(Transform transform, Op left, Op right) {
        return transform.transform(this, left, right);
    }

    @Override
    public Op2 copy(Op left, Op right) {
        OpSimJoin sj = new OpSimJoin(left, right);
        sj.k = this.k;
        sj.distance = this.distance;
        sj.dist = this.dist;
        sj.attr1 = new LinkedList<>(this.attr1);
        sj.attr2 = new LinkedList<>(this.attr2);
        return sj;
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

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public List<Var> getAttr1() {
        return attr1;
    }

    public void setAttr1(List<Var> attr1) {
        this.attr1 = attr1;
    }

    public List<Var> getAttr2() {
        return attr2;
    }

    public void setAttr2(List<Var> attr2) {
        this.attr2 = attr2;
    }
}
