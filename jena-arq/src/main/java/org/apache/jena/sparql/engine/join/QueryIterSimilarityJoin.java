package org.apache.jena.sparql.engine.join;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.query.DistanceFunction;
import org.apache.jena.sparql.algebra.op.OpKNNSimJoin;
import org.apache.jena.sparql.algebra.op.OpSimJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.*;

public class QueryIterSimilarityJoin extends QueryIterSim {


    QueryIterSimilarityJoin(QueryIterator left, QueryIterator right, OpSimJoin opSimJoin, ExecutionContext execCxt) {
        super(left,right,execCxt);
        this.right = right;
        this.k = ((OpKNNSimJoin)opSimJoin).getK();
        this.attrLeft = opSimJoin.getLeftAttrs();
        this.attrRight = opSimJoin.getRightAttrs();
        this.distVar = opSimJoin.getDist();
        this.distFunc = opSimJoin.getDistanceFunc();
        this.leftRows = Iter.toList(left);
        this.left = leftRows.iterator();
        s_countLHS = leftRows.size();
        s_countResults = s_countLHS * k;
        for(int i=0; i<s_countLHS; i++){
            knn.put(i, new PriorityQueue<>(((OpKNNSimJoin)opSimJoin).getK(),Neighbor.comparator));
        }
        nestedLoop();
        consolidate();
        iterator = results.iterator();
    }

    public QueryIterSimilarityJoin(QueryIterator left, QueryIterator right, int k, DistanceFunction distFunc, Var v, List<Var> attrs, ExecutionContext execCtx) {
        super(left, right, execCtx);
        this.right = right;
        this.k = k;
        this.distVar = v;
        this.attrRight = this.attrLeft = attrs.subList(1, attrs.size());
        this.distFunc = distFunc;
        this.leftRows = Iter.toList(left);
        this.left = leftRows.iterator();
        s_countLHS = leftRows.size();
        s_countResults = s_countLHS * k;
        for(int i=0; i<s_countLHS; i++){
            knn.put(i, new PriorityQueue<>(k,Neighbor.comparator));
        }
        nestedLoop();
        consolidate();
        iterator = results.iterator();
    }

    public QueryIterSimilarityJoin(QueryIterator left, QueryIterator right, int k, DistanceFunction distFunc, Var v, List<Var> attrs, Map<String, Pair<Double, Double>> minmax, ExecutionContext execCxt) {
        super(left, right, execCxt);
        this.norm = minmax;
        this.right = right;
        this.k = k;
        this.distVar = v;
        this.attrRight = this.attrLeft = attrs.subList(1, attrs.size());
        this.distFunc = distFunc;
        this.leftRows = Iter.toList(left);
        this.left = leftRows.iterator();
        s_countLHS = leftRows.size();
        s_countResults = s_countLHS * k;
        for(int i=0; i<s_countLHS; i++){
            knn.put(i, new PriorityQueue<>(k,Neighbor.comparator));
        }
        nestedLoop();
        consolidate();
        iterator = results.iterator();
    }


    private void nestedLoop() {
        while (right.hasNext()){
            Binding r = right.nextBinding();
//            List<Node> rKey = getKey(r, attrRight);
            List<Node> rvals = new ArrayList<>();
            for (Var v: attrRight) {
                rvals.add(r.get(v));
            }
            int i = 0;
            while(left.hasNext()){
                s_countRHS++;
                Binding l = left.next();
//                List<Node> lKey = getKey(l, attrLeft);
//                if(sameKey(rKey,lKey)){
//                    i++;
//                    continue;
//                }
                List<Node> lvals = new ArrayList<>();
                for (Var v: attrLeft) {
                    lvals.add(l.get(v));
                }
                double d = Distances.compute(lvals, rvals, distFunc, attrLeft, norm);
                if (knn.get(i).size() < k+1){
//                    if(isAlreadyIn(rKey,knn.get(i))){
//                        if (d<knn.get(i).peek().getDistance())
//                            remove(rKey, knn.get(i));
//                    }
                    knn.get(i).add(new Neighbor<>(r, d));
                } else if(d< knn.get(i).peek().getDistance()){
                    knn.get(i).poll();
                    knn.get(i).add(new Neighbor<>(r, d));
                }
                i++;
            }
            left = leftRows.iterator();
        }
    }

    private void remove(List<Node> rKey, PriorityQueue<Neighbor> neighbors) {
        List<Neighbor> toRemove = new LinkedList<>();
        for(Neighbor n : neighbors) {
            for (Node key : rKey) {
                Binding b = ((Binding) n.getKey());
                for (Iterator<Var> it = b.vars(); it.hasNext(); ) {
                    Var v = it.next();
                    if (v.isURI()) {
                        if (!v.getURI().equalsIgnoreCase(key.getURI()))
                            continue;
                    }
                }
            }
            toRemove.add(n);
        }
        neighbors.removeAll(toRemove);
    }

    private boolean isAlreadyIn(List<Node> rKey, PriorityQueue<Neighbor> neighbors) {
        if (neighbors.size()==0) return false;
        for(Neighbor n : neighbors) {
            for (Node key : rKey) {
                Binding b = ((Binding) n.getKey());
                for (Iterator<Var> it = b.vars(); it.hasNext(); ) {
                    Var v = it.next();
                    if (v.isURI()) {
                        if (!v.getURI().equalsIgnoreCase(key.getURI()))
                            return false;
                    }
                }
            }
        }
        return true;
    }

    public static QueryIterator create(QueryIterator left, QueryIterator right, OpSimJoin opSimJoin, ExecutionContext execCxt) {
        /*if ( ! left.hasNext() ) {
            left.close() ;
            right.close() ;
            return QueryIterNullIterator.create(execCxt) ;
        }
        if ( ! right.hasNext() ) {
            right.close() ;
            return left ;
        }*/
        return new QueryIterSimilarityJoin(left, right, opSimJoin, execCxt);
    }

    /**
     * Implement this, not hasNext()
     */
    @Override
    protected boolean hasNextBinding() {
        if(slot!=null) return true;
        if(iterator.hasNext()){
            slot = iterator.next();
            return true;
        }
        return false;
    }

    /**
     * Implement this, not next() or nextBinding()
     * Returning null is turned into NoSuchElementException
     * Does not need to call hasNext (can presume it is true)
     */
    @Override
    protected Binding moveToNextBinding() {
        if ( !hasNextBinding() )
            return null;
        Binding x = slot;
        slot = null;
        return x;
    }

    /**
     * Cancellation of the query execution is happening
     */
    @Override
    protected void requestSubCancel() {

    }

    /**
     * Pass on the close method - no need to close the left or right QueryIterators passed to the QueryIter2 constructor
     */
    @Override
    protected void closeSubIterator() {
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        super.output(out, sCxt);
    }

}
