package org.apache.jena.sparql.engine.join;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.query.DistanceFunction;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpSimJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter2;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.*;

public class QueryIterSimilarityJoin extends QueryIter2 {

    private final int k;
    private final Iterator<Binding> iterator;
    private long s_countLHS = 0;
    private long s_countRHS = 0;
    private long s_countResults = 0;

    private final Var distVar;
    private final DistanceFunction distFunc;

    private final List<Binding> leftRows;
    private Iterator<Binding> left;
    private QueryIterator right;

    private final List<Var> attrRight;
    private final List<Var> attrLeft;

    private Binding slot = null;
    private boolean finished = false;

    private Map<Integer, PriorityQueue<Neighbor>> knn = new HashMap<>();
    private List<Binding> results = new LinkedList<>();

    private QueryIterSimilarityJoin(QueryIterator left, QueryIterator right, OpSimJoin opSimJoin, ExecutionContext execCxt) {
        super(left,right,execCxt);
        this.right = right;
        this.k = opSimJoin.getK();
        this.attrLeft = opSimJoin.getAttr1();
        this.attrRight = opSimJoin.getAttr2();
        this.distVar = opSimJoin.getDist();
        this.distFunc = opSimJoin.getDistanceFunc();
        this.leftRows = Iter.toList(left);
        this.left = leftRows.iterator();
        s_countLHS = leftRows.size();
        s_countResults = s_countLHS * k;
        for(int i=0; i<s_countLHS; i++){
            knn.put(i, new PriorityQueue<>(opSimJoin.getK(),Neighbor.comparator));
        }
        nestedLoop();
        consolidate();
        iterator = results.iterator();
    }

    private void consolidate() {
        for (int i: knn.keySet()) {
            List<Binding> res = Algebra.join(leftRows.get(i), knn.get(i), distVar);
            results.addAll(res);
        }
    }

    //TODO: do external quickjoin
    private void nestedLoop() {
        while (right.hasNext()){
            Binding r = right.nextBinding();
            List<Node> rKey = getKey(r, attrRight);
            List<Node> rvals = new ArrayList<>();
            for (Var v: attrRight) {
                rvals.add(r.get(v));
            }
            int i = 0;
            while(left.hasNext()){
                s_countRHS++;
                Binding l = left.next();
                List<Node> lKey = getKey(l, attrLeft);
                if(sameKey(rKey,lKey)){
                    i++;
                    continue;
                }
                List<Node> lvals = new ArrayList<>();
                for (Var v: attrLeft) {
                    lvals.add(l.get(v));
                }
                double d = Distances.compute(lvals, rvals, distFunc);
                if (knn.get(i).size() < k){
                    knn.get(i).add(new Neighbor<>(r, d));
                } else if(d< knn.get(i).peek().distance){
                    knn.get(i).poll();
                    knn.get(i).add(new Neighbor<>(r, d));
                }
                i++;
            }
            left = leftRows.iterator();
        }
    }

    private boolean sameKey(List<Node> rKey, List<Node> lKey) {
        if(rKey.size()!=lKey.size())
            return false;
        for(int i=0; i<rKey.size();i++){
            if(!rKey.get(i).getURI().equals(lKey.get(i).getURI()))
                return false;
        }
        return true;
    }

    private List<Node> getKey(Binding binding, List<Var> vars) {
        List<Node> key = new ArrayList<>();
        for (Iterator<Var> it = binding.vars(); it.hasNext(); ) {
            Var v = it.next();
            if(!vars.contains(v)){
                key.add(binding.get(v));
            }
        }
        return key;
    }

    public static QueryIterator create(QueryIterator left, QueryIterator right, OpSimJoin opSimJoin, ExecutionContext execCxt) {
        if ( ! left.hasNext() ) {
            left.close() ;
            right.close() ;
            return QueryIterNullIterator.create(execCxt) ;
        }
        if ( ! right.hasNext() ) {
            right.close() ;
            return left ;
        }
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

    public static class Neighbor<K>{
        private K key;
        private double distance;

        static public Comparator<Neighbor> comparator= (n1, n2) -> n1.distance>n2.distance? -1:1;

        protected Neighbor(K key, double distance) {
            this.key = key;
            this.distance = distance;
        }

        public K getKey() {
            return key;
        }

        public double getDistance() {
            return distance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Neighbor<?> neighbor = (Neighbor<?>) o;
            return key.equals(neighbor.key) &&
                    distance == neighbor.distance;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, distance);
        }
    }

}
