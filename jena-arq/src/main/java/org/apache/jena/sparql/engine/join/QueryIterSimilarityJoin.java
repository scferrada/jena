package org.apache.jena.sparql.engine.join;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.query.DistanceFunction;
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
    private long s_countLHS = 0;
    private long s_countRHS = 0;
    private long s_countResults = 0;

    private final Var distVar;
    private final DistanceFunction distFunc;

    private final List<Binding> leftRows;
    private Iterator<Binding> left;
    private QueryIterator right;
    private Binding rowRight = null;

    private final Iterable<Var> attrRight;
    private final Iterable<Var> attrLeft;

    private Binding slot = null;
    private boolean finished = false;

    private Map<Integer, PriorityQueue> results = new HashMap<>();

    public class Neighbor<K, V>{
        private K key;
        private V distance;

        protected Neighbor(K key, V distance) {
            this.key = key;
            this.distance = distance;
        }

        public K getKey() {
            return key;
        }

        public V getDistance() {
            return distance;
        }
    }

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
        for(int i=0; i<s_countLHS; i++){
            results.put(i, new PriorityQueue<Double>(opSimJoin.getK(),Collections.reverseOrder()));
        }
        quickJoin();
    }
    //TODO: do external quickjoin
    private void quickJoin() {

    }

    //TODO:la mayía pasa aqui
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
        return left.hasNext();
    }

    /**
     * Implement this, not next() or nextBinding()
     * Returning null is turned into NoSuchElementException
     * Does not need to call hasNext (can presume it is true)
     */
    @Override
    protected Binding moveToNextBinding() {
        return left.next();
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
