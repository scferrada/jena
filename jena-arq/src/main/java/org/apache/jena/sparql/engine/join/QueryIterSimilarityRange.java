package org.apache.jena.sparql.engine.join;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.query.DistanceFunction;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpRangeSimJoin;
import org.apache.jena.sparql.algebra.op.OpSimJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter2;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;

import java.util.*;

public class QueryIterSimilarityRange extends QueryIter2 {

    private final double r;
    private DistanceFunction distFunc;
    private long s_countLHS     = 0;
    private long s_countRHS     = 0;
    private long s_countResults = 0;

    private final List<Binding> leftRows;
    private Iterator<Binding> left     = null;
    private QueryIterator       right;
    private Binding             rowRight = null;

    protected List<Var> attrRight;
    protected List<Var> attrLeft;

    private Binding slot     = null;
    private boolean finished = false;
    private Var distVar;

    public QueryIterSimilarityRange(QueryIterator left, QueryIterator right, OpSimJoin sim, ExecutionContext execCxt) {
        super(left, right, execCxt);
        leftRows = Iter.toList(left);
        s_countLHS = leftRows.size();
        this.right = right;
        this.attrLeft = sim.getLeftAttrs();
        this.attrRight = sim.getRightAttrs();
        this.r = ((OpRangeSimJoin)sim).getRadius();
        this.distFunc = sim.getDistanceFunc();
        this.distVar = sim.getDist();
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
        return new QueryIterSimilarityRange(left, right, opSimJoin, execCxt);
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

    /**
     * Implement this, not hasNext()
     */
    @Override
    protected boolean hasNextBinding() {
        if ( finished )
            return false;
        if ( slot == null ) {
            slot = moveToNextBindingOrNull();
            if ( slot == null ) {
                close();
                return false;
            }
        }
        return true;
    }

    private Binding moveToNextBindingOrNull() {
        if ( isFinished() )
            return null;

        for ( ;; ) { // For rows from the right.
            if ( rowRight == null ) {
                if ( right.hasNext() ) {
                    rowRight = right.next();
                    s_countRHS++;
                    left = leftRows.iterator();
                } else
                    return null;
            }
            List<Node> rvals = new LinkedList<>();
            for (Var v : attrRight) {
                rvals.add(rowRight.get(v));
            }

            // There is a rowRight
            while (left.hasNext()) {
                Binding rowLeft = left.next();
                List<Node> lvals = new LinkedList<>();
                for (Var v : attrLeft) {
                    lvals.add(rowLeft.get(v));
                }
                double distance = Distances.compute(lvals, rvals, distFunc);
                if(distance>=this.r){
                    continue;
                }
                Binding r = Algebra.joinR(rowLeft, rowRight, distance, distVar);
                return r;
            }
            // Nothing more for this rowRight.
            rowRight = null;
        }
    }

    /**
     * Implement this, not next() or nextBinding()
     * Returning null is turned into NoSuchElementException
     * Does not need to call hasNext (can presume it is true)
     */
    @Override
    protected Binding moveToNextBinding() {
        Binding r = slot;
        slot = null;
        return r;
    }
}
