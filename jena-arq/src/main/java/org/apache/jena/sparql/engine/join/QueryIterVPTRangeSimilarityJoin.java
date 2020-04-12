package org.apache.jena.sparql.engine.join;

import com.eatthepath.jvptree.DistanceFunction;
import com.eatthepath.jvptree.VPTree;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpRangeSimJoin;
import org.apache.jena.sparql.algebra.op.OpSimJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter2;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class QueryIterVPTRangeSimilarityJoin extends QueryIter2 {

    private final double r;
    private org.apache.jena.query.DistanceFunction distFunc;
    private long s_countLHS     = 0;
    private long s_countRHS     = 0;
    private long s_countResults = 0;

    private final List<Binding> rightRows;
    private Iterator<Binding> right = null;
    private QueryIterator left;
    private Binding rowLeft = null;

    protected List<Var> attrRight;
    protected List<Var> attrLeft;

    private Binding slot     = null;
    private boolean finished = false;
    private Var distVar;

    VPTree<List<Double>, QueryIterVPTSimilarityJoin.VPVector<Binding>> index;
    private Iterator<QueryIterVPTSimilarityJoin.VPVector<Binding>> cache;

    private QueryIterVPTRangeSimilarityJoin(QueryIterator left, QueryIterator right, OpSimJoin sim, ExecutionContext execCxt) {
        super(left,right,execCxt);
        rightRows = Iter.toList(right);
        s_countLHS = rightRows.size();
        this.left = left;
        this.attrLeft = sim.getLeftAttrs();
        this.attrRight = sim.getRightAttrs();
        this.r = ((OpRangeSimJoin)sim).getRadius();
        this.distFunc = sim.getDistanceFunc();
        this.distVar = sim.getDist();
        DistanceFunction<List<Double>> fun = (l, rg) -> {
            double d = 0;
            for(int i=0; i<l.size();i++){
                d += (l.get(i)-rg.get(i))*(l.get(i)-rg.get(i));
            }
            return Math.sqrt(d);
        };
        List<QueryIterVPTSimilarityJoin.VPVector<Binding>> data = materialize();
        this.index = new VPTree(fun, data);
    }

    private List<QueryIterVPTSimilarityJoin.VPVector<Binding>> materialize() {
        List<QueryIterVPTSimilarityJoin.VPVector<Binding>> res = new LinkedList<>();
        for(Binding b : rightRows){
            List<Double> row = new LinkedList<>();
            for (Var v : attrRight ) {
                row.add(Double.parseDouble((String) b.get(v).getLiteralValue()));
            }
            res.add(new QueryIterVPTSimilarityJoin.VPVector<>(b, row));
        }
        return res;
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
        return new QueryIterVPTRangeSimilarityJoin(left, right, opSimJoin, execCxt);
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
        if(cache != null && this.cache.hasNext())
            return this.cache.next().getKey();
        for ( ;; ) { // For rows from the left.
            if (rowLeft == null) {
                if (left.hasNext()) {
                    rowLeft = left.next();
                    s_countRHS++;
                } else
                    return null;
            }
            List<Double> lvals = new LinkedList<>();
            for (Var v : attrLeft) {
                lvals.add(Double.parseDouble((String) rowLeft.get(v).getLiteralValue()));
            }
            List<QueryIterVPTSimilarityJoin.VPVector<Binding>> x= index.getAllWithinDistance(new QueryIterVPTSimilarityJoin.VPVector<>(rowLeft, lvals), this.r);
            this.cache = x.iterator();
            rowLeft = null;
            return this.cache.next().getKey();
        }
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
}
