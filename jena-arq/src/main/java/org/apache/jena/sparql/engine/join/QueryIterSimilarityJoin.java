package org.apache.jena.sparql.engine.join;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.op.OpSimJoin;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.ujmp.core.DenseMatrix;

import java.util.*;

public class QueryIterSimilarityJoin{

    private long s_countLHS     = 0;
    private long s_countRHS     = 0;
    private long s_countResults = 0;

    private final List<Binding> leftRows;
    private QueryIterator   left;
    private QueryIterator       right;
    private Binding             rowRight = null;

    private Binding slot     = null;
    private boolean finished = false;

    private  OpSimJoin opSimJoin;

    private Map<Integer, PriorityQueue> results = new HashMap<>();

    public QueryIterSimilarityJoin(QueryIterator left, QueryIterator right, OpSimJoin opSimJoin, ExecutionContext execCxt) {
        this.right = right;
        this.left = left;
        this.opSimJoin = opSimJoin;
        this.leftRows = Iter.toList(left);
        s_countLHS = leftRows.size();
        for(int i=0; i<s_countLHS; i++){
            results.put(i, new PriorityQueue<Double>(opSimJoin.getK(),Collections.reverseOrder()));
        }
    }

    //TODO:la mayía pasa aqui
    public QueryIterator compute() {
        //DenseMatrix64F leftData = new DenseMatrix64F(leftRows.size(), opSimJoin.getAttr1().size());
        //DenseMatrix64F rightData = new DenseMatrix64F();
        DenseMatrix x = DenseMatrix.Factory.zeros(4,4);
        for ( ; left.hasNext() ; )
        {
            Binding b = left.nextBinding() ;
            System.out.println(b) ;
        }
        left.close() ;
        return null;
    }
}
