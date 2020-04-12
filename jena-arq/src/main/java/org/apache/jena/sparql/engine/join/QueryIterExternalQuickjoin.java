package org.apache.jena.sparql.engine.join;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.PairOfSameType;
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

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class QueryIterExternalQuickjoin extends QueryIter2 {

    private final Var distVar;
    private final DistanceFunction distFunc;
    private final double r;
    private final List<Var> attrRight;
    private final List<Var> attrLeft;
    private final List<Binding> rightRows;
    private final List<Binding> leftRows;
    private final Iterator<Binding> iterator;

    private Stack<List<Binding>> partitions = new Stack<>();
    private Stack<PairOfSameType<List<Binding>>> winPartitions = new Stack<>();
    private int l = 30;
    private int c = 500;
    private List<Binding> results = new LinkedList<>();

    public QueryIterExternalQuickjoin(QueryIterator left, QueryIterator right, OpSimJoin sim, ExecutionContext execCxt) {
        super(left, right, execCxt);
        this.leftRows = Iter.toList(left);
        this.rightRows = Iter.toList(right);
        this.attrLeft = sim.getLeftAttrs();
        this.attrRight = sim.getRightAttrs();
        this.r = ((OpRangeSimJoin) sim).getRadius();
        this.distFunc = sim.getDistanceFunc();
        this.distVar = sim.getDist();
        quickjoin();
        this.iterator = results.iterator();
    }

    private void quickjoin() {
        List<Binding> data = new ArrayList<>(leftRows);
        data.addAll(rightRows);
        partitions.push(data);
        while (!partitions.empty()) {
            List<Binding> current = partitions.pop();
            if (current.size() <= this.c) {
                join(current);
                continue;
            }
            List<Binding> pivots = new ArrayList<>(this.l);
            ThreadLocalRandom.current().ints(0, current.size()).distinct().limit(this.l).forEach(value -> pivots.add(current.get(value)));
            List<List<Binding>> parts = new ArrayList<>(this.l);
            List<Binding>[][] windows = new List[this.l][this.l];
            partition(current, pivots, parts, windows);
            partitions.addAll(parts);
            for (int i = 0; i < windows.length - 1; i++) {
                for (int j = i + 1; i < windows[0].length; j++) {
                    PairOfSameType<List<Binding>> pair = new PairOfSameType<>(windows[i][j], windows[j][i]);
                    winPartitions.add(pair);
                }
            }
            while (!winPartitions.empty()) {
                PairOfSameType<List<Binding>> currentWin = winPartitions.pop();
                int totalLength = currentWin.getLeft().size() + currentWin.getRight().size();
                if (totalLength <= this.c) {
                    List<Binding> temp = new LinkedList<>(currentWin.getLeft());
                    temp.addAll(currentWin.getRight());
                    join(temp);
                    continue;
                }
                List<Binding> pivotsWin = new ArrayList<>(this.l);
                ThreadLocalRandom.current()
                        .ints(0, totalLength)
                        .distinct()
                        .limit(this.l)
                        .forEach(value -> {
                            if (value >= currentWin.getLeft().size())
                                pivotsWin.add(currentWin.getRight().get(value - currentWin.getLeft().size()));
                            else
                                pivotsWin.add(currentWin.getLeft().get(value));
                        });
                List<List<Binding>> parts1 = new ArrayList<>(this.l);
                List<List<Binding>> parts2 = new ArrayList<>(this.l);
                List<Binding>[][] windows1 = new List[this.l][this.l];
                List<Binding>[][] windows2 = new List[this.l][this.l];
                partition(currentWin.getLeft(), pivots, parts1, windows1);
                partition(currentWin.getRight(), pivots, parts2, windows2);
                for (int i = 0; i < this.l; i++)
                    winPartitions.add(new PairOfSameType<>(parts1.get(i), parts2.get(i)));
                for (int i = 0; i < windows.length - 1; i++) {
                    for (int j = i + 1; i < windows[0].length; j++) {
                        winPartitions.add(new PairOfSameType<>(windows1[i][j], windows2[j][i]));
                        winPartitions.add(new PairOfSameType<>(windows1[j][i], windows2[j][i]));
                    }
                }
            }
        }
    }

    private void join(List<Binding> data) {
        for (int i = 0; i < data.size() - 1; i++) {
            List<Node> lvals = new LinkedList<>();
            for (Var v : attrLeft) {
                lvals.add(data.get(i).get(v));
            }
            for (int j = i + 1; j < data.size(); j++) {
                List<Node> rvals = new LinkedList<>();
                for (Var v : attrLeft) {
                    lvals.add(data.get(j).get(v));
                }
                double dist = Distances.compute(lvals, rvals, distFunc);
                if (dist < this.r)
                    results.add(Algebra.joinR(data.get(i), data.get(j), dist, distVar));
                results.add(Algebra.joinR(data.get(j), data.get(i), dist, distVar));
            }
        }
    }

    private void partition(List<Binding> bindings, List<Binding> pivots, List<List<Binding>> parts, List<Binding>[][] windows) {
        double minDist = Double.MAX_VALUE;
        List<List<Node>> pvals = new LinkedList<>();
        for (Binding p : pivots) {
            List<Node> temp = new ArrayList<>(attrLeft.size());
            for (Var v : attrLeft) {
                temp.add(p.get(v));
            }
            pvals.add(temp);
        }
        for (Binding b : bindings) {
            List<Node> bvals = new ArrayList<>(attrLeft.size());
            for (Var v : attrLeft) {
                bvals.add(b.get(v));
            }
            int closest = -1;
            for (int i = 0; i < pivots.size(); i++) {
                double dist = Distances.compute(bvals, pvals.get(i), distFunc);
                if (dist < minDist) {
                    closest = i;
                    minDist = dist;
                }
            }
            parts.get(closest).add(b);
            for (int j = 0; j < pivots.size(); j++) {
                if (j == closest) continue;
                double halfDist = (Distances.compute(bvals, pvals.get(closest), distFunc) - Distances.compute(bvals, pvals.get(j), distFunc)) / 2.0;
                if (-halfDist < this.r) {
                    windows[j][closest].add(b);
                }
            }
        }
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
        return iterator.hasNext();
    }

    /**
     * Implement this, not next() or nextBinding()
     * Returning null is turned into NoSuchElementException
     * Does not need to call hasNext (can presume it is true)
     */
    @Override
    protected Binding moveToNextBinding() {
        return iterator.next();
    }
}
