package arq.examples;

import org.apache.jena.atlas.web.TypedInputStream;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

import java.io.File;

public class Benchmark {

    public static void main(String[] args) {

            double promedio = 0.0;
        for (int w=0; w<8; w++) {
            long start = System.nanoTime();
            Model model = ExQuerySelect2.createSimModel2(args[0]);
            String s = "SELECT *  WHERE{ " +
                    "?id1 <http://ex.com/b> ?b1 ; " +
                    "<http://ex.com/c> ?c1 ;" +
                    "<http://ex.com/d> ?d1 ;" +
                    "<http://ex.com/e> ?e1 ;" +
                    "<http://ex.com/f> ?f1 ;" +
                    "<http://ex.com/g> ?g1 ;" +
                    "<http://ex.com/h> ?h1 ;" +
                    "<http://ex.com/i> ?i1  ." +
                    " } similarity join on (?b1 ?c1 ?d1 ?e1 ?f1 ?g1 ?h1 ?i1) " +
                    "(?b2 ?c2 ?d2 ?e2 ?f2 ?g2 ?h2 ?i2) " +
                    "with distance euclidean as ?d within 5.9 " +
                    "SELECT * WHERE{" +
                    "?id2 <http://ex.com/b> ?b2 ; " +
                    "<http://ex.com/c> ?c2 ; " +
                    "<http://ex.com/d> ?d2 ; " +
                    "<http://ex.com/e> ?e2 ; " +
                    "<http://ex.com/f> ?f2 ; " +
                    "<http://ex.com/g> ?g2 ; " +
                    "<http://ex.com/h> ?h2 ; " +
                    "<http://ex.com/i> ?i2 . }" ;
            // Parse
            Query query = QueryFactory.create(s, Syntax.syntaxSPARQL_SJ_11);

            // Generate algebra
            Op op = Algebra.compile(query);

            op = Algebra.optimize(op);
            // Execute it.
            QueryIterator res = Algebra.exec(op, model);

            // Results
            int results = 0;
            for (; res.hasNext(); ) {
                results ++;
                res.nextBinding();
            }
            long end = System.nanoTime();
            System.out.println(results);
            System.out.println((end - start) / 1000000.0);
            promedio += (end - start) / 1000000.0;
        }
        System.out.println(promedio/8.0);
    }
}
