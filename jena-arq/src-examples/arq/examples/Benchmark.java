package arq.examples;

import org.apache.jena.atlas.web.TypedInputStream;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

import java.io.File;

public class Benchmark {

    public static void main(String[] args) {
        File root = new File(args[1]);
        for(File f : root.listFiles()) {
            Model model = loadTriples(f);
            long start = System.nanoTime();
            String s = "SELECT DISTINCT ?id ?a1 ?b1 { ?id <http://ex.com/a> ?a1; <http://ex.com/b> ?b1 } similarity join on (?a1 ?b1) (?a2 ?b2) with distance manhattan as ?d top 3 SELECT DISTINCT ?id2 ?a2 ?b2 { ?id2 <http://ex.com/a> ?a2; <http://ex.com/b> ?b2 }";
            // Parse
            Query query = QueryFactory.create(s, Syntax.syntaxSPARQL_SJ_11);

            // Generate algebra
            Op op = Algebra.compile(query);
            op = Algebra.optimize(op);

            // Execute it.
            QueryIterator res = Algebra.exec(op, model);
            long end = System.nanoTime();
            // Results
            for (; res.hasNext(); ) {
                Binding b = res.nextBinding();
                System.out.println(b.toString());
            }
            //res.close() ;

            System.out.println((end - start) / 1000000.0);
        }
    }

    private static Model loadTriples(File f) {
        Model m = ModelFactory.createDefaultModel();

        return null;
    }
}
