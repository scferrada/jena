package org.apache.jena.fuseki;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.query.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.jena.sparql.engine.iterator.QueryIteratorResultSet;
import org.apache.jena.sparql.engine.join.Join;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RunQuery {

    public static void main(String[] args) throws IOException {
        String query =  "PREFIX wdt:<http://www.wikidata.org/prop/direct/>\n" +
                        "PREFIX wd:<http://www.wikidata.org/entity/>\n" +
                                "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>\n" +
                                "SELECT distinct ?city1 ?populatie ?households ?area ?elevation WHERE {\n" +
                                "GRAPH <http://imgpedia.dcc.uchile.cl/wikidata>{\n" +
                                "?city1 wdt:P31/wdt:P279* wd:Q515 ;  \n" +
                                "    wdt:P1082 ?populatie ;\n" +
                                "\twdt:P1538 ?households;\n" +
                                "\twdt:P2046 ?area;\n" +
                                "\twdt:P2044 ?elevation ." +
                                "}}";
        Map<Var, Var> varmap = new HashMap<>();
        varmap.put(new Var("city1"),new Var("city2"));
        long start = System.nanoTime();
        QueryEngineHTTP ex = (QueryEngineHTTP) QueryExecutionFactory.sparqlService("http://imgpedia.dcc.uchile.cl/sparql", query, "");
        ResultSet rs = ex.execSelect();
        rs = ResultSetFactory.makeRewindable(rs);
        Map<String, Pair<Double, Double>> minmax = normalize((ResultSetRewindable) rs);
        ResultSet ls = ResultSetFactory.copyResults(rs);
        rs = new ResultSetRename(rs,varmap);
        QueryIterator left = new QueryIteratorResultSet(ls);
        QueryIterator right = new QueryIteratorResultSet(rs);
        List<Var> attrs = new LinkedList<>();
        attrs.add(new Var("boiling_point"));
        attrs.add(new Var("populatie"));
        attrs.add(new Var("households"));
        attrs.add(new Var("area"));
        //attrs.add(new Var("water"));
        attrs.add(new Var("elevation"));
        QueryIterator queryIterator = Join.simjoin(left, right, 1, DistanceFunction.MANHATTAN, new Var("d"), attrs, minmax,null);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000.0);
        queryIterator.output(new IndentedWriter(new FileWriter("out.txt")));
        ex.close();
    }

    private static Map<String, Pair<Double, Double>> normalize(ResultSetRewindable rs) {
        List<String> vars = rs.getResultVars();
        vars.remove("city1");
        Map<String, Pair<Double, Double>> values = new HashMap<>();
        while (rs.hasNext()){
            Binding b = rs.nextBinding();
            for (String v:vars) {
                double d = ((Number)b.get(Var.alloc(v)).getLiteralValue()).doubleValue();
                if (values.get(v) == null)
                    values.put(v, new Pair<>(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
                if (d<values.get(v).getLeft()) values.get(v).setLeft(d);
                if (d>values.get(v).getRight()) values.get(v).setRight(d);
            }
        }
        rs.reset();
        return values;
    }

}
