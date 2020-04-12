package org.apache.jena.fuseki;

import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpKNNSimJoin;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.jena.sparql.engine.http.QueryExceptionHTTP;
import org.apache.jena.sparql.engine.iterator.QueryIteratorResultSet;
import org.apache.jena.sparql.engine.join.Join;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Services {
    public static void main(String[] args) throws IOException {
        int N = 10;
        String graphStr = "GRAPH <http://imgpedia.dcc.uchile.cl/wikidata> {";
        int[] ks = {1,2,4,8};
        File root = new File(args[0]);
        Map<Var, Var> varmap = new HashMap<>();
        varmap.put(new Var("id1"),new Var("id2"));
        for (File f : root.listFiles()) {
            System.out.println(f.getAbsolutePath());
            List<String> lines = FileUtils.readLines(f, "utf-8");
            int fin = ((lines.size()-4)/2)+2;
            lines.add(2, graphStr);
            lines.add(fin-1, "}");
            String s = String.join("\n", lines);
            String select = String.join("\n", lines.subList(0,fin+1));
            SimJoinQuery q = (SimJoinQuery) QueryFactory.create(s, Syntax.syntaxSPARQL_SJ_11);
            Op op = Algebra.compile(q);
            for (int k : ks) {
                System.out.println(k);
                if (Files.exists(Paths.get(args[1], ""+k, f.getName()+".time"))) {
                    System.out.println("Skipping "+f.getName());
                    continue;
                }
                double[] times = new double[N];
                for (int i = 0; i < N; i++) {
                    try (QueryEngineHTTP ex = (QueryEngineHTTP) QueryExecutionFactory.sparqlService("http://imgpedia.dcc.uchile.cl/sparql", select, "");){
                        long start = System.nanoTime();
                        ResultSet rs = ex.execSelect();
                        rs = ResultSetFactory.makeRewindable(rs);
                        ResultSet ls = ResultSetFactory.copyResults(rs);
                        rs = new ResultSetRename(rs,varmap);
                        QueryIterator left = new QueryIteratorResultSet(ls);
                        QueryIterator right = new QueryIteratorResultSet(rs);
                        OpKNNSimJoin opsj = (OpKNNSimJoin) ((OpProject)op).getSubOp();
                        opsj.setK(k);
                        opsj.setRightAttrs(opsj.getLeftAttrs());
                        QueryIterator queryIterator = Join.simjoin(left, right, opsj, null);
                        long end = System.nanoTime();
                        ex.close();
                        times[i] = (end - start) / 1000000.0;
                        System.out.println((end - start) / 1000000.0);
                        queryIterator.output(new IndentedWriter(new FileWriter(Paths.get(args[1], ""+k,f.getName()+i).toString())));
                    } catch (QueryExceptionHTTP e) {
                        e.printStackTrace();
                        i--;
                        continue;
                    }
                }
                FileWriter fw = new FileWriter(Paths.get(args[1], ""+k,f.getName()+".time").toString());
                fw.write(Arrays.toString(times));
                fw.close();
            }
        }
    }
}
