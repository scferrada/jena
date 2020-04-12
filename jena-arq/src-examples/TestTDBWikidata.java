import org.apache.jena.query.*;
import org.apache.jena.tdb.TDBFactory;

public class TestTDBWikidata {

    public static void main(String[] args) {
        Dataset dataset = TDBFactory.createDataset("wikidata");
        System.out.println("got data");
        try (QueryExecution qExec = QueryExecutionFactory.create("SELECT * { ?s ?p ?o} limit 3", dataset) ) {
            System.out.println("goin2 exec");
            ResultSet rs = qExec.execSelect() ;
            System.out.println("exec don");
            while(rs.hasNext()){
                System.out.println(rs.next());
            }
        }
    }


}
