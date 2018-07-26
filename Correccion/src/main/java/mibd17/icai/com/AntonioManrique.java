package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;/
import org.apache.hadoop.hbase.util.Bytes;

public class AntonioManrique {
    public static void main(String[] args) {
        // Obtenemos la configuración del cluster
        Configuration conf = HBaseConfiguration.create();
//        conf.addResource(new Path("/home/icai/tmp/Cloudera/hbase-site.xml"));
//        conf.addResource(new Path("/home/icai/tmp/Cloudera/core-site.xml"));

        Connection connection;
        Admin adm = null;

        try {
            connection = ConnectionFactory.createConnection(conf);
            adm = connection.getAdmin();

            String tableName = "PjelSpace:Tabla1Pema";
            TableName table = TableName.valueOf(tableName);

            if (!adm.tableExists(table)) {
                System.exit(1);
            }

            //Recuperamos la tabla
            Table tbl = connection.getTable(table);

            System.out.println("\n---- Ejercicios con Scan ----\n");

            System.out.println("\nObtener la última versión de las filas\n");
            Scan scan = new Scan();
            ResultScanner scanner = tbl.getScanner(scan);
            imprime(scanner);

            System.out.println("\nObtener las últimas 2 versiones de todas las filas (incluyendo deletes)\n");
            scan = new Scan();
            scan.setMaxVersions(2);
            scan.setRaw(true);
            scanner = tbl.getScanner(scan);
            imprime(scanner);

            System.out.println("\nObtener las últimas 2 versiones de todas las filas (sin incluir deletes) \n");
            scan.setMaxVersions(2);
            scan.setRaw(false);
            scanner = tbl.getScanner(scan);
            imprime(scanner);


            System.out.println("\nObtener la última version  de la familia dv de todas las filas\n");
            scan = new Scan();
            scan.addFamily(Bytes.toBytes("dv"));
            scanner = tbl.getScanner(scan);
            imprime(scanner);

            System.out.println("\nObtener la última version  de Nombre, color y modelo de las filas entre 0002 (inclusive) y 0004 (no inclusive)\n");
            scan = new Scan();
            scan.addColumn(Bytes.toBytes("dp"), Bytes.toBytes("Nombre")).
                    addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Color")).
                    addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Modelo")).
                    setStartRow(Bytes.toBytes("0000")).
                    setStopRow(Bytes.toBytes("0004"));
            scanner = tbl.getScanner(scan);
            imprime(scanner);

            System.out.println("\nObtener la última version  de matricula de las filas entre 0110 y 0200\n");
            scan = new Scan();
            scan.addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Matricula")).
                    setStartRow(Bytes.toBytes("011")).
                    setStopRow(Bytes.toBytes("02"));
            scanner = tbl.getScanner(scan);
            imprime(scanner);

            System.out.println("\nObtener la última version  de Nombre de las filas entre 0020 y 0000 en orden inverso\n");
            scan = new Scan();
            scan.addColumn(Bytes.toBytes("dp"), Bytes.toBytes("Nombre")).
                    setStartRow(Bytes.toBytes("0020")).
                    setStopRow(Bytes.toBytes("0000")).
                    setReversed(true);
            scanner = tbl.getScanner(scan);
            imprime(scanner);

            System.out.println("\nObtener la última version comprendidas entre 2 y 4 de las todas filas\n");
            scan = new Scan();
            scan.setTimeRange(2, 4);
            scanner = tbl.getScanner(scan);
            imprime(scanner);

            //Ejercicios de filtros
            System.out.println("\n--- Ejercicios Filter ----\n");
            System.out.println("\nObtener aquellas filas que sean:\n");
            System.out.println("\n-id igual a 0004\n");
            scan = new Scan();
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("0004")));
            scan.setFilter(filter);
            scanner = tbl.getScanner(scan);
            imprime(scanner);

            System.out.println("\n-id menor que 0004\n");
            scan = new Scan();
            filter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("0004")));
            scan.setFilter(filter);
            scanner = tbl.getScanner(scan);
            imprime(scanner);

            System.out.println("\n-id mayor que 0004\n");
            scan = new Scan();
            filter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes("0004")));
            scan.setFilter(filter);
            scanner = tbl.getScanner(scan);
            imprime(scanner);

            System.out.println("\n-id que contenga el substring 02\n");
            scan = new Scan();
            filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("02"));
            scan.setFilter(filter);
            scanner = tbl.getScanner(scan);
            imprime(scanner);

            tbl.close();
            connection.close();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }

    public static void imprime(ResultScanner scanner){
        for (Result res : scanner) {
            System.out.println(res);
        }
        scanner.close();
    }
}
