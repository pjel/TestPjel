package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class AntonioMoreno {
    public static void main(String [] args) {
        // Obtenemos la configuración del cluster
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/home/icai/tmp/Cloudera/hbase-site.xml"));
        conf.addResource(new Path("/home/icai/tmp/Cloudera/core-site.xml"));


        Connection connection;
        Admin adm = null;

        try{
            connection = ConnectionFactory.createConnection(conf);
            adm = connection.getAdmin();

            String tableName = "amm:ammtab";
            TableName table = TableName.valueOf(tableName);

            if(!adm.tableExists(table)){
                System.exit(1);
            }

            //Recuperamos la tabla
            Table tbl = connection.getTable(table);

            //Obtener la última versión de todas las filas
            Scan scan1 = new Scan();
            ResultScanner scanner1 = tbl.getScanner(scan1);

            for (Result res : scanner1) {
                System.out.println(res);
            }
            scanner1.close();

            System.out.println("\n\nObtener las últimas 5 versiones de todas las filas\n\n");

            //Obtener las últimas 5 versiones de todas las filas
            Scan scan11 = new Scan();
            scan11.setMaxVersions(5);
            scan11.setRaw(true);
            ResultScanner scanner11 = tbl.getScanner(scan11);

            for (Result res : scanner11) {
                System.out.println(res);
            }
            scanner11.close();


            System.out.println("\n\nObtener la última version  de la familia dp de todas las filas\n\n");

            Scan scan2 = new Scan();
            scan2.addFamily(Bytes.toBytes("dp"));
            ResultScanner scanner2 = tbl.getScanner(scan2);
            for (Result res : scanner2) {
                System.out.println(res);
            }
            scanner2.close();

            System.out.println("\n\nObtener la última version  de matricula, color y año de las filas entre 0000 y 0012\n\n");


            Scan scan3 = new Scan();
            scan3.addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Matricula")).
                    addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Color")).
                    addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Anyo")).
                    setStartRow(Bytes.toBytes("0000")).
                    setStopRow(Bytes.toBytes("0012"));
            ResultScanner scanner3 = tbl.getScanner(scan3);
            for (Result res : scanner3) {
                System.out.println(res);
            }
            scanner3.close();

            System.out.println("\n\nObtener la última version  de matricula de las filas entre 0100 y 0200\n\n");
            Scan scan4 = new Scan();
            scan4.addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Matricula")).
                    setStartRow(Bytes.toBytes("01")).
                    setStopRow(Bytes.toBytes("02"));
            ResultScanner scanner4 = tbl.getScanner(scan4);
            for (Result res : scanner4) {
                System.out.println(res);
            }
            scanner4.close();

            System.out.println("\n\nObtener la última version  de matricula de las filas entre 0020 y 0000 en orden inverso\n\n");
            Scan scan5 = new Scan();
            scan5.addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Color")).
                    setStartRow(Bytes.toBytes("0020")).
                    setStopRow(Bytes.toBytes("0000")).
                    setReversed(true);
            ResultScanner scanner5 = tbl.getScanner(scan5);
            for (Result res : scanner5) {
                System.out.println(res);
            }
            scanner5.close();


            System.out.println("\n\nObtener la última version comprendidas entre 10 y 99 de las todas filas\n\n");
            Scan scan6 = new Scan();
            scan6.setTimeRange(10,99);
            ResultScanner scanner6 = tbl.getScanner(scan6);

            for (Result res : scanner6) {
                System.out.println(res);
            }
            scanner6.close();

            System.out.println("\n\nObtener la última version de color y que estén entre 5 y 56 de las todas filas\n\n");
            Scan scan7 = new Scan();
            scan7.addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Color")).
                    scan7.setTimeRange(5,56);
            ResultScanner scanner7 = tbl.getScanner(scan7);

            for (Result res : scanner7) {
                System.out.println(res);
            }
            scanner7.close();

            System.out.println("\n\nObtener la última version  del año  comprendidas entre 0001 y 0300 \n\n");
            Scan scan8 = new Scan();
            scan8.addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Anyo")).

                    setStartRow(Bytes.toBytes("0001")).
                    setStopRow(Bytes.toBytes("0300")).

                    ResultScanner scanner8 = tbl.getScanner(scan8);
            for (Result res : scanner8) {
                System.out.println(res);
            }
            scanner8.close();

            System.out.println("\n\nObtener la última version  de la familia dp de todas las filas en orden inverso\n\n");

            Scan scan9 = new Scan();
            scan9.addFamily(Bytes.toBytes("dp"));
            ResultScanner scanner9 = tbl.getScanner(scan9);
            setReversed(true);
            for (Result res : scanner9) {
                System.out.println(res);
            }
            scanner9.close();


            System.out.println("\n\nObtener la última version  de nombres de las filas entre 0000 y 0300 \n\n");
            Scan scan10 = new Scan();
            scan10.addColumn(Bytes.toBytes("dp"), Bytes.toBytes("Nombre")).
                    setStartRow(Bytes.toBytes("0000")).
                    setStopRow(Bytes.toBytes("0030")).

                    ResultScanner scanner10 = tbl.getScanner(scan10);
            for (Result res : scanner10) {
                System.out.println(res);
            }
            scanner10.close();



            tbl.close();
            connection.close();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");

    }

}
