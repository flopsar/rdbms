package org.flopsar.rdbms;

import com.flopsar.fdbc.api.Connection;
import com.flopsar.fdbc.api.ConnectionFactory;
import com.flopsar.fdbc.exception.FDBCException;
import org.flopsar.rdbms.jdbc.JDBCConnector;
import org.flopsar.rdbms.jdbc.PostgresDML;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;


public class RDBMS {

    private static final String OPTION_JDBC_HOST = "jdbc.host";
    private static final String OPTION_JDBC_PORT = "jdbc.port";
    private static final String OPTION_JDBC_USER = "jdbc.user";
    private static final String OPTION_JDBC_PASSWORD = "jdbc.password";
    private static final String OPTION_JDBC_DATABASE = "jdbc.database";
    private static final String OPTION_JDBC_DRIVER = "jdbc.driver";
    private static final String OPTION_FDBC_DATABASE = "fdbc.database";

    private static final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss");
    private final static Executor TRANSPORT_POOL = Executors.newCachedThreadPool();

    private enum MODE {
        ONE("one"),ALL("all"),CYCLIC("cyclic");

        private String n;
        MODE(String n){
            this.n = n;
        }
    }


    private static Properties readSettings(String settings) {
        Properties props = new Properties();
        FileInputStream fis  = null;
        try {
            fis = new FileInputStream(settings);
            props.load(new FileInputStream(settings));
            return props;
        } catch (Exception ex){
            ex.printStackTrace();
            return null;
        } finally {
            if(fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }



    private static long readTime(String t){
            try {
                return dateTimeFormat.parse(t).getTime();
            } catch (ParseException e) {
                System.out.println("Valid date time format: "+dateTimeFormat.toPattern());
                return -1;
            }
    }




    private static void oneShot(Map<String,String> options,final long timeFrom,final long timeTo) throws Exception {
        int jdbcPort = Integer.valueOf(options.get(OPTION_JDBC_PORT));
        Connection fdbc = ConnectionFactory.getConnection(options.get(OPTION_FDBC_DATABASE));
        java.sql.Connection jdbc = JDBCConnector.getConnection(options.get(OPTION_JDBC_DRIVER),options.get(OPTION_JDBC_HOST),
                jdbcPort,options.get(OPTION_JDBC_DATABASE),options.get(OPTION_JDBC_USER),options.get(OPTION_JDBC_PASSWORD));
        final Transfer transfer = new Transfer(new PostgresDML(),fdbc,jdbc);

        TRANSPORT_POOL.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println(String.format("Transferring invocations data. Time range %s - %s",new Date(timeFrom),new Date(timeTo)));
                transfer.transferInvocationTrees(timeFrom,timeTo);
            }
        });
        TRANSPORT_POOL.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println(String.format("Transferring KV data. Time range %s - %s",new Date(timeFrom),new Date(timeTo)));
                transfer.transferKV(timeFrom,timeTo);
            }
        });
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    private static void cyclic(Map<String,String> options,final long timeFrom) throws Exception {
        int jdbcPort = Integer.valueOf(options.get(OPTION_JDBC_PORT));
        Connection fdbc = ConnectionFactory.getConnection(options.get(OPTION_FDBC_DATABASE));
        java.sql.Connection jdbc = JDBCConnector.getConnection(options.get(OPTION_JDBC_DRIVER),options.get(OPTION_JDBC_HOST),
                jdbcPort,options.get(OPTION_JDBC_DATABASE),options.get(OPTION_JDBC_USER),options.get(OPTION_JDBC_PASSWORD));
        final Transfer transfer = new Transfer(new PostgresDML(),fdbc,jdbc);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
        final AtomicLong tf1 = new AtomicLong(timeFrom);
        final AtomicLong tf2 = new AtomicLong(timeFrom);

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                Logger.getLogger("RDBMS").info(String.format("Transferring invocations data from %s",new Date(tf1.get())));
                transfer.transferInvocationTrees(tf1.get(), now);
                Logger.getLogger("RDBMS").info("Transferring invocations data cycle ended.");
                tf1.set(now);
            }
        },0,5, TimeUnit.SECONDS);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                Logger.getLogger("RDBMS").info(String.format("Transferring kv data from %s",new Date(tf2.get())));
                long s = transfer.transferKV(tf2.get(),now);
                Logger.getLogger("RDBMS").info(String.format("Transferred %d kv entries.",s));
                tf2.set(now);
            }
        },0,5, TimeUnit.SECONDS);
    }



    private static void usage(){
        System.out.println("Usage:");
        System.out.println("  rdbms <mode> <rdbms.properties> [<time-from>] [<time-to>]");
        System.out.println("    <mode> = one|cyclic|all");
        System.out.println("    <time-*> = "+dateTimeFormat.toPattern());
    }



    private static String extractLocal(String archPath){
        try {
            File extracted = ConnectionFactory.extractDatabaseArchive(archPath);
            return extracted.getAbsolutePath();
        } catch (FDBCException e) {
            e.printStackTrace();
            return null;
        }
    }




    public static void main(String[] args) throws Exception {

        if (args.length < 2){
            usage();
            System.exit(1);
        }
        String m = args[0];
        MODE mode = null;
        for (MODE mm : MODE.values()){
            if (mm.n.equals(m)){
                mode = mm; break;
            }
        }
        if (mode == null){
            System.err.println("Invalid mode value.");
            usage();
            System.exit(1);
        }

        Properties properties = readSettings(args[1]);
        if (properties == null){
            System.exit(1);
        }
        Map<String,String> options =new HashMap<String, String>();
        options.put(OPTION_FDBC_DATABASE,properties.getProperty(OPTION_FDBC_DATABASE));
        options.put(OPTION_JDBC_HOST,properties.getProperty(OPTION_JDBC_HOST));
        options.put(OPTION_JDBC_PORT,properties.getProperty(OPTION_JDBC_PORT));
        options.put(OPTION_JDBC_DATABASE,properties.getProperty(OPTION_JDBC_DATABASE));
        options.put(OPTION_JDBC_USER,properties.getProperty(OPTION_JDBC_USER));
        options.put(OPTION_JDBC_PASSWORD,properties.getProperty(OPTION_JDBC_PASSWORD));
        options.put(OPTION_JDBC_DRIVER,properties.getProperty(OPTION_JDBC_DRIVER));

        final long timeFrom = (args.length > 2) ? readTime(args[2]) : 0;
        final long timeTo = (args.length > 3) ? readTime(args[3]) : System.currentTimeMillis();
        if (timeFrom > timeTo || timeTo < 0) {
            System.err.println("Invalid time range.");
            System.exit(1);
        }

        String jdbcDriver = options.get(OPTION_JDBC_DRIVER);
        String driver = null;
        for (String drv : JDBCConnector.SUPPORTED_JDBC_DRIVERS){
            if (drv.equals(jdbcDriver)){
                driver = drv;
                break;
            }
        }
        if (driver == null){
            throw new Exception("Unsupported JDBC driver.");
        }

        switch (mode){
            case ALL:{
                String arch = options.get(OPTION_FDBC_DATABASE);
                if (arch == null){
                    System.err.println("Flopsar archive file required!");
                    System.exit(1);
                }
                String outdir = extractLocal(arch);
                options.put(OPTION_FDBC_DATABASE,outdir);
                oneShot(options,0,0);
            } break;
            case ONE:
                oneShot(options,timeFrom,timeTo);break;
            case CYCLIC:
                cyclic(options,timeFrom);break;
        }
    }










}
