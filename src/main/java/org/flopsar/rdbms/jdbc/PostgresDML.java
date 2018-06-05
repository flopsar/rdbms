package org.flopsar.rdbms.jdbc;

import com.flopsar.api.AgentId;
import com.flopsar.core.protocol.Protocol;
import com.flopsar.fdbc.api.*;
import org.flopsar.rdbms.fdbc.SimpleSymbolsCache;

import java.sql.Connection;
import java.sql.*;
import java.util.List;
import java.util.Map;


public class PostgresDML implements DML {

    private static final String SQL_SEQ_PARAM = "SELECT nextval('paramseq');";
    private static final String SQL_SEQ_INV = "SELECT nextval('invseq');";
    private static final String SQL_INSERT_INVOCATION = "INSERT INTO Invocation (id,agent,root,rootinfo,classname,methodname,duration,cputime,callorder,stackdepth,tstamp,exception) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String SQL_INSERT_PARAMETER = "INSERT INTO Parameter (id,keyname,val) VALUES (?,?,?)";
    private static final String SQL_INSERT_INVPAR_REL = "INSERT INTO InvPar (invocation,parameter) VALUES (?,?)";
    private static final String SQL_SELECT_SYMBOL = "SELECT id FROM Symbol WHERE stype = ? AND name = ? AND description = ?";
    private static final String SQL_INSERT_SYMBOL = "INSERT INTO Symbol (stype,name,description) VALUES (?,?,?)";
    private static final String SQL_SELECT_AGENT = "SELECT id FROM Agent WHERE atype = ? AND name = ?";
    private static final String SQL_INSERT_AGENT = "INSERT INTO Agent (atype,name) VALUES (?,?)";
    private static final String SQL_INSERT_KV = "INSERT INTO KV (symbol,tstamp,val,agent) VALUES (?,?,?,?)";


    private final SimpleSymbolsCache cache;


    public PostgresDML(SimpleSymbolsCache cache){
        this.cache = cache;
    }

    public PostgresDML(){
        this.cache = new SimpleSymbolsCache();
    }








    private int findSymbol(Symbol symbol, SimpleSymbolsCache cache, java.sql.Connection jdbc){
        int symbolId = cache.getId(symbol.getRawName(),symbol.getType());
        if (symbolId < 0){
            try {
                if ((symbolId = insertSymbol(symbol,jdbc)) < 0)
                    return -1;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            cache.put(symbol.getRawName(),symbol.getType(),symbolId);
        }
        return symbolId;
    }






    @Override
    public int insertAgent(AgentId agent, Connection jdbc) throws SQLException {
        PreparedStatement select = null,insert = null;
        try {
            select = jdbc.prepareStatement(SQL_SELECT_AGENT);
            select.setInt(1,agent.getType().getV());
            select.setString(2,agent.getName());
            ResultSet selectResult = select.executeQuery();
            if (selectResult.next()){
                return selectResult.getInt(1);
            }

            insert = jdbc.prepareStatement(SQL_INSERT_AGENT, Statement.RETURN_GENERATED_KEYS);
            insert.setInt(1,agent.getType().getV());
            insert.setString(2,agent.getName());
            if (0 == insert.executeUpdate()){
                return -1;
            }
            jdbc.commit();
            ResultSet ids = insert.getGeneratedKeys();
            if (ids.next()){
                return ids.getInt(1);
            }
        } finally {
            if (select != null)
                select.close();
            if (insert != null)
                insert.close();
        }
        return -1;
    }






    @Override
    public int insertSymbol(Symbol symbol, Connection jdbc) throws SQLException {
        PreparedStatement select = null,insert = null;
        try {
            select = jdbc.prepareStatement(SQL_SELECT_SYMBOL);
            select.setInt(1,symbol.getType().getId());
            select.setString(2,symbol.getType() == SymbolType.SYMBOL_KV
                    ? symbol.getName().replace(Protocol.SYMBOL_SEPARATOR,'/') : symbol.getName());
            if (symbol.getType() == SymbolType.SYMBOL_METHOD)
                select.setString(3,symbol.getDescription());
            else
                select.setNull(3,Types.VARCHAR);
            ResultSet selectResult = select.executeQuery();
            if (selectResult.next()){
                return selectResult.getInt(1);
            }

            insert = jdbc.prepareStatement(SQL_INSERT_SYMBOL, Statement.RETURN_GENERATED_KEYS);
            insert.setInt(1,symbol.getType().getId());
            insert.setString(2,symbol.getType() == SymbolType.SYMBOL_KV
                    ? symbol.getName().replace(Protocol.SYMBOL_SEPARATOR,'/') : symbol.getName());
            if (symbol.getType() == SymbolType.SYMBOL_METHOD)
                insert.setString(3,symbol.getDescription());
            else
                insert.setNull(3,Types.VARCHAR);
            if (0 == insert.executeUpdate()){
                return -1;
            }
            jdbc.commit();
            ResultSet ids = insert.getGeneratedKeys();
            if (ids.next()){
                return ids.getInt(1);
            }
        } finally {
            if (select != null)
                select.close();
            if (insert != null)
                insert.close();
        }
        return -1;
    }






    @Override
    public long insertInvocationTree(AgentId agent, MethodInvocationTree tree, Connection jdbc) throws SQLException {
        MethodInvocationTreeItem root = tree.getRoot();
        final int threadId = findSymbol(root.getThreadName(),cache,jdbc);
        final int classId = findSymbol(root.getClassSymbol(),cache,jdbc);
        final int methodId = findSymbol(root.getMethodSymbol(),cache,jdbc);
        final int agentId = insertAgent(agent,jdbc);
        long rid = -1;

        try {
            rid = insertRootInvocation(jdbc,agentId,threadId,classId,methodId,root.getDuration(),
                    root.getCPUTime(),root.getTimeStamp(),tree.getTotalInvocationsCount(),
                    (int)tree.getRegisteredInvocationsCount(),root.getExceptionStackTrace(),root.getParameters());
            if (root.getChildren() != null)
                for (MethodInvocationTreeItem i : root.getChildren())
                    transferTreeItems(agentId,i,cache,jdbc,rid);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rid;
    }













    private void transferTreeItems(int agentId, MethodInvocationTreeItem item, SimpleSymbolsCache cache, java.sql.Connection jdbc, long rootId) throws SQLException {
        final int classId = findSymbol(item.getClassSymbol(),cache,jdbc);
        final int methodId = findSymbol(item.getMethodSymbol(),cache,jdbc);

        try {
            insertInvocation(jdbc,agentId,rootId,0,classId,methodId,item.getDuration(),item.getCPUTime(),
                    item.getTimeStamp(),item.getExceptionStackTrace(),item.getParameters(),item.getCallOrder(),item.getStackDepth());
            if (item.getChildren() != null)
                for (MethodInvocationTreeItem i : item.getChildren())
                    transferTreeItems(agentId,i,cache,jdbc,rootId);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }




    @Override
    public void insertKV(AgentId agent, Symbol symbol, List<MetricValue> values, Connection jdbc) throws SQLException {
        final int agentId = insertAgent(agent,jdbc);
        final int symbolId = findSymbol(symbol,cache,jdbc);
        final PreparedStatement insert = jdbc.prepareStatement(SQL_INSERT_KV);
        for (MetricValue mv : values){
            insert.setInt(1,symbolId);
            insert.setTimestamp(2,new Timestamp(mv.getTimeStamp()));
            insert.setLong(3,mv.getValue());
            insert.setInt(4,agentId);
            insert.addBatch();
        }
        insert.executeBatch();
        jdbc.commit();
        insert.close();
    }







    private long getNextId(Connection jdbc,String query) throws SQLException {
        Statement seq = jdbc.createStatement();
        ResultSet rs = seq.executeQuery(query);
        if (rs.next())
            return rs.getLong(1);
        return -1;
    }






    private long insertInvocation(Connection jdbc,int agentId, long rootId, long rinfoId,int clazz,
                                 int method,long duration,long cputime,long tstamp, String exception,
                                 Map<String,String> params,int callorder,int stackdepth) throws SQLException {

        long[] paramsId = params != null ? new long[params.size()] : null;
        if (params != null){
            PreparedStatement insertParams = jdbc.prepareStatement(SQL_INSERT_PARAMETER);
            int idx = 0;
            for (String k: params.keySet()){
                paramsId[idx] = getNextId(jdbc, SQL_SEQ_PARAM);
                insertParams.setLong(1,paramsId[idx++]);
                insertParams.setString(2,k);
                insertParams.setString(3,params.get(k).replace((char)0,'\n'));
                insertParams.addBatch();
            }
            insertParams.executeBatch();
            insertParams.close();
        }

        final long invId = getNextId(jdbc, SQL_SEQ_INV);
        PreparedStatement insert = jdbc.prepareStatement(SQL_INSERT_INVOCATION);

        try {
            insert.setLong(1,invId);
            insert.setInt(2,agentId);
            insert.setLong(3,rootId < 0 ? invId : rootId);
            if (rinfoId > 0)
                insert.setLong(4,rinfoId);
            else
                insert.setNull(4,Types.BIGINT);
            insert.setInt(5,clazz);
            insert.setInt(6,method);
            insert.setLong(7,duration);
            insert.setLong(8,cputime);
            insert.setInt(9,callorder);
            insert.setInt(10,stackdepth);
            insert.setTimestamp(11,new Timestamp(tstamp));
            insert.setString(12,exception != null ? exception : "NA");
            insert.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (paramsId != null) {
            PreparedStatement insertParamRel = jdbc.prepareStatement(SQL_INSERT_INVPAR_REL);
            for (long id : paramsId){
                insertParamRel.setLong(1,invId);
                insertParamRel.setLong(2,id);
                insertParamRel.addBatch();
            }
            insertParamRel.executeBatch();
        }
        return invId;
    }




    private long insertRootInvocation(Connection jdbc, int agentId, int thread, int clazz,
                                            int method, long duration, long cputime, long tstamp,
                                            long totalcalls, int registeredcalls, String exception,
                                            Map<String,String> params) throws SQLException {
        jdbc.setAutoCommit(false);
        final long rinfoId = getNextId(jdbc, SQL_SEQ_INV);
        PreparedStatement rinfo = jdbc.prepareStatement("INSERT INTO RootInfo (id,threadname,totalcalls,registeredcalls) VALUES (?,?,?,?)");
        rinfo.setLong(1,rinfoId);
        rinfo.setInt(2,thread);
        rinfo.setLong(3,totalcalls);
        rinfo.setInt(4,registeredcalls);
        rinfo.executeUpdate();
        rinfo.close();

        long id = insertInvocation(jdbc,agentId,-1,rinfoId,clazz,method,duration,cputime,tstamp,exception,params,0,0);
        jdbc.commit();
        return id;
    }



}
