package org.flopsar.rdbms.jdbc;

import com.flopsar.api.AgentId;
import com.flopsar.fdbc.api.MethodInvocationTree;
import com.flopsar.fdbc.api.MetricValue;
import com.flopsar.fdbc.api.Symbol;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;


/**
 *
 */
public interface DML {

    /**
     * Inserts an agent data.
     * @param agent agent data to insert.
     * @param jdbc database JDBC connection.
     * @return database primary key of the inserted agent.
     * @throws SQLException
     */
    int insertAgent(AgentId agent, Connection jdbc) throws SQLException;

    /**
     * Inserts a symbol.
     * @param symbol symbol to insert.
     * @param jdbc database JDBC connection.
     * @return database primary key of the inserted symbol.
     * @throws SQLException
     */
    int insertSymbol(Symbol symbol, Connection jdbc) throws SQLException;

    /**
     *
     * @param agent agent identifier.
     * @param tree
     * @param jdbc database JDBC connection.
     * @return
     * @throws SQLException
     */
    long insertInvocationTree(AgentId agent, MethodInvocationTree tree, Connection jdbc) throws SQLException;

    /**
     * Inserts a list of key-value metrics.
     * @param agent agent identifier.
     * @param symbol
     * @param values list of metrics to insert.
     * @param jdbc database JDBC connection.
     * @throws SQLException
     */
    void insertKV(AgentId agent, Symbol symbol, List<MetricValue> values,Connection jdbc) throws SQLException;

}


