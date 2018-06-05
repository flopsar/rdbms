
-- list all symbols
SELECT SymbolType.stype,Symbol.name,Symbol.description
FROM Symbol INNER JOIN SymbolType ON Symbol.stype = SymbolType.id
ORDER BY SymbolType.stype;

--list all agents
SELECT AgentType.atype,Agent.name
FROM Agent INNER JOIN AgentType ON Agent.atype = AgentType.id
ORDER BY Agent.name;

--list all root invocations
SELECT t.name,c.name,m.name,m.description,i.tstamp,i.duration,ri.registeredcalls,ri.totalcalls,i.exception
FROM Invocation i,Symbol c,Symbol m,Symbol t,RootInfo ri
WHERE i.rootinfo = ri.id
AND c.id = i.classname
AND m.id = i.methodname
AND t.id = ri.threadname;


-- list all parameters for a specific Invocartion (id=862)
SELECT i.id,p.keyname,p.val
FROM InvPar pi,Invocation i,Parameter p
WHERE pi.invocation = i.id
AND p.id = pi.parameter
AND i.id = 862;


-- count invocations whose parameter values contain 'phrase'
SELECT COUNT(DISTINCT sub.id) FROM (
SELECT i.id
FROM InvPar pi,Invocation i,Parameter p
WHERE pi.invocation = i.id
AND p.id = pi.parameter
AND p.val LIKE '%phrase%'
ORDER BY i.id) sub;




