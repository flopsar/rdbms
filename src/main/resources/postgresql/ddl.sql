
-- PostgreSQL 10 Schema


CREATE SEQUENCE paramseq START 1;
CREATE SEQUENCE invseq START 1;


CREATE TABLE AgentType (
    id INTEGER PRIMARY KEY,
    atype varchar(10) NOT NULL UNIQUE
);

CREATE TABLE Agent (
  id SERIAL PRIMARY KEY,
  atype INTEGER NOT NULL,
  name varchar(128) NOT NULL,
  CONSTRAINT FK_AgentType FOREIGN KEY (atype) REFERENCES AgentType(id)
);


CREATE TABLE SymbolType (
    id INTEGER PRIMARY KEY,
    stype varchar(10) NOT NULL UNIQUE
);


CREATE TABLE Symbol (
    id SERIAL PRIMARY KEY,
    stype INTEGER NOT NULL,
    name varchar(1024) NOT NULL,
    description varchar(1024),
    CONSTRAINT FK_SymbolType FOREIGN KEY (stype) REFERENCES SymbolType(id)
);


CREATE TABLE KV (
  symbol INTEGER,
  tstamp TIMESTAMP WITH TIME ZONE,
  val BIGINT,
  agent INTEGER NOT NULL,
  CONSTRAINT FK_Symbol FOREIGN KEY (symbol) REFERENCES Symbol(id),
  CONSTRAINT FK_Agent FOREIGN KEY (agent) REFERENCES Agent(id)
);


CREATE TABLE Parameter (
  id BIGINT PRIMARY KEY,
  keyname VARCHAR(256),
  val TEXT
);





CREATE TABLE RootInfo (
  id BIGINT PRIMARY KEY,
  threadname INTEGER,
  totalcalls BIGINT,
  registeredcalls INTEGER,
  CONSTRAINT FK_SymbolThread FOREIGN KEY (threadname) REFERENCES Symbol(id)
);



CREATE TABLE Invocation (
  id BIGINT PRIMARY KEY,
  agent INTEGER,
  root BIGINT,
  rootinfo BIGINT,
  classname INTEGER,
  methodname INTEGER,
  duration BIGINT,
  cputime BIGINT,
  callorder INTEGER,
  stackdepth INTEGER,
  tstamp TIMESTAMP WITH TIME ZONE,
  exception TEXT,
  CONSTRAINT FK_SymbolClass FOREIGN KEY (classname) REFERENCES Symbol(id),
  CONSTRAINT FK_SymbolMethod FOREIGN KEY (methodname) REFERENCES Symbol(id),
  CONSTRAINT FK_Root FOREIGN KEY (root) REFERENCES Invocation(id),
  CONSTRAINT FK_Agent FOREIGN KEY (agent) REFERENCES Agent(id),
  CONSTRAINT FK_RootInfo FOREIGN KEY (rootinfo) REFERENCES RootInfo(id)
);




CREATE TABLE InvPar (
  invocation BIGINT NOT NULL,
  parameter BIGINT NOT NULL,
  CONSTRAINT FK_Invocation FOREIGN KEY (invocation) REFERENCES Invocation(id),
  CONSTRAINT FK_Parameter FOREIGN KEY (parameter) REFERENCES Parameter(id)
);




CREATE INDEX symbol_name ON Symbol (name);
CREATE UNIQUE INDEX symbol_idx ON Symbol(stype,name,description);
CREATE UNIQUE INDEX agent_idx ON Agent(atype,name);


