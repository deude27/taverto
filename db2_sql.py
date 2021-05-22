import json
import re
import os
import sys
import string
from tqdm import tqdm
from collections import defaultdict
from taverto.modules.sqlparser.grammar.db2_sql_dtls import SQL_STMNT, CCNUM, SSNNO, SET_PROFILE_REGEX
from taverto.modules.sqlparser.grammar.db2_query_transform import transformSqlStmnt
from taverto.modules.sqlparser.grammar.db2_jcl_process import JCL_STMT


class Db2Sql:
    """
    Class to access DB2 queries or procedure script and parse them to
    collect inventory and to transform them to presto sql format
    """

    def __init__(self, name=None, dbName=None, sql=None, type=None, **kwargs):
        self.name = name
        self.dbName = dbName
        self._sql = sql.strip()
        self.type = type.lower()
        self.sqlStmnts = list()
        # list queries and procs used in the sql stmnts
        self.objectsUsed = set()
        self.queries = set()
        self.procs = set()
        self.forms = set()
        self.sourceTables = set()
        self.targetTables = set()
        self.unknownObjects = set()
        self.fileExport = False
        self.saveAsTable = False
        self.jcl = None
        self.prestoSql = list()
        self.override = False
        for k, v in kwargs.items():
            setattr(self, k, v)
        # self.parseProcStmnts()

    @classmethod
    def fromSqlText(cls, name="", sql=None, type=None, **kwargs):
        object = cls(name=name, sql=sql, type=type, **kwargs)
        return object

    @classmethod
    def fromSqlObjectSummary(cls, summary: dict):
        object = cls(**summary)
        object.sql = object._sql
        return object

    def parseSqlStmnts(self):
        self.maskSpi()
        self._results = SQL_STMNT.searchString(self._sql)
        for r in self._results:
            if r.objectName:
                objectName = self.getobjectName(r.objectName)
            if r.action in ["RUN", "DI"]:
                """
                If RUN stmnt is for SET????_Q query which sets client profile
                then get the client id and change the dbName to TA{clientId}
                and use that moving forward for all querys/procs/forms which are
                used without fully qualified name
                """
                setProfileMatch = re.match(SET_PROFILE_REGEX, objectName)
                if setProfileMatch:
                    clientId = setProfileMatch.group("clientId")
                    self.dbName = f"TA{clientId}"
                sqlStmnt = {"objectName": objectName}
                if r.formName != "":
                    sqlStmnt["formName"] = self.getobjectName(r.formName)
                    self.forms.add(sqlStmnt["formName"])
                self.objectsUsed.add(objectName)
                self.sqlStmnts.append(sqlStmnt)
            if r.action == "SAVE":
                """
                if action issave, then store the object name as targetTable
                for the last executed query object and set saveAsTable to True
                """
                self.targetTables.add(objectName)
                self.sqlStmnts[-1]["targetTable"] = objectName
                self.saveAsTable = True
            if r.action in ["PRINT", "EXPORT"]:
                """
                if action is PRINT/EXPORT set fileExport to True for the last excuted
                query object and add export file details if available
                """
                self.sqlStmnts[-1]["fileOutput"] = r.asDict()
                self.fileExport = True

            if r.action in ["FROM", "JOIN"]:
                """
                if action is from  or join add the object name to sourceTables
                """
                self.sourceTables.add(objectName)

    def maskSpi(self):
        """
        Method to mask any Credit Card Number or SSN Number stored in
        query text.
        """
        self.sql = (CCNUM | SSNNO).transformString(self._sql)

    def getobjectName(self, objectName):
        """
        Method to return fully qualified object name used in query/procs.
        If object name doesnt have db name, then add current active db name
        """
        # return f"{self.dbName}.{objectName}" if objectName.find(".") <= 0 else objectName
        return f"{self.dbName}.{objectName.split('.')[-1]}"
        # objectName=objectName.split('.')
        # if len(objectName) == 1:
        #     objectName.insert(0,self.dbName)
        # return ".".join(objectName)

    def summary(self, ignoreFields=[]):
        """
        return the summary of sql/proc as dict
        """
        return {
            k: list(v) if type(v) == set else v
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in ignoreFields
        }

    def convertQuery(self):
        """
        Convert the db2 sql query into presto format using pyparsing grammar
        """
        if not getattr(self, "convertedSql", None):
            self.convertedSql = transformSqlStmnt(self.sql)


queryNameRegex = r"^QUERY\s*:\s*(?P<objectName>\w+\.\w+)$"
lastUsedRegex = r"^LAST USE DATE\s+(?P<lastUsedDate>\d{4}-\d{2}-\d{2}$)"


def readDb2ObjectTextFile(schema, objectType, file):
    """
    Read DB2 Object File created out of mainframe which is in below format.

    QUERY : QUERY_NAME
    LAST USED DATE : last used date
    QUERY_START
    <query stmnt>
    QUERY_END
    After extracting the sql text, created Db2Sql instance for each object and saves
    in a dictionary with object Name as key.
    """
    objectName = None
    captureStmnt = None
    lastUsedDate = None
    sql = []
    objects = {}
    with open(file, "r") as f:
        for line in tqdm(f.readlines(), f"Reading DB2 extract {file}"):
            line = line.strip()
            if not captureStmnt:
                m = re.match(queryNameRegex, line)
                if m:
                    objectName = m.group("objectName")
                    continue
                m = re.match(lastUsedRegex, line)
                if m:
                    lastUsedDate = m.group("lastUsedDate")
            if line.strip() == "QUERY_START":
                captureStmnt = True
                continue
            elif line == "QUERY_END":
                if objectName:
                    name = objectName.split(".")[-1]
                    dbName = objectName.split(".")[0]
                    queryDtls = dict(
                        name=name, dbName=dbName, lastUsedDate=lastUsedDate, sql="\n".join(sql), type=objectType
                    )
                    p: Db2Sql = Db2Sql.fromSqlText(**queryDtls)       
                    p.parseSqlStmnts() 
                    objects[objectName] = p
                captureStmnt = False
                objectName = None
                lastUsedDate = None
                sql = []
                continue
            if captureStmnt and objectName:
                sql.append(line)
    return objects


class Db2Jcl:
    """
    Class to access DB2 JCL script and parse them to
    collect Jobname, procname,output dataset name
    """

    def __init__(self, name=None, jcl=None, type=None, **kwargs):
        self.name = name
        self._jcl = jcl.strip()
        self.type = type.lower()
        # list queries and procs used in the sql stmnts
        self.procs = set()
        self.dsqDsnNames = set()
        self.dsqPrint = False
        self.jcl = None
        for k, v in kwargs.items():
            setattr(self, k, v)
        # self.parseProcStmnts()

    @classmethod
    def fromJclText(cls, name="", jcl=None, type=None, **kwargs):
        object = cls(name=name, jcl=jcl, type=type, **kwargs)
        return object

    @classmethod
    def fromJclObjectSummary(cls, summary: dict):
        object = cls(**summary)
        object.jcl = object._jcl
        return object

    def parseJclStmnts(self):
        self._results = JCL_STMT.searchString(self._jcl)
        for r in self._results:
            if r.jclname:
                objectName = self.getobjectName(r.jclname)
            if r.procname:
                setProfileMatch = re.match(SET_PROFILE_REGEX, objectName)
                if setProfileMatch:
                    clientId = setProfileMatch.group("clientId")
                    self.dbName = f"TA{clientId}"
                sqlStmnt = {"objectName": objectName}

            if r.dsqdsnname:
                """
                if action issave, then store the object name as targetTable
                for the last executed query object and set saveAsTable to True
                """
                self.targetTables.add(objectName)
                self.sqlStmnts[-1]["targetTable"] = objectName
                self.saveAsTable = True


    def getobjectName(self, objectName):
        """
        Method to return fully qualified object name used in query/procs.
        If object name doesnt have db name, then add current active db name
        """
        # return f"{self.dbName}.{objectName}" if objectName.find(".") <= 0 else objectName
        return f"{self.dbName}.{objectName.split('.')[-1]}"
        # objectName=objectName.split('.')
        # if len(objectName) == 1:
        #     objectName.insert(0,self.dbName)
        # return ".".join(objectName)

    def summary(self, ignoreFields=[]):
        """
        return the summary of sql/proc as dict
        """
        return {
            k: list(v) if type(v) == set else v
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in ignoreFields
        }

    def convertQuery(self):
        """
        Convert the db2 sql query into presto format using pyparsing grammar
        """
        if not getattr(self, "convertedSql", None):
            self.convertedSql = transformSqlStmnt(self.sql)

jobNameRegex = r"^//\s{1-8}*:\s*(?P<objectName>\w+\.\w+)$"
procNameRegex = r"^PARM(M=B,I+"

def readDb2ObjectJclTextFile(job_name, objectType, file):
    """
    Read DB2 Object File created out of mainframe which is in below format.

    QUERY : QUERY_NAME
    LAST USED DATE : last used date
    QUERY_START
    <query stmnt>
    QUERY_END
    After extracting the sql text, created Db2Sql instance for each object and saves
    in a dictionary with object Name as key.
    """
    jobName = None
    captureStmnt = None
    jcl = []
    procName = []
    dsnName = []
    objects = {}
    with open(file, "r") as f:
        for line in tqdm(f.readlines(), f"Reading DB2 extract {file}"):
            line = line.strip()
            comment = line[0,2]
            while comment != "//*":
                if line.strip() == "JCL_START":
                    continue
                if not captureStmnt:
                    m = re.match(jobNameRegex, line)
                    if m:
                        jobName = m.group("jobName")
                        captureStmnt = True
                        continue   
                if captureStmnt: 
                    if line[0:10] == "PARM(M=B,I":
                        procName = m.group("procName") 
                    m = re.findall(".DSN",line)
                    if m:
                        dsnName = m.group("dsnName")          
                elif line == "JCL_END":
                    if jobName:
                        name = jobName.split(".")[-1]
                        dbName = procName.split(".")[0]
                        queryDtls = dict(
                            name=name, dbName=dbName, procName = procName, dsnName =dsnName, jcl="\n".join(jcl), type=objectType
                        )
                        p: Db2Jcl = Db2Jcl.fromJclText(**queryDtls)
                        p.parseJclStmnts()
                        objects[objectName] = p
                    captureStmnt = False
                    jobName = None
                    jcl = []
                    continue
                if captureStmnt and jobName:
                    jcl.append(line)
            
    return objects


def readDb2ObjectJson(file):
    objects = {}
    with open(file, "r") as f:
        db2ObjectsSummary = json.load(f)

    for name, summary in tqdm(db2ObjectsSummary.items(), desc=f"Reading summary {file}"):
        objects[name] = Db2Sql.fromSqlObjectSummary(summary=summary)
    return objects


if __name__ == "main":
    proc = [
        "SET PROFILE (CONFIRM=NO",
        "RUN SETSQLID_Q",
        "RUN TA6899.ROD_RUNTEST_P",
        "PRINT REPORT (WIDTH = 133, PRINTER = ' '",
        "RUN QUERY TA6899.ROD_EMOB_ONBOARDING_Q(F=TA6899.ROD_EMOB_ONBOARDING_F",
        "SAVE DATA AS TA6899.FINAL_TEMP",
        "SAVE DATA AS CCRPT1_T2",
        "TSO TABANNER",
        "TARPRINT",
        "EXPORT REPORT TO 'DB2T.TA6899.CLI.TEMP1' --DOWNLOADS REPORT IN EXPORT FORMAT   TSO WQMFFIX DB2T.TA6899.CLI.TEMP1 DB2T.TA6899.CLI.TEMP2",
        "EXPORT REPORT TO DB2T.TA6899.CLI.TEMP2 --DOWNLOADS REPORT IN EXPORT FORMAT   TSO WQMFFIX DB2T.TA6899.CLI.TEMP1 DB2T.TA6899.CLI.TEMP2",
        "ERASE PROC TA6899.QMF_BATCH_T065902 (CONFIRM=NO",
    ]
    procParsed = Db2Sql("TEST_PROC_P", "\n".join(proc), "proc")
    print(procParsed.summary())
    print([r.asDict() for r in procParsed.results])
