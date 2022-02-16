import psycopg2
import logging
import os

class ParmValidator():
    
    def getTestQuery(self, testType, testParmNames, testParmValues):
        query = ""
        match_query = ""
        
        # advice for the future
        # query_parts = list()
        # if colVal is None:
        #     query_parts.append(f"{colName} is null")
        # else:
        #     query_parts.append(f"{colName} = '{colVal}'")
        # query = " ".join(query_parts)

        # instead of index
        # params = dict(zip(testParmNames, testParmValues))
        # query = params['query']
        
        if (testType == "custom_query"):
            query = "select * from (" + testParmValues[testParmNames.index('query')] + ")"
        else:
            query = "select " 
            #columnn_names
            if (testType == "allowed_increment"):
                query = query + "count(*)"
            else:
                query = query + testParmValues[testParmNames.index('column_names')]
                
            if (testType == "window_match"):
                query = query + "," + testParmValues[testParmNames.index('date_column')]
            
            #schema.table
            query = query + " from " + testParmValues[testParmNames.index('schema_name')] \
                  + "." + testParmValues[testParmNames.index('table_name')]
                        
            #subset_condition
            subset_codition = testParmValues[testParmNames.index('subset_condition')]
            if (subset_codition not in (None,'')):
                query = query + " where (" + subset_codition + ")"
                
            if (testType == "condition_check"):
                if (subset_codition not in (None,'')):
                    query = query + " and "
                else:
                    query = query + " where "
                query = query + "(" + testParmValues[testParmNames.index('condition')] + ")"
            elif (testType == "aggragate_match"):
                query = query + " group by " + testParmValues[testParmNames.index('groupby_nammes')]
                having_condition = testParmValues[testParmNames.index('having_condition')]
                if (having_codition not in (None,'')):
                    query = query + " having " + having_condition
            
            #match    
            if (testType in ("aggregate_match","data_match","prior_match")):
                match_query = "select "
                if (testType == "prior_match"):
                    match_query = match_query + testParmValues[testParmNames.index('column_names')] \
                                + " from " + testParmValues[testParmNames.index('schema_prior')] \
                                + "." + testParmValues[testParmNames.index('table_name')]
                    match_subset_codition = testParmValues[testParmNames.index('subset_condition')]
                    if (subset_codition not in (None,'')):
                        match_query = match_query + " where " + match_subset_codition
                else:
                    match_query = match_query + testParmValues[testParmNames.index('match_column_names')] \
                                + " from " + testParmValues[testParmNames.index('match_schema_name')] \
                                + "." + testParmValues[testParmNames.index('match_table_name')]
                    match_subset_condition = testParmValues[testParmNames.index('match_subset_condition')]
                    if (match_subset_condition not in (None,'')):
                        match_query = match_query + " where " + match_subset_condition
                    if (testType == "aggregate_match"):
                        match_query = match_query + " group by " \
                        + testParmValues[testParmNames.index('match_groupby_names')]
                        match_having_condition = testParmValues[testParmNames.index('match_having_condition')]
                        if (match_having_condition not in (None,'')):
                            match_query = match_query + " having " + match_having_condition
                #combine
                query = query + " limit 1; " + match_query
        
        query = query + " limit 1"
        #query = query.replace("\r","").replace("\n"," ")
        self.logger.info('%s, query = %s', testType, query.replace("\r","").replace("\n"," "))
        return query
    
    def updateTest(self, testType, testParmNames, testParmValues, resultCode, resultMessage):
        query = "update " + str(self.qc_schema) + "." + str(testType) + "_test_set " \
              + "set test_disable = " + str(resultCode)
        if (resultMessage not in (None,'')):
            query = query + ", check_result = trim('" + str(resultMessage).replace("'","\\'") + "')"
        else:
            query = query + ", check_result=NULL"
        query = query + " where "
        i = 1
        for colName in testParmNames:
            colVal = testParmValues[testParmNames.index(colName)]
            if colVal is None:
                query = query + colName + " is null "
            else:
                query = query + colName + " = '" \
                      + str(colVal).replace("'","\\'") + "'"
            if (i != len(testParmNames)):
                query = query + " and "
            i = i + 1
        #query = query.replace("\r","").replace("\n"," ")
        #self.logger.info('update query = %s', query)
        cursor = self.conn.cursor()                    
        cursor.execute(query)  
        cursor.close()
        self.conn.commit()
    
    def executeTest(self, testType, testParmNames, testParmValues):
        test_disable = testParmValues[testParmNames.index('test_disable')]
        check_result = testParmValues[testParmNames.index('check_result')]
        query = self.getTestQuery(testType, testParmNames, testParmValues)
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            if (test_disable == -1 or check_result not in (None,'')):
                self.updateTest(testType, testParmNames, testParmValues, 0, None)
        except Exception as err: 
            self.conn.rollback()
            errmsg = "[" + err.pgcode + "] " + err.pgerror.replace("\n"," ").replace("^","")
            self.logger.error("%s %s: %s", testType, testParmValues, errmsg)
            self.updateTest(testType, testParmNames, testParmValues, -1, errmsg)
            pass
        cursor.close()

    def getTestParameters(self, testType):
        cursor = self.conn.cursor()
        query = "select * from " + self.qc_schema + "." + testType + "_test_set " \
              + "where lower(test_group) = lower('" \
              +  self.qc_group + "') and nvl(test_disable,0) <> 1;"
        #self.logger.info('query = %s', query)
        
        cursor.execute(query)
        columnNames = [desc[0] for desc in cursor.description]
        for row in cursor:
            self.executeTest(testType, columnNames, row)
        cursor.close()
    
    def runTests(self):
        self.conn = self.db_connection()
        for testType in ["primary_key", "not_null", "uniqueness",
                         "allowed_increment", "window_match", "value_match",
                         "data_match", "prior_match", "aggregate_match",
                         "condition_check", "custom_query"
                        ]:
            self.getTestParameters(testType)
        self.conn.close()

    def db_connection(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database)
        return conn
    
    def __init__(self):
        self.logger = logging.getLogger()
        
        for required_key in ["db_host", "db_username", "db_password", "db_db", "db_port",
                              "qc_group", "qc_schema"]:
            if required_key not in os.environ.keys():
                raise Exception('missing key %s in config.json', required_key)
        #QC parms
        self.qc_group = os.environ['qc_group']
        self.qc_schema = os.environ['qc_schema']
        
        #DB parms
        self.host = os.environ['db_host']
        self.username = os.environ['db_username']
        self.password = os.environ['db_password']
        self.database = os.environ['db_db']
        self.port = os.environ['db_port']

def main():
    ParmValidator().runTests()

if __name__ == "__main__":
    main()
