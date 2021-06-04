# -*- coding: utf-8 -*-
"""
Created on Fri Oct 30 16:19:59 2020

@author: Evgeni.Vargin
"""


import cx_Oracle as db

class OraConnection(db.connect):
    def __init__(self,inUsername,inPassword,inSID):
        try:
            self.conn = db.connect(inUsername,inPassword,inSID)
            self.cur = self.conn.cursor()
        except(db.DatabaseError,Error):
            print(Error)
            exit()
    
    def exec(self,inSQL):
        self.cur.execute(inSQL)
    
    def get_cursor(self,inSQL):
        ret = self.cur.var(db.CURSOR)
        self.cur.execute("""begin GetAnyCursor('%s', :ret); end; """%(inSQL),ret=ret)
        return ret
 
    def get_fields(self,cur):
        return cur.getvalue().description

    def get_data(self,cur):
        return cur.getvalue().fetchall()
        
    def close(self):
        self.conn.close()
      
