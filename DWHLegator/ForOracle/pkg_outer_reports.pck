CREATE OR REPLACE PACKAGE DM_SKB.pkg_outer_reports
  IS
  TYPE recRepLog IS RECORD (log_id NUMBER
                           ,dt DATE
                           ,rep_name VARCHAR2(256)
                           ,rep_descr VARCHAR2(1000)
                           ,status_name VARCHAR2(30)
                           ,apex_user VARCHAR2(500)
                           ,exec_time VARCHAR2(256)
                           ,rep_params VARCHAR2(4000));
  TYPE tabRepLog IS TABLE OF recRepLog;
  TYPE recCA IS RECORD (O1 VARCHAR2(4000)
                       ,O2 VARCHAR2(4000)
                       ,O3 VARCHAR2(4000)
                       ,O4 VARCHAR2(4000)
                       ,O5 VARCHAR2(4000)
                       ,O6 VARCHAR2(4000)
                       ,O7 VARCHAR2(4000)
                       ,O8 VARCHAR2(4000));
  TYPE tabCA IS TABLE OF recCA;
  TYPE recCADetail IS RECORD 
   (reportdate VARCHAR2(4000)
   ,reporttype VARCHAR2(4000)
   ,inn VARCHAR2(4000)
   ,tb VARCHAR2(4000)
   ,full_description VARCHAR2(4000)
   ,gosb VARCHAR2(4000)
   ,vko VARCHAR2(4000)
   ,status VARCHAR2(4000)
   ,branch VARCHAR2(4000)
   ,get_holding VARCHAR2(4000)
   ,holding_id VARCHAR2(4000)
   ,seg VARCHAR2(4000)
   ,accessory VARCHAR2(4000)
   ,priority VARCHAR2(4000)
   ,is_new VARCHAR2(4000)
   ,is_active VARCHAR2(4000)
   ,is_sleeping VARCHAR2(4000)
   ,is_dead VARCHAR2(4000)
   ,is_lost VARCHAR2(4000)
   ,is_asleep VARCHAR2(4000)
   ,is_gone VARCHAR2(4000)
   ,is_activated VARCHAR2(4000)
   ,is_got_lost VARCHAR2(4000)
   ,is_liquid VARCHAR2(4000)
   ,is_comply VARCHAR2(4000)
   ,is_kb VARCHAR2(4000)
   ,get_first_prod VARCHAR2(4000)
   ,get_max_asleep VARCHAR2(4000)
   ,get_min_new VARCHAR2(4000)
   ,get_liq_stat VARCHAR2(4000)
   ,get_pred_base VARCHAR2(4000)
   ,get_comp_stat VARCHAR2(4000));
  TYPE tabCADetail IS TABLE OF recCADetail;
  TYPE recCols10 IS RECORD
   (col_01 VARCHAR2(4000)
   ,col_02 VARCHAR2(4000)
   ,col_03 VARCHAR2(4000)
   ,col_04 VARCHAR2(4000)
   ,col_05 VARCHAR2(4000)
   ,col_06 VARCHAR2(4000)
   ,col_07 VARCHAR2(4000)
   ,col_08 VARCHAR2(4000)
   ,col_09 VARCHAR2(4000)
   ,col_10 VARCHAR2(4000)
   );
  TYPE tabCols10 IS TABLE OF recCols10;
  TYPE recCols20 IS RECORD
   (col_01 VARCHAR2(4000)
   ,col_02 VARCHAR2(4000)
   ,col_03 VARCHAR2(4000)
   ,col_04 VARCHAR2(4000)
   ,col_05 VARCHAR2(4000)
   ,col_06 VARCHAR2(4000)
   ,col_07 VARCHAR2(4000)
   ,col_08 VARCHAR2(4000)
   ,col_09 VARCHAR2(4000)
   ,col_10 VARCHAR2(4000)
   ,col_11 VARCHAR2(4000)
   ,col_12 VARCHAR2(4000)
   ,col_13 VARCHAR2(4000)
   ,col_14 VARCHAR2(4000)
   ,col_15 VARCHAR2(4000)
   ,col_16 VARCHAR2(4000)
   ,col_17 VARCHAR2(4000)
   ,col_18 VARCHAR2(4000)
   ,col_19 VARCHAR2(4000)
   ,col_20 VARCHAR2(4000)
   );
  TYPE tabCols20 IS TABLE OF recCols20;
  TYPE recCols30 IS RECORD
   (col_01 VARCHAR2(4000)
   ,col_02 VARCHAR2(4000)
   ,col_03 VARCHAR2(4000)
   ,col_04 VARCHAR2(4000)
   ,col_05 VARCHAR2(4000)
   ,col_06 VARCHAR2(4000)
   ,col_07 VARCHAR2(4000)
   ,col_08 VARCHAR2(4000)
   ,col_09 VARCHAR2(4000)
   ,col_10 VARCHAR2(4000)
   ,col_11 VARCHAR2(4000)
   ,col_12 VARCHAR2(4000)
   ,col_13 VARCHAR2(4000)
   ,col_14 VARCHAR2(4000)
   ,col_15 VARCHAR2(4000)
   ,col_16 VARCHAR2(4000)
   ,col_17 VARCHAR2(4000)
   ,col_18 VARCHAR2(4000)
   ,col_19 VARCHAR2(4000)
   ,col_20 VARCHAR2(4000)
   ,col_21 VARCHAR2(4000)
   ,col_22 VARCHAR2(4000)
   ,col_23 VARCHAR2(4000)
   ,col_24 VARCHAR2(4000)
   ,col_25 VARCHAR2(4000)
   ,col_26 VARCHAR2(4000)
   ,col_27 VARCHAR2(4000)
   ,col_28 VARCHAR2(4000)
   ,col_29 VARCHAR2(4000)
   ,col_30 VARCHAR2(4000)
   );
  TYPE tabCols30 IS TABLE OF recCols30;
  
  /*********************************************************************************/
  FUNCTION get_ti_as_hms (inInterval IN NUMBER /*интервал в днях*/) RETURN VARCHAR2;
  PROCEDURE pr_log_write(inRepName IN VARCHAR2,inMessage IN VARCHAR2);
  FUNCTION CommaTextFromCursor(inSQL IN CLOB,inSeparator IN VARCHAR2,inUppercase IN VARCHAR2 DEFAULT 'INITCAP') RETURN VARCHAR2;
  PROCEDURE prepare_log_table(outRes OUT VARCHAR2);
  FUNCTION GetReportSQL(inRepName VARCHAR2,inRepParamValues VARCHAR2) RETURN CLOB;
  FUNCTION GetRepLog(inBegDate IN DATE,inEndDate IN DATE) RETURN tabRepLog PIPELINED;
  FUNCTION GetRepCADescr(inSlideID VARCHAR2,inRepDate VARCHAR2,inRepType VARCHAR2,inHolding VARCHAR2,inSegment VARCHAR2,inTB VARCHAR2) RETURN VARCHAR2;
  FUNCTION GetRepCols10(inRepName VARCHAR2,inParams VARCHAR2 DEFAULT NULL,inUser VARCHAR2 DEFAULT NULL) RETURN tabCols10 PIPELINED;
  FUNCTION GetRepCols20(inRepName VARCHAR2,inParams VARCHAR2 DEFAULT NULL,inUser VARCHAR2 DEFAULT NULL) RETURN tabCols20 PIPELINED;
  FUNCTION GetRepCA(inRepName VARCHAR2,inRepDate VARCHAR2,inRepType VARCHAR2,inSgnAlias VARCHAR2,inClIsHolding VARCHAR2,inClSegment VARCHAR2, inTB VARCHAR2 DEFAULT NULL,inGroupColumn VARCHAR2 DEFAULT 'Тербанк', inUser VARCHAR2 DEFAULT NULL) RETURN tabCA PIPELINED;
  FUNCTION GetRepCADetail(inRepName VARCHAR2,inRepDate VARCHAR2,inRepType VARCHAR2,inSgnAlias VARCHAR2,inClIsHolding VARCHAR2,inClSegment VARCHAR2,inTB VARCHAR2 DEFAULT NULL) RETURN tabCADetail PIPELINED;
END pkg_outer_reports;
/
CREATE OR REPLACE PACKAGE BODY DM_SKB.pkg_outer_reports
  IS

FUNCTION get_ti_as_hms (inInterval IN NUMBER /*интервал в днях*/) RETURN VARCHAR2
  IS
BEGIN
  RETURN LPAD(TO_CHAR(TRUNC(inInterval*24*60*60/3600)),3,' ')||'h '||LPAD(TO_CHAR(TRUNC(MOD(inInterval*24*60*60,3600)/60)),2,' ')||'m '||LPAD(TO_CHAR(ROUND(MOD(MOD(inInterval*24*60*60,3600),60),0)),2,' ')||'s';
END get_ti_as_hms;

PROCEDURE pr_log_write(inRepName IN VARCHAR2,inMessage IN VARCHAR2)
  IS
    vBuff VARCHAR2(32700);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
   vBuff :=
   'BEGIN'||CHR(10)||
   'INSERT INTO tb_rep_log (dat, rep_name, message) VALUES (SYSDATE,:1,:2);'||CHR(10)||
   'END;';
   EXECUTE IMMEDIATE vBuff USING IN inRepName, IN inMessage;
   COMMIT;
END pr_log_write;

FUNCTION CommaTextFromCursor(inSQL IN CLOB,inSeparator IN VARCHAR2,inUppercase IN VARCHAR2 DEFAULT 'INITCAP') RETURN VARCHAR2
  /*INITCAP:UPPER:LOWER*/
  IS
    vRes VARCHAR2(32700);
    vOwner VARCHAR2(256) := pkg_etl_signs.GetVarValue('vOwner');
BEGIN
  IF inSQL IS NOT NULL THEN
    SELECT LISTAGG(CASE inUppercase WHEN 'LOWER' THEN LOWER(col_name) WHEN 'UPPER' THEN UPPER(col_name) ELSE INITCAP(col_name) END,inSeparator) WITHIN GROUP (ORDER BY col_num) AS str
      INTO vRes
      FROM TABLE(pkg_etl_signs.DescribeColumns(inSQL));
  END IF;
  RETURN vRes;
EXCEPTION WHEN OTHERS THEN
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.CommaTextFromCursor','ERROR :: '||SQLERRM);
  RETURN NULL;
END CommaTextFromCursor;

PROCEDURE prepare_log_table(outRes OUT VARCHAR2)
  IS
    vBuff VARCHAR2(32700);
    vOwner VARCHAR2(4000) := 'DM_SKB';--GetVarValue('vOwner');
BEGIN
  -- Создание таблицы
  vBuff :=
  'CREATE TABLE '||lower(vOwner)||'.tb_rep_log'||CHR(10)||
  '  (id NUMBER,dat DATE, rep_name VARCHAR2(256), message VARCHAR2(4000)) NOLOGGING';
  BEGIN
    EXECUTE IMMEDIATE vBuff;
    outRes := 'Table "'||lower(vOwner)||'.tb_rep_log" created successfully'||CHR(10);
  EXCEPTION WHEN OTHERS THEN
    outRes := 'Table "'||lower(vOwner)||'.tb_rep_log" not created. Error: '||SQLERRM||CHR(10);
  END;
  -- Создание индексов
  vBuff := 'CREATE UNIQUE INDEX '||lower(vOwner)||'.idx_tb_rep_log_u001 ON '||lower(vOwner)||'.tb_rep_log (id) NOLOGGING';
  BEGIN
    EXECUTE IMMEDIATE vBuff;
    outRes := outRes||'-------------------------'||CHR(10)||'Unique index "'||lower(vOwner)||'.idx_tb_rep_log_u001" created successfully'||CHR(10);
  EXCEPTION WHEN OTHERS THEN
    outRes := outRes||'-------------------------'||CHR(10)||'Unique index "'||lower(vOwner)||'.idx_tb_rep_log_u001" not created. Error: '||SQLERRM||CHR(10);
  END;

  vBuff := 'CREATE INDEX '||lower(vOwner)||'.idx_tb_rep_log_002 ON '||lower(vOwner)||'.tb_rep_log (dat) NOLOGGING';
  BEGIN
    EXECUTE IMMEDIATE vBuff;
    outRes := outRes||'-------------------------'||CHR(10)||'Index "'||lower(vOwner)||'.idx_tb_rep_log_002" created successfully'||CHR(10);
  EXCEPTION WHEN OTHERS THEN
    outRes := outRes||'-------------------------'||CHR(10)||'Index "'||lower(vOwner)||'.idx_tb_rep_log_002" not created. Error: '||SQLERRM||CHR(10);
  END;

  vBuff := 'CREATE INDEX '||lower(vOwner)||'.idx_tb_rep_log_003 ON '||lower(vOwner)||'.tb_rep_log (rep_name) NOLOGGING';
  BEGIN
    EXECUTE IMMEDIATE vBuff;
    outRes := outRes||'-------------------------'||CHR(10)||'Index "'||lower(vOwner)||'.idx_tb_rep_log_003" created successfully'||CHR(10);
  EXCEPTION WHEN OTHERS THEN
    outRes := outRes||'-------------------------'||CHR(10)||'Index "'||lower(vOwner)||'.idx_tb_rep_log_003" not created. Error: '||SQLERRM||CHR(10);
  END;

  -- Создание последовательности
  vBuff := 'CREATE SEQUENCE '||lower(vOwner)||'.tb_rep_log_id_seq MINVALUE 1 MAXVALUE 9999999999999999999999999999 START WITH 1 INCREMENT by 1 NOCACHE';
  BEGIN
    EXECUTE IMMEDIATE vBuff;
    outRes := outRes||'-------------------------'||CHR(10)||'Sequence "'||lower(vOwner)||'.tb_rep_log_id_seq" created successfully'||CHR(10);
  EXCEPTION WHEN OTHERS THEN
    outRes := outRes||'-------------------------'||CHR(10)||'Sequence "'||lower(vOwner)||'.tb_rep_log_id_seq" not created. Error: '||SQLERRM||CHR(10);
  END;

  -- Создание триггера
  vBuff :=
  'CREATE OR REPLACE TRIGGER '||lower(vOwner)||'.tb_rep_log_id_trg BEFORE INSERT ON '||lower(vOwner)||'.tb_rep_log FOR EACH ROW'||CHR(10)||
  'BEGIN SELECT '||lower(vOwner)||'.tb_rep_log_id_seq.nextval INTO :NEW.id FROM dual; END tb_rep_log_id_trg;';
  BEGIN
    EXECUTE IMMEDIATE vBuff;
    outRes := outRes||'-------------------------'||CHR(10)||'Trigger "'||lower(vOwner)||'.tb_rep_log_id_trg" compiled successfully'||CHR(10);
  EXCEPTION WHEN OTHERS THEN
    outRes := outRes||'-------------------------'||CHR(10)||'Trigger "'||lower(vOwner)||'.tb_rep_log_id_trg" not compiled. Error: '||SQLERRM||CHR(10);
  END;
END prepare_log_table;

FUNCTION GetReportSQL(inRepName VARCHAR2,inRepParamValues VARCHAR2) RETURN CLOB
  IS
    vOwner VARCHAR2(30) := pkg_etl_signs.GetVarValue('vOwner');
    vRepID NUMBER;
    vPivot CLOB;
    vSQL CLOB;
    vTop VARCHAR2(32700);
    vDown VARCHAR2(32700);
    vPivVal VARCHAR2(4000);
    vPivAls VARCHAR2(30);
    vCalcPivVal CLOB;
    vPivValRes VARCHAR2(32700);
    vCalcPivAls CLOB;
    vPivAlsRes VARCHAR2(30);
    vPivCur SYS_REFCURSOR;
    vCou INTEGER := 0;
    --
    errNotFindedRep EXCEPTION;
BEGIN
  BEGIN
    SELECT id,rep_pivot_values,rep_sql INTO vRepID,vPivot,vSQL FROM tb_outer_reports WHERE rep_name = inRepName;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RAISE errNotFindedRep;
  END;
  
  --Подменяем параметры их значениями
  FOR idx IN (
    SELECT p.ord,p.p_name,v.str AS p_val
      FROM tb_outer_rep_params p
           LEFT JOIN TABLE(pkg_etl_signs.parse_str(inRepParamValues,'#!#')) v ON  v.ord = p.ord
      WHERE p.rep_id = vRepID
    ORDER BY p.ord
  ) LOOP
    vPivot := REPLACE(vPivot,':'||idx.p_name,'q''['||idx.p_val||']''');
    vSQL := REPLACE(vSQL,':'||idx.p_name,CASE WHEN idx.p_val IS NOT NULL THEN 'q''['||idx.p_val||']''' ELSE 'NULL' END);
  END LOOP;

  IF vPivot IS NOT NULL THEN
      OPEN vPivCur FOR vPivot;
      LOOP
        FETCH vPivCur INTO vPivVal,vPivAls,vCalcPivVal,vCalcPivAls;
        EXIT WHEN vPivCur%NOTFOUND;
        vTop := vTop||'     ,'||vPivAls||CHR(10);
        vDown := vDown||CASE WHEN vCou > 0 THEN ',' END||'q''['||vPivVal||']'' AS '||vPivAls;
        IF vCalcPivVal IS NULL THEN vSQL := REPLACE(vSQL,':[PIVOT_VALUE'||vCou||']',vPivVal);
          ELSE EXECUTE IMMEDIATE vCalcPivVal USING OUT vPivValRes;
               vSQL := REPLACE(vSQL,':[PIVOT_VALUE'||vCou||']',vPivValRes);
        END IF;
        IF vCalcPivAls IS NULL THEN vSQL := REPLACE(vSQL,':[PIVOT_ALS'||vCou||']',vPivAls);
          ELSE EXECUTE IMMEDIATE vCalcPivAls USING OUT vPivAlsRes;
               vSQL := REPLACE(vSQL,':[PIVOT_VALUE'||vCou||']',vPivAlsRes);
        END IF;
        vCou := vCou + 1;
      END LOOP;
      CLOSE vPivCur;
    NULL;
  END IF;
  
  vSQL := REPLACE(REPLACE(vSQL,':[PIVOT_TOP]',vTop),':[PIVOT_DOWN]',vDown);
  RETURN vSQL;
EXCEPTION
  WHEN errNotFindedRep THEN pkg_etl_signs.pr_log_write(LOWER(vOwner)||'.pkg_outer_reports.GetReportSQL','ERROR :: Отчет "'||inRepName||'" не найден в таблице "'||LOWER(vOwner)||'.tb_outer_reports"');
  WHEN OTHERS THEN pkg_etl_signs.pr_log_write(LOWER(vOwner)||'.pkg_outer_reports.GetReportSQL','ERROR :: '||SQLERRM);
  RETURN NULL;
END GetReportSQL;

FUNCTION GetRepLog(inBegDate IN DATE,inEndDate IN DATE) RETURN tabRepLog PIPELINED
  IS
    rec recRepLog;
    vOwner VARCHAR2(30) := pkg_etl_signs.GetVarValue('vOwner');
BEGIN
  FOR idx IN (
    SELECT a.log_id
          ,a.dt
          ,a.rep_name
          ,r.rep_descr
          ,a.status_name
          ,REPLACE(REPLACE(a.apex_user,'USER => ',''),'"','') apex_user
          ,CASE WHEN a.cou >= 5 THEN a.exec_time ELSE NULL END AS exec_time
          ,a.rep_params
      FROM (
        SELECT l.id AS log_id
              ,l.dat AS dt
              ,t.ord AS ord
              ,t.str
              ,MAX(t.str) KEEP (dense_rank LAST ORDER BY t.ord) OVER (PARTITION BY l.id) AS rep_params
              ,COUNT(1) OVER (PARTITION BY l.id) AS cou
          FROM dm_skb.tb_rep_log l
               CROSS JOIN TABLE(dm_skb.pkg_etl_signs.parse_str(l.message,' :: ')) t
          WHERE TRIM(SUBSTR(message,1,INSTR(message,'::') - 1)) IN ('SUCCESSFULLY','ERROR')
            AND l.dat BETWEEN inBegDate AND inEndDate
      ) PIVOT (MAX(str) FOR ord IN (1 AS status_name,2 AS rep_name,3 AS apex_user, 4 AS exec_time)) a
      LEFT JOIN dm_skb.tb_outer_reports r ON r.rep_name = a.rep_name 
    ORDER BY log_id DESC
  ) LOOP
    rec.log_id := idx.log_id;
    rec.dt := idx.dt;
    rec.rep_name := idx.rep_name;
    rec.rep_descr := idx.rep_descr;
    rec.status_name := idx.status_name;
    rec.apex_user := idx.apex_user;
    rec.exec_time := idx.exec_time;
    rec.rep_params := idx.rep_params;
    PIPE ROW(rec);
  END LOOP;
EXCEPTION WHEN OTHERS THEN
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepLog','ERROR :: '||SQLERRM);
END GetRepLog;

FUNCTION GetRepCADescr(inSlideID VARCHAR2,inRepDate VARCHAR2,inRepType VARCHAR2,inHolding VARCHAR2,inSegment VARCHAR2,inTB VARCHAR2) RETURN VARCHAR2
  IS
BEGIN
  RETURN '<b>Клиентская аналитика</b><br><br><b>Раздел:</b>      '||CASE inSlideID       
                  WHEN '1' THEN 'Новые'
                  WHEN '2' THEN 'Отток клиентов'
                  WHEN '201' THEN 'Отток клиентов :: Ликвидация'
                  WHEN '202' THEN 'Отток клиентов :: Комплаенс'
                  WHEN '203' THEN 'Отток клиентов :: Другие КБ'
                  WHEN '204' THEN 'Отток клиентов :: Прочие'
                  WHEN '3' THEN 'Отток активных'
                  WHEN '301' THEN 'Отток активных :: Ликвидация'
                  WHEN '302' THEN 'Отток активных :: Комплаенс'
                  WHEN '303' THEN 'Отток активных :: Другие КБ'
                  WHEN '304' THEN 'Отток активных :: Прочие'
                  WHEN '4' THEN 'Отток спящих'
                  WHEN '401' THEN 'Отток спящих :: Ликвидация'
                  WHEN '402' THEN 'Отток спящих :: Комплаенс'
                  WHEN '403' THEN 'Отток спящих :: Другие КБ'
                  WHEN '404' THEN 'Отток спящих :: Прочие'
                  WHEN '5' THEN 'Отток мертвых'
                  WHEN '501' THEN 'Отток мертвых :: Ликвидация'
                  WHEN '502' THEN 'Отток мертвых :: Комплаенс'
                  WHEN '503' THEN 'Отток мертвых :: Другие КБ'
                  WHEN '504' THEN 'Отток мертвых :: Прочие'
                  WHEN '6' THEN 'Отток новых'
                  WHEN '601' THEN 'Отток новых :: Ликвидация'
                  WHEN '602' THEN 'Отток новых :: Комплаенс'
                  WHEN '603' THEN 'Отток новых :: Другие КБ'
                  WHEN '604' THEN 'Отток новых :: Прочие'
                  WHEN '7' THEN 'Клиенты в активной СЖЦ'
                  WHEN '8' THEN 'Заснувшие'
                  WHEN '9' THEN 'Спящие'
                  WHEN '10' THEN 'Мертвые'
                  WHEN '11' THEN 'Активизированные'
                  WHEN '12' THEN 'Умершие'
                  WHEN '13' THEN 'Открыто счетов'
                  WHEN '14' THEN 'Открыто 2-3... счетов'
                  WHEN '15' THEN 'Закрыто счетов'
                  WHEN '16' THEN 'Открыто спецсчетов (Гособоронзаказ)'
                  WHEN '17' THEN 'Действующие пакеты'
                  WHEN '18' THEN 'Клиенты, которым продали пакеты'
                  WHEN '19' THEN 'Новые клиенты с пакетами'
                  WHEN '20' THEN 'Кол-во нов. клиентов по счетам'
                ELSE inSlideID END||'<br>'||
'<b>Дата:</b>        '||inRepDate||'<br>'||
'<b>Тип отчета:</b>  '||inRepType||'<br>'||
'<b>Холдинг:</b>     '||CASE WHEN inHolding = 'yes' THEN 'Вкл.' ELSE 'Выкл.' END||'<br>'||
'<b>Сегмент:</b>     '||inSegment||'<br>'||
'<b>Группировка:</b> '||inTB;
END GetRepCADescr;

FUNCTION GetRepCols10(inRepName VARCHAR2,inParams VARCHAR2 DEFAULT NULL,inUser VARCHAR2 DEFAULT NULL) RETURN tabCols10 PIPELINED
  IS
    vRepDescr VARCHAR2(4000);
    vRepSQL CLOB;
    vOwner VARCHAR2(30) := pkg_etl_signs.GetVarValue('vOwner');
    vUser VARCHAR2(50) := NVL(inUser,UPPER(sys_context('userenv','OS_USER')));
    vRepParamsAsHTMLTable VARCHAR2(32700);
    vTop VARCHAR2(32700);
    vCou INTEGER := 0;
    rec recCols10;
    cur INTEGER;       -- хранит идентификатор (ID) курсора
    ret INTEGER;       -- хранит возвращаемое по вызову значение
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: '||inRepName||' :: USER => "'||vUser||'"';
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols10',vMes);

  IF inParams IS NOT NULL THEN
    SELECT r.rep_descr
          ,'<table>'||CHR(10)||LISTAGG('<tr><th>'||p.p_name||'</th><td>'||s.str||'</td></tr>',CHR(10)) WITHIN GROUP (ORDER BY p.ord)||CHR(10)||'</table>' AS rec
      INTO vRepDescr,vRepParamsAsHTMLTable
      FROM tb_outer_reports r 
           INNER JOIN tb_outer_rep_params p
             ON p.rep_id = r.id
           LEFT JOIN TABLE(pkg_etl_signs.parse_str(inParams,'#!#')) s
             ON s.ord = p.ord
      WHERE rep_name = inRepName
    GROUP BY r.rep_descr;
  END IF;
  
  vRepSQL := GetReportSQL(inRepName,inParams);

  vTop := 'SELECT ';
  FOR idx IN (
    WITH
      a AS (
        SELECT LEVEL AS col_num,'col_'||LPAD(to_char(LEVEL),2,'0') AS col FROM dual CONNECT BY LEVEL <= 10
      )
      SELECT a.col_num,a.col,b.col_name
        FROM a
             LEFT JOIN TABLE(pkg_Etl_Signs.DescribeColumns(vRepSQL)) b
               ON b.col_num = a.col_num
      ORDER BY a.col_num
  ) LOOP
    vTop := vTop||CASE WHEN vCou > 0 THEN '      ,' END||CASE WHEN idx.col_name IS NOT NULL THEN '"'||idx.col_name||'"' ELSE 'NULL' END||' AS '||idx.col||CHR(10);
    vCou := vCou + 1;
  END LOOP;
  vRepSQL := vTop||'  FROM ('||CHR(10)||'    '||vRepSQL||CHR(10)||'  )';
  --dbms_output.put_line(vRepSQL);
  
  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur,vRepSQL,dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.col_01,4000);
  dbms_sql.define_column(cur,2,rec.col_02,4000);
  dbms_sql.define_column(cur,3,rec.col_03,4000);
  dbms_sql.define_column(cur,4,rec.col_04,4000);
  dbms_sql.define_column(cur,5,rec.col_05,4000);
  dbms_sql.define_column(cur,6,rec.col_06,4000);
  dbms_sql.define_column(cur,7,rec.col_07,4000);
  dbms_sql.define_column(cur,8,rec.col_08,4000);
  dbms_sql.define_column(cur,9,rec.col_09,4000);
  dbms_sql.define_column(cur,10,rec.col_10,4000);

  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.col_01);
    dbms_sql.column_value(cur,2,rec.col_02);
    dbms_sql.column_value(cur,3,rec.col_03);
    dbms_sql.column_value(cur,4,rec.col_04);
    dbms_sql.column_value(cur,5,rec.col_05);
    dbms_sql.column_value(cur,6,rec.col_06);
    dbms_sql.column_value(cur,7,rec.col_07);
    dbms_sql.column_value(cur,8,rec.col_08);
    dbms_sql.column_value(cur,9,rec.col_09);
    dbms_sql.column_value(cur,10,rec.col_10);
    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);

  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime)||' :: '||vRepParamsAsHTMLTable;
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols10',vMes);

  vMes := 'FINISH :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols10',vMes);

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime)||' :: Отчет "'||inRepName||'" не найден в таблице "'||LOWER(vOwner)||'.tb_outer_reports"';
    pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols10',vMes);

    vMes := 'FINISH :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols10',vMes);
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime)||' :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols10',vMes);

    vMes := 'FINISH :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols10',vMes);
END GetRepCols10;

FUNCTION GetRepCols20(inRepName VARCHAR2,inParams VARCHAR2 DEFAULT NULL,inUser VARCHAR2 DEFAULT NULL) RETURN tabCols20 PIPELINED
  IS
    vRepDescr VARCHAR2(4000);
    vRepSQL CLOB;
    vOwner VARCHAR2(30) := pkg_etl_signs.GetVarValue('vOwner');
    vUser VARCHAR2(50) := NVL(inUser,UPPER(sys_context('userenv','OS_USER')));
    vRepParamsAsHTMLTable VARCHAR2(32700);
    vTop VARCHAR2(32700);
    vCou INTEGER := 0;
    rec recCols20;
    cur INTEGER;       -- хранит идентификатор (ID) курсора
    ret INTEGER;       -- хранит возвращаемое по вызову значение
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: '||inRepName||' :: USER => "'||vUser||'"';
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols20',vMes);

  IF inParams IS NOT NULL THEN
    SELECT r.rep_descr
          ,'<table>'||CHR(10)||LISTAGG('<tr><th>'||p.p_name||'</th><td>'||s.str||'</td></tr>',CHR(10)) WITHIN GROUP (ORDER BY p.ord)||CHR(10)||'</table>' AS rec
      INTO vRepDescr,vRepParamsAsHTMLTable
      FROM tb_outer_reports r 
           INNER JOIN tb_outer_rep_params p
             ON p.rep_id = r.id
           LEFT JOIN TABLE(pkg_etl_signs.parse_str(inParams,'#!#')) s
             ON s.ord = p.ord
      WHERE rep_name = inRepName
    GROUP BY r.rep_descr;
  END IF;
  
  vRepSQL := GetReportSQL(inRepName,inParams);

  vTop := 'SELECT ';
  FOR idx IN (
    WITH
      a AS (
        SELECT LEVEL AS col_num,'col_'||LPAD(to_char(LEVEL),2,'0') AS col FROM dual CONNECT BY LEVEL <= 20
      )
      SELECT a.col_num,a.col,b.col_name
        FROM a
             LEFT JOIN TABLE(pkg_Etl_Signs.DescribeColumns(vRepSQL)) b
               ON b.col_num = a.col_num
      ORDER BY a.col_num
  ) LOOP
    vTop := vTop||CASE WHEN vCou > 0 THEN '      ,' END||CASE WHEN idx.col_name IS NOT NULL THEN '"'||idx.col_name||'"' ELSE 'NULL' END||' AS '||idx.col||CHR(10);
    vCou := vCou + 1;
  END LOOP;
  vRepSQL := vTop||'  FROM ('||CHR(10)||'    '||vRepSQL||CHR(10)||'  )';
  --dbms_output.put_line(vRepSQL);
  
  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur,vRepSQL,dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.col_01,4000);
  dbms_sql.define_column(cur,2,rec.col_02,4000);
  dbms_sql.define_column(cur,3,rec.col_03,4000);
  dbms_sql.define_column(cur,4,rec.col_04,4000);
  dbms_sql.define_column(cur,5,rec.col_05,4000);
  dbms_sql.define_column(cur,6,rec.col_06,4000);
  dbms_sql.define_column(cur,7,rec.col_07,4000);
  dbms_sql.define_column(cur,8,rec.col_08,4000);
  dbms_sql.define_column(cur,9,rec.col_09,4000);
  dbms_sql.define_column(cur,10,rec.col_10,4000);
  dbms_sql.define_column(cur,11,rec.col_11,4000);
  dbms_sql.define_column(cur,12,rec.col_12,4000);
  dbms_sql.define_column(cur,13,rec.col_13,4000);
  dbms_sql.define_column(cur,14,rec.col_14,4000);
  dbms_sql.define_column(cur,15,rec.col_15,4000);
  dbms_sql.define_column(cur,16,rec.col_16,4000);
  dbms_sql.define_column(cur,17,rec.col_17,4000);
  dbms_sql.define_column(cur,18,rec.col_18,4000);
  dbms_sql.define_column(cur,19,rec.col_19,4000);
  dbms_sql.define_column(cur,20,rec.col_20,4000);

  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.col_01);
    dbms_sql.column_value(cur,2,rec.col_02);
    dbms_sql.column_value(cur,3,rec.col_03);
    dbms_sql.column_value(cur,4,rec.col_04);
    dbms_sql.column_value(cur,5,rec.col_05);
    dbms_sql.column_value(cur,6,rec.col_06);
    dbms_sql.column_value(cur,7,rec.col_07);
    dbms_sql.column_value(cur,8,rec.col_08);
    dbms_sql.column_value(cur,9,rec.col_09);
    dbms_sql.column_value(cur,10,rec.col_10);
    dbms_sql.column_value(cur,11,rec.col_11);
    dbms_sql.column_value(cur,12,rec.col_12);
    dbms_sql.column_value(cur,13,rec.col_13);
    dbms_sql.column_value(cur,14,rec.col_14);
    dbms_sql.column_value(cur,15,rec.col_15);
    dbms_sql.column_value(cur,16,rec.col_16);
    dbms_sql.column_value(cur,17,rec.col_17);
    dbms_sql.column_value(cur,18,rec.col_18);
    dbms_sql.column_value(cur,19,rec.col_19);
    dbms_sql.column_value(cur,20,rec.col_20);
    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);

  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime)||' :: '||vRepParamsAsHTMLTable;
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols20',vMes);

  vMes := 'FINISH :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols20',vMes);

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime)||' :: Отчет "'||inRepName||'" не найден в таблице "'||LOWER(vOwner)||'.tb_outer_reports"';
    pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols20',vMes);

    vMes := 'FINISH :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols20',vMes);
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime)||' :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols20',vMes);

    vMes := 'FINISH :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCols20',vMes);
END GetRepCols20;

FUNCTION GetRepCA(inRepName VARCHAR2,inRepDate VARCHAR2,inRepType VARCHAR2,inSgnAlias VARCHAR2,inClIsHolding VARCHAR2,inClSegment VARCHAR2,inTB VARCHAR2 DEFAULT NULL,inGroupColumn VARCHAR2 DEFAULT 'Тербанк', inUser VARCHAR2 DEFAULT NULL) RETURN tabCA PIPELINED
IS
  vOwner VARCHAR2(30) := pkg_etl_signs.GetVarValue('vOwner');
  vUser VARCHAR2(50) := NVL(inUser,UPPER(sys_context('userenv','OS_USER')));
  vRepParamValues VARCHAR2(32700);
  vRepParamsAsHTMLTable VARCHAR2(32700);
  rec recCA;
  cur INTEGER;       -- хранит идентификатор (ID) курсора
  ret INTEGER;       -- хранит возвращаемое по вызову значение
  --
  vMes VARCHAR2(32700);
  vBegTime DATE := SYSDATE;
  vEndTime DATE;
BEGIN
  vMes := 'START :: '||inRepName||' :: USER => "'||vUser||'"';
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCA',vMes);

  vRepParamValues := inRepDate||'#!#'||inRepType||'#!#'||inSgnAlias||'#!#'||inClIsHolding||'#!#'||inClSegment||'#!#'||inTB||'#!#'||inGroupColumn;
  vRepParamsAsHTMLTable := '<table><tr><th>inRepDate</th><td>'||inRepDate||'</td></tr><tr><th>inRepType</th><td>'||inRepType||'</td></tr><tr><th>inSgnAlias</th><td>'||inSgnAlias||'</td></tr><tr><th>inClIsHolding</th><td>'||inClIsHolding||'</td></tr><tr><th>inClSegment</th><td>'||inClSegment||'</td></tr><tr><th>inTB</th><td>'||inTB||'</td></tr><tr><th>inGroupColumn</th><td>'||inGroupColumn||'</td></tr></table>';
  cur := dbms_sql.open_cursor;
  
  --dbms_output.put_line(dm_skb.pkg_outer_reports.GetReportSQL(inRepName,vRepParamValues));
  
  dbms_sql.parse(cur, /*dm_skb.pkg_outer_reports.*/GetReportSQL(inRepName,vRepParamValues), dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.O1,4000);
  dbms_sql.define_column(cur,2,rec.O2,4000);
  dbms_sql.define_column(cur,3,rec.O3,4000);
  dbms_sql.define_column(cur,4,rec.O4,4000);
  dbms_sql.define_column(cur,5,rec.O5,4000);
  dbms_sql.define_column(cur,6,rec.O6,4000);
  dbms_sql.define_column(cur,7,rec.O7,4000);
  --dbms_sql.define_column(cur,8,rec.O8,4000);

  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.O1);
    dbms_sql.column_value(cur,2,rec.O2);
    dbms_sql.column_value(cur,3,rec.O3);
    dbms_sql.column_value(cur,4,rec.O4);
    dbms_sql.column_value(cur,5,rec.O5);
    dbms_sql.column_value(cur,6,rec.O6);
    dbms_sql.column_value(cur,7,rec.O7);
    --dbms_sql.column_value(cur,8,rec.O8);
    PIPE ROW(rec);
  END LOOP;
  
  dbms_sql.close_cursor(cur);
  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime)||' :: '||vRepParamsAsHTMLTable;
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCA',vMes);

  vMes := 'FINISH :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCA',vMes);
EXCEPTION WHEN OTHERS THEN
  vEndTime := SYSDATE;
  vMes := 'ERROR :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime)||' :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCA',vMes);

  vMes := 'FINISH :: '||inRepName||' :: USER => "'||vUser||'" :: Время выполнения: '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCA',vMes);
END GetRepCA;


FUNCTION GetRepCADetail(inRepName VARCHAR2,inRepDate VARCHAR2,inRepType VARCHAR2,inSgnAlias VARCHAR2,inClIsHolding VARCHAR2,inClSegment VARCHAR2,inTB VARCHAR2 DEFAULT NULL) RETURN tabCADetail PIPELINED
IS
  vOwner VARCHAR2(30) := pkg_etl_signs.GetVarValue('vOwner');
  vRepParamValues VARCHAR2(32700);
  rec recCADetail;
  cur INTEGER;       -- хранит идентификатор (ID) курсора
  ret INTEGER;       -- хранит возвращаемое по вызову значение
BEGIN
  
  vRepParamValues := inRepDate||'#!#'||inRepType||'#!#'||inSgnAlias||'#!#'||inClIsHolding||'#!#'||inClSegment||'#!#'||inTB;
  cur := dbms_sql.open_cursor;
  
  --dbms_output.put_line(dm_skb.pkg_outer_reports.GetReportSQL(inRepName,vRepParamValues));
  
  dbms_sql.parse(cur,/*dm_skb.pkg_outer_reports.*/GetReportSQL(inRepName,vRepParamValues), dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.reportdate,4000);
  dbms_sql.define_column(cur,2,rec.reporttype,4000);
  dbms_sql.define_column(cur,3,rec.inn,4000);
  dbms_sql.define_column(cur,4,rec.tb,4000);
  dbms_sql.define_column(cur,5,rec.full_description,4000);
  dbms_sql.define_column(cur,6,rec.gosb,4000);
  dbms_sql.define_column(cur,7,rec.vko,4000);
  dbms_sql.define_column(cur,8,rec.status,4000);
  dbms_sql.define_column(cur,9,rec.branch,4000);
  dbms_sql.define_column(cur,10,rec.get_holding,4000);
  dbms_sql.define_column(cur,11,rec.holding_id,4000);
  dbms_sql.define_column(cur,12,rec.seg,4000);
  dbms_sql.define_column(cur,13,rec.accessory,4000);
  dbms_sql.define_column(cur,14,rec.priority,4000);
  dbms_sql.define_column(cur,15,rec.is_new,4000);
  dbms_sql.define_column(cur,16,rec.is_active,4000);
  dbms_sql.define_column(cur,17,rec.is_sleeping,4000);
  dbms_sql.define_column(cur,18,rec.is_dead,4000);
  dbms_sql.define_column(cur,19,rec.is_lost,4000);
  dbms_sql.define_column(cur,20,rec.is_asleep,4000);
  dbms_sql.define_column(cur,21,rec.is_gone,4000);
  dbms_sql.define_column(cur,22,rec.is_activated,4000);
  dbms_sql.define_column(cur,23,rec.is_got_lost,4000);
  dbms_sql.define_column(cur,24,rec.is_liquid,4000);
  dbms_sql.define_column(cur,25,rec.is_comply,4000);
  dbms_sql.define_column(cur,26,rec.is_kb,4000);
  dbms_sql.define_column(cur,27,rec.get_first_prod,4000);
  dbms_sql.define_column(cur,28,rec.get_max_asleep,4000);
  dbms_sql.define_column(cur,29,rec.get_min_new,4000);
  dbms_sql.define_column(cur,30,rec.get_liq_stat,4000);
  dbms_sql.define_column(cur,31,rec.get_pred_base,4000);
  dbms_sql.define_column(cur,32,rec.get_comp_stat,4000);

  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.reportdate);
    dbms_sql.column_value(cur,2,rec.reporttype);
    dbms_sql.column_value(cur,3,rec.inn);
    dbms_sql.column_value(cur,4,rec.tb);
    dbms_sql.column_value(cur,5,rec.full_description);
    dbms_sql.column_value(cur,6,rec.gosb);
    dbms_sql.column_value(cur,7,rec.vko);
    dbms_sql.column_value(cur,8,rec.status);
    dbms_sql.column_value(cur,9,rec.branch);
    dbms_sql.column_value(cur,10,rec.get_holding);
    dbms_sql.column_value(cur,11,rec.holding_id);
    dbms_sql.column_value(cur,12,rec.seg);
    dbms_sql.column_value(cur,13,rec.accessory);
    dbms_sql.column_value(cur,14,rec.priority);
    dbms_sql.column_value(cur,15,rec.is_new);
    dbms_sql.column_value(cur,16,rec.is_active);
    dbms_sql.column_value(cur,17,rec.is_sleeping);
    dbms_sql.column_value(cur,18,rec.is_dead);
    dbms_sql.column_value(cur,19,rec.is_lost);
    dbms_sql.column_value(cur,20,rec.is_asleep);
    dbms_sql.column_value(cur,21,rec.is_gone);
    dbms_sql.column_value(cur,22,rec.is_activated);
    dbms_sql.column_value(cur,23,rec.is_got_lost);
    dbms_sql.column_value(cur,24,rec.is_liquid);
    dbms_sql.column_value(cur,25,rec.is_comply);
    dbms_sql.column_value(cur,26,rec.is_kb);
    dbms_sql.column_value(cur,27,rec.get_first_prod);
    dbms_sql.column_value(cur,28,rec.get_max_asleep);
    dbms_sql.column_value(cur,29,rec.get_min_new);
    dbms_sql.column_value(cur,30,rec.get_liq_stat);
    dbms_sql.column_value(cur,31,rec.get_pred_base);
    dbms_sql.column_value(cur,32,rec.get_comp_stat);

    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);
EXCEPTION
  WHEN OTHERS THEN
    pkg_etl_signs.pr_log_write(lower(vOwner)||'.pkg_outer_reports.GetRepCADetail','ERROR :: '||SQLERRM);
END GetRepCADetail;

END pkg_outer_reports;
/
