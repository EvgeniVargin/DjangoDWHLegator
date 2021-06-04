CREATE OR REPLACE PACKAGE DM_SKB.pkg_etl_signs
  IS
    TYPE recCols IS RECORD (col_num INTEGER,col_name VARCHAR2(30),col_type VARCHAR2(30),col_len NUMBER);
    TYPE tabCols IS TABLE OF recCols;
    TYPE recStr IS RECORD (ord NUMBER,str VARCHAR2(4000));
    TYPE tabStr IS TABLE OF recStr;
    TYPE TRec IS RECORD
      (obj_gid VARCHAR2(256)
      ,source_system_id VARCHAR2(30)
      ,sign_name VARCHAR2(256)
      ,sign_val VARCHAR2(4000));
    TYPE TTab IS TABLE OF TRec;
    TYPE TRecMass IS RECORD
      (effective_start DATE
      ,effective_end DATE
      ,obj_gid VARCHAR2(256)
      ,source_system_id VARCHAR2(30)
      ,sign_name VARCHAR2(256)
      ,sign_val VARCHAR2(4000));
    TYPE TTabMass IS TABLE OF TRecMass;
    TYPE TRecCHBuilder IS RECORD
      (Id VARCHAR2(256),Parent_id VARCHAR2(256),Unit VARCHAR2(256),Params VARCHAR2(4000),Skip NUMBER);
    TYPE TTabCHBuilder IS TABLE OF TRecCHBuilder;
    TYPE TRecTree IS RECORD (Id VARCHAR2(4000),ParentId VARCHAR2(4000));
    TYPE TTabTree IS TABLE OF TRecTree;
    TYPE TRecAnltSpecImp IS RECORD
      (val VARCHAR2(4000)
      ,parent_val VARCHAR2(4000)
      ,name VARCHAR2(4000)
      ,condition CLOB/*VARCHAR2(32700)*/);
    TYPE TTabAnltSpecImp IS TABLE OF TRecAnltSpecImp;
    TYPE TRecLabels IS RECORD
      (id NUMBER,parent_id NUMBER,caption VARCHAR2(4000), ord NUMBER, form_id NUMBER);
    TYPE TTabLabels IS TABLE OF TRecLabels;
    TYPE TRecReports IS RECORD (id NUMBER,query_name VARCHAR2(256), query_descr VARCHAR2(4000), ord NUMBER);
    TYPE TTabReports IS TABLE OF TRecReports;
    TYPE TRecAggrTables IS RECORD (table_name VARCHAR2(4000),cols VARCHAR2(4000),aliases VARCHAR2(4000),col_types VARCHAR2(4000), col_comments VARCHAR2(4000), fct_key_field VARCHAR2(30));
    TYPE TTabAggrTables IS TABLE OF TRecAggrTables;
    TYPE TRecStarFldList IS RECORD (table_alias VARCHAR2(256),table_name VARCHAR2(256),table_comment VARCHAR2(4000),col_name VARCHAR2(256),col_type VARCHAR2(30),preaggr_flg NUMBER, col_comment VARCHAR2(4000),entity_id NUMBER,entity_name VARCHAR2(4000));
    TYPE TTabStarFldList IS TABLE OF TRecStarFldList;

  FUNCTION GetLabels(inOSUser VARCHAR2) RETURN TTabLabels PIPELINED; -- Используется только в GUI. Возвращает коллекцию. Пункты меню, доступные пользователю, согласно его ролей
  FUNCTION GetReports(inOSUser VARCHAR2,inFormID NUMBER) RETURN TTabReports PIPELINED; -- Используется только в GUI. Возвращает коллекцию. Наименования пользовательских отчетов
  PROCEDURE pr_log_write(inUnit IN VARCHAR2,inMessage IN VARCHAR2); -- Для логирования
  PROCEDURE send_message_about_project(inProjID NUMBER,inTheme VARCHAR2,inMessage CLOB); -- (ВРЕМЕННО ДОБАВЛЕНА ДЛЯ ОТЛАДКИ, ПОСЛЕ ОТЛАДКИ УБРАТЬ ИЗ СПЕЦИФИКАЦИИ)Отправка сообщений
  FUNCTION DescribeColumns(inSQL IN CLOB) RETURN tabCols PIPELINED; -- Возвращает набор колонок произвольного SQL-запроса
  PROCEDURE SendMainLogs(inOPID NUMBER); -- Отправка центральных логов
  PROCEDURE pr_stat_write(inSignName IN VARCHAR2,inAnltCode IN VARCHAR2,inSec NUMBER,inAction VARCHAR2); --Для логирования статистики времени расчетов. Позволяет легко выявлять "тяжелые" расчеты для последующей оптимизации
  FUNCTION get_ti_as_hms (inInterval IN NUMBER /*интервал в днях*/) RETURN VARCHAR2; -- Для записи в логах временных интервалов в удобной для чтения форме. Возвращает временной интервал в виде строки в формате: 0h 12m 52
  FUNCTION parse_str(inStr VARCHAR2,inSeparator IN VARCHAR2) RETURN tabStr PIPELINED; -- Для парсинга единой строки в набор из множества строк. Возвращает коллекцию строк.
  FUNCTION split_clob(inCLOB IN CLOB,inStrLen IN INTEGER) RETURN tabStr PIPELINED; -- Для разбивки CLOB на строки указанной длины. Возвращает коллекцию строк.
  FUNCTION gather_clob(inTable IN SYS_REFCURSOR) RETURN CLOB; -- Собирает единый CLOB из коллекции строк
  FUNCTION isEqual(n1 IN NUMBER,n2 IN NUMBER) RETURN NUMBER; -- Для сравнения числовых значений. Возвращает 0 или 1 которые следует интерпретировать как ложь или истина
  FUNCTION isEqual(v1 IN VARCHAR2,v2 IN VARCHAR2) RETURN NUMBER; -- Для сравнения строковых значений. Возвращает 0 или 1 которые следует интерпретировать как ложь или истина
  FUNCTION isEqual(d1 IN DATE,d2 IN DATE) RETURN NUMBER; -- Для сравнения значений в виде даты. Возвращает 0 или 1 которые следует интерпретировать как ложь или истина
  FUNCTION isEqual(c1 IN CLOB,c2 IN CLOB) RETURN NUMBER; -- Для сравнения значений в виде текста. Возвращает 0 или 1 которые следует интерпретировать как ложь или истина
  FUNCTION DBLinkReady(inDBLinkName VARCHAR2) RETURN BOOLEAN; -- Проверяет доступность Data Base Link'а. Возвращает истину или ложь
  FUNCTION TableNotEmpty(inTbl VARCHAR2) RETURN BOOLEAN; -- Проверяет что таблица не пуста. Возвращает истину или ложь
  FUNCTION GetConditionResult(inCondition IN CLOB,inParams VARCHAR2 DEFAULT NULL,inComment VARCHAR2 DEFAULT NULL) RETURN NUMBER; -- Осуществляет проверку произвольного логического выражения. Возвращает 0 или 1. Может принимать входящие параметры с поисанием их типов данных. 
  FUNCTION GetGroupIdByName(inGroupName IN VARCHAR2) RETURN NUMBER; -- Возвращает ИД группы по её наименованию
  PROCEDURE mass_load_parallel_by_date_pe(inBeg IN DATE, inEnd IN DATE, inUnit IN VARCHAR2 DEFAULT NULL
    ,inParams IN VARCHAR2 DEFAULT NULL); -- !!!СЛЕДУЕТ ИСПОЛЬЗОВАТЬ ЧЕРЕЗ ОБЕРТКУ mass_load_parallel_by_month!!! Позволяет запускать процедуры в многопоточном за периоды при условии что сама процедура имеет входящие параметры начала и окончания периода. Период может вырождаться в один день (тогда начало и конец периода совпадают). Использует функционал  DBMS_PARALLEL_EXECUTE порождает столько потоков сколько дней в указанном периоде
  PROCEDURE mass_load_parallel_by_month (inBegDate IN DATE, inEndDate IN DATE, inProcedure IN VARCHAR2
    ,inParams VARCHAR2 DEFAULT NULL); -- Позволяет запускать процедуры в многопоточном за периоды при условии что сама процедура имеет входящие параметры начала и окончания периода. Период может вырождаться в один день (тогда начало и конец периода совпадают). Использует функционал  DBMS_PARALLEL_EXECUTE порождает столько потоков сколько дней в периоде, но не более чем дней в месяце. Если в периоде несколько месяцев, то месяцы пересчитываются последовательно
  PROCEDURE mass_load_parallel_by_ydate_pe
    (inBegDate IN DATE, inEndDate IN DATE, inUnit IN VARCHAR2
    ,inParams IN VARCHAR2 DEFAULT NULL
    ,inLastDay BOOLEAN DEFAULT TRUE
    ,inMonthlyDay VARCHAR2 DEFAULT NULL); -- !!!СЛЕДУЕТ ИСПОЛЬЗОВАТЬ ЧЕРЕЗ ОБЕРТКУ mass_load_parallel_by_year!!! Позволяет запускать процедуры в многопоточном за периоды при условии что сама процедура имеет входящие параметры начала и окончания периода. При этом последовательно перебираются числа месяца. Например сначала все 1-е числа всех месяцов, входящих в период, затем 2-е и т.д. Использует функционал  DBMS_PARALLEL_EXECUTE порождает столько потоков сколько месяцев в указанном периоде
  PROCEDURE mass_load_parallel_by_year
    (inBegDate IN DATE, inEndDate IN DATE, inProcedure IN VARCHAR2
    ,inParams VARCHAR2 DEFAULT NULL
    ,inLastDay BOOLEAN DEFAULT TRUE
    ,inMonthlyDay VARCHAR2 DEFAULT NULL
    ,inYearParallel BOOLEAN DEFAULT FALSE
    ,inHeadJobName IN VARCHAR2 DEFAULT NULL); -- Позволяет запускать процедуры в многопоточном за периоды при условии что сама процедура имеет входящие параметры начала и окончания периода. При этом последовательно перебираются числа месяца. Например сначала все 1-е числа всех месяцов, входящих в период, затем 2-е и т.д. Использует функционал  DBMS_PARALLEL_EXECUTE порождает 12 потоков (по количеству месяцев в году), при этом последовательно перебираются годы, если в периоде их несколько
  PROCEDURE MyExecute(inScript IN VARCHAR2); -- Выполнение произвольного PLSQL-блока без входящих параметров
  FUNCTION AnyExecute(inScript IN CLOB,inParams IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2; -- Выполнение произвольного PLSQL-блока, имеющего входящие параметры 
  PROCEDURE prepare_entity(inID IN NUMBER,outRes OUT CLOB); -- Подготовка таблиц хранения для сущности (динамический DDL)
  FUNCTION get_sign(inSign IN VARCHAR2,inDate IN DATE, inSQL IN VARCHAR2 DEFAULT NULL) RETURN TTab PIPELINED; -- Возвращает набор данных для показателя (на основе SQL, написанного разработчиком для указанного показателя)
  FUNCTION get_sign_anlt(inSign IN VARCHAR2,inDate IN DATE, inAnltCode IN VARCHAR2, inReverse NUMBER DEFAULT 0) RETURN TTab PIPELINED; -- Возвращает набор данных для "раскраски" показателя произвольным ключевым значением
  FUNCTION get_anlt_spec_imp(inDate IN DATE, inAnltCode IN VARCHAR2) RETURN TTabAnltSpecImp PIPELINED; -- Для импорта спецификации аналитики. Возвращает набор который необходимо импортировать. (часто требуется импортировать уже готовую иерархическую структуру в качестве спецификации, а не набивать руками)
  FUNCTION get_sign_mass(inSign IN VARCHAR2,inDate IN DATE) RETURN TTabMass PIPELINED; -- Возвращает набор данныхсодержащий всю историю изменения показателя сразу. (в случае если для показателя написан такой SQL)
  -- Подготовка субпартиций в таблицах
  FUNCTION CheckSubpartition(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2) RETURN VARCHAR2; -- Полдготовка партиции/субпартиции для хранимого набора. Возвращает наименование подготовленной партиции/субпартиции
  PROCEDURE CheckSubpartition(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2); -- аналог в виде процедуры (без возвращаемого значения)
  FUNCTION CompressSubpartition(inDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2) RETURN VARCHAR2; -- Сжатие партиции/субпартиции. Возвращает наименование сжатой партиции/субпартиции
  PROCEDURE CompressSubpartition(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2); -- аналог в виде процедуры (без возвращаемого значения)
  --
  PROCEDURE tb_load_daily(inBegDate IN DATE,inEndDate IN DATE,inSign VARCHAR2,inAnltCode IN VARCHAR2); -- !!!СЛЕДУЕТ ИСПОЛЬЗОВАТЬ ЧЕРЕЗ ОБЕРТКУ load_sign!!! расчет показателя, хранимого периодами за указанную дату
  PROCEDURE ptb_load_daily(inBegDate IN DATE,inEndDate IN DATE,inSign VARCHAR2,inAnltCode IN VARCHAR2); -- !!!СЛЕДУЕТ ИСПОЛЬЗОВАТЬ ЧЕРЕЗ ОБЕРТКУ load_sign!!! расчет показателя, хранимого по датам за указанную дату
  PROCEDURE load_sign(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2,inPrepareSegments NUMBER); -- обертка !!!ДЛЯ РАСЧЕТА ПОКАЗАТЕЛЕЙ СЛЕДУЕТ ИСПОЛЬЗОВАТЬ ИМЕННО ЕЕ!!!
  --  ******************  КОНЕЧНЫЕ ПРОЦЕДУРЫ ДЛЯ ЗАПУСКА РАСЧЕТОВ *****************
  --  ******************  ДЛЯ ИСПОЛЬЗОВАНИЯ В РАБОЧЕМ ПОРЯДКЕ *********************
  -- параллельная заливка указанных показателей за одну дату
  -- если параметр inSign не указан, то параллельная заливка ВСЕХ показателей за одну дату
  -- пример параметра inSign:  'ACCOUNT_CUM_COLATERAL,ACCOUNT_SUM_61312,ACOUNT_SUM_91414'
  PROCEDURE load_new(inSQL IN CLOB,inJobName IN VARCHAR2 DEFAULT NULL); -- Основной инструмент динамически выстраиваемого Чейна на основе SQL
  PROCEDURE load (inBegDate IN DATE,inEndDate IN DATE); -- Обертка запуск расчета всех показателей за определенную дату/период. В одной дате показатели расчитываются параллельно, но с учетом зависимостей если таковые имеются (классический Oracle Chain), при этом, даты в периоде пересчитываются последовательно
  PROCEDURE load_rel_asc(inBegDate IN DATE,inEndDate IN DATE,inSigns IN VARCHAR2,inUnit IN VARCHAR2,inAdvFilter VARCHAR2 DEFAULT NULL); -- Обертка запуск указанных и всех зависимых показателей за определенную дату/период. Цепь расчета выстраивается с учетом всех зависимостей. При построении цепи расчетов учитываются флаг архива показателя и доп. условия расчета
  PROCEDURE load_rel_desc(inBegDate IN DATE,inEndDate IN DATE,inSigns IN VARCHAR2,inUnit IN VARCHAR2,inAdvFilter VARCHAR2 DEFAULT NULL); -- Обертка запуск всех, от которых зависят указанные и самих указанных показателей за определенную дату/период. Цепь расчета выстраивается с учетом всех зависимостей. При построении цепи расчетов учитываются флаг архива показателя и доп. условия расчета
  PROCEDURE load_all_anlts(inBegDate IN DATE,inEndDate IN DATE); -- Обертка запуск расчета всех аналитик за определенную дату/период. В одной дате аналитики расчитываются параллельно, при этом, даты в периоде пересчитываются последовательно 
  -- *******************************************************************************
  -- *******************************************************************************
  -- Массовая загрузка показателя
  PROCEDURE mass_load(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2,inPrepareSegments NUMBER); -- Обертка для массового расчета показателя
  -- Склеивание периодов в исторических показателях
  --(необходимо например после "раздельно - массовой" загрузки исторического показателя)
  PROCEDURE sign_gluing(inSign IN VARCHAR2,inAnltCode IN VARCHAR2,inMask IN VARCHAR2 DEFAULT '111'); -- Склеивание показателя, хранящегося периодами
  PROCEDURE tmp_load_prev(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2); -- вставка первичных наборов данных за первые числа месяцев (при массовых расчетах за период)
  PROCEDURE tmp_load_daily(inBegDate IN DATE,inEndDate IN DATE,inSign VARCHAR2,inAnltCode IN VARCHAR2); -- обработка первичных наборов, расчеты за каждую дату (при массовых расчетах за период)
  PROCEDURE tb_upd_eff_end(inSign IN VARCHAR2,inAnltCode IN VARCHAR2,inDate IN DATE DEFAULT NULL); -- апдейт колонки effective_end показателя, хранящегося периодами, на указанную дату, или 31.12.5999 если дата не указана
  PROCEDURE tb_load_mass(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2
    ,inMask IN VARCHAR2 DEFAULT '111111'); -- !!!СЛЕДУЕТ ИСПОЛЬЗОВАТЬ ЧЕРЕЗ ОБЕРТКУ mass_load!!! массовый расчет за период показателя, хранящегося периодами
  /***************************************************************
   * Расшифровка маски:                                          *
   *  1-й символ: Предварительная загрузка 1-х чисел месяца      *
   *              в промежуточную тиаблицу                       *
   *  2-й символ: Прогрузка данных в промежуточной таблице       *
   *              за каждое число месяца, начиная со 2-го        *
   *  3-й символ: Очистка целевой партиции (если 0 то происходит *
   *              подгонка effective_start и effective_end       *
   *              по началу и окончанию периода)                 *
   *  4-й символ: Загрузка данных в целевую таблицу              *
   *  5-й символ: Сжатие данных и перестроение индексов в        *
   *              целевой таблице                                *
   *  6-й символ: Сбор статистики по целевой таблице             *
   ***************************************************************/
  PROCEDURE SignExtProcessing(inSign IN VARCHAR2,inDate IN DATE); -- НЕ ИСПОЛЬЗУЕТСЯ 
  FUNCTION get_empty_sign_id RETURN NUMBER; -- Возвращает минимальный не занятый ИД из справочника показателей
  FUNCTION DropSignPartitions(inSign IN VARCHAR2) RETURN VARCHAR2; -- !!!СЛЕДУЕТ ИСПОЛЬЗОВАТЬ ЧЕРЕЗ ОБЕРТКУ drop_sign!!! Удаление партиций с хранимыми данными (используется при удалении показателя)
  PROCEDURE drop_sign(inSign IN VARCHAR2,outRes OUT VARCHAR2); -- Обертка. Удаление показателя
  FUNCTION GetTreeList(inSQL IN CLOB) RETURN TTabTree PIPELINED; -- Возвращает иерархический  набор данных в виде дерева
  FUNCTION GetChainList(inSQL IN CLOB) RETURN TTabCHBuilder PIPELINED; -- Возвращает иерархический набор данных на основе переданного SQL. Используется Чейн-билдером
  FUNCTION GetTreeSQL(inFullSQL IN CLOB
                   ,inStartSQL IN CLOB
                   ,inIncludeChilds IN INTEGER DEFAULT 0)
    RETURN CLOB; -- НЕ ИСПОЛЬЗУЕТСЯ
  FUNCTION ChainBuilder(inSQL CLOB) RETURN VARCHAR2; -- Динамическое построение Chain'а на основе переданного SQL
  FUNCTION ChainStarter(inChainName IN VARCHAR2,inHeadJobName IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2; --Старт динамически построенного чейна
  PROCEDURE ChainKiller(inChainName VARCHAR2); -- Удаление динамически построенного и отработавшего чейна
  
  PROCEDURE calc(inBegDate IN DATE,inEndDate IN DATE,inSendMessage BOOLEAN DEFAULT FALSE); -- Запуск пула расчетов за период (оычно запускается за одну дату, в периоде даты пересчитывает последовательно)
  PROCEDURE CalcSignsByGroup(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER,inJobName VARCHAR2); -- Пересчет всех показателей группы
  PROCEDURE CalcSignsByStar(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER,inJobName VARCHAR2); -- Пересчет всех показателей звезды
  PROCEDURE CalcAnltByGroup(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER,inJobName VARCHAR2,inParallelJobs NUMBER DEFAULT 30); -- Раскраска всех фактов звезды ключевыми значениями. В качестве входящего параметра принимает ИД группы аналитик
  PROCEDURE CalcAnltByStar(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER,inJobName VARCHAR2,inParallelJobs NUMBER DEFAULT 30); -- Раскраска всех фактов звезды ключевыми значениями. В качестве входящего параметра принимает ИД звезды
  /*********  ИМПОРТ - ЭКСПОРТ *************/
  --FUNCTION AnltSpecImpGetCondition(inSignName VARCHAR2,inIds VARCHAR2 DEFAULT NULL,inProduct IN NUMBER DEFAULT 0) RETURN CLOB; -- 0 - показатель; 1 - продукт
  PROCEDURE ImportAnltSpecs(inDate IN DATE,inAnltCodes IN VARCHAR2); --Параллельный запуск импорта спецификаций указанных аналитик
  PROCEDURE AnltSpecImport(inDate IN DATE,inAnltCode IN VARCHAR2); --Импорт спецификации аналитики (часто требуется импортировать уже готовую иерархическую структуру в качестве спецификации, а не набивать руками)
 /******  ЗВЁЗДЫ И ВСЁ ЧТО С НИМИ СВЯЗАНО ***************************/
  FUNCTION  GetAnltLineSQL(inSQL IN CLOB,inIDName IN VARCHAR2
    ,inPIDName IN VARCHAR2,inName IN VARCHAR2,inValue IN VARCHAR2) RETURN CLOB; -- Возвращает динамически построенный SQL запрос для иерархического измерения, который используется при разворачивании звезды за дату
  FUNCTION StarGetFldList(inDate DATE,inGroupID NUMBER) RETURN TTabStarFldList PIPELINED; -- Возвращает список всех таблиц звезды с полями и флагом предагрегации по каждому полю
  PROCEDURE StarPrepareAggrTable(inDate IN DATE,inAggrID IN NUMBER); -- Подготовка таблицы - агрегата звезды за дату
  PROCEDURE StarPrepareAggrs(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER); -- ОБЕРТКА подготовка всех агрегатов звезды за период
  PROCEDURE StarPrepareDim(inDate IN DATE,inGroupID IN NUMBER,inEntityID IN NUMBER); -- Подготовка простых измерений звезды, содержащих детальные данные (динамический DDL)
  PROCEDURE PrepareTableBySQL(inDate IN DATE,inTableName IN VARCHAR2,inSQL IN CLOB,inComment IN VARCHAR2 DEFAULT NULL); -- Подготовка таблицы (партиции по AS_OF_DATE) по произвольному SQL
  PROCEDURE StarPrepareAnlt(inDate IN DATE,inGroupID IN NUMBER,inAnltCode IN VARCHAR2); -- Подготовка иерархических измерений звезды, содержащих детальные данные (динамический DDL)
  PROCEDURE StarPrepareFct(inDate IN DATE,inGroupID IN NUMBER); -- Подготовка таблиц фактов звезды, содержащих детальные данные (динамический DDL)
  --PROCEDURE StarFctOnDate(inDate IN DATE,inGroupID IN NUMBER,inEntityID IN NUMBER); -- зполнение таблицы фактов за дату, содержащей детальные данные (выполнение DML)
  PROCEDURE StarFctOnDateSign(inDate IN DATE,inGroupID IN NUMBER,inSign IN VARCHAR2); -- вставка данных в таблицу фактов за дату, содержащую детальные данные (выполнение DML) альтернатива предыдущей общей заливке для выполнения многопоточной вставки по каждому показателю
  PROCEDURE StarAggrOnDate(inDate IN DATE,inAggrID IN NUMBER); -- заполнение произвольной таблицы - предагрегата за дату (выполнение DML)
  PROCEDURE StarDimOnDate(inDate IN DATE,inGroupID IN NUMBER,inEntityID IN NUMBER); -- заполнение таблицы измерения за дату, содержащей детальные данные (выполнение DML)
  PROCEDURE StarAnltOnDate(inDate IN DATE,inGroupID IN NUMBER,inAnltAlias IN VARCHAR2); -- заполнение таблицы иерархического измерения (аналитики) за дату, содержащей детальные данные

  PROCEDURE StarPrepare(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER); -- ОБЕРТКА запуск подготовки таблиц звезды
  PROCEDURE StarClear(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER); -- ОБЕРТКА запуск очистки звезды за период
  PROCEDURE StarAggrsLoadData(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER); -- ОБЕРТКА запуск загрузки данных в таблицы предагрегатов
  PROCEDURE
  /************************************
   Описание маски (0 - не выполнять, 1 - выполнять):
   1-й символ - предварительный пересчет всех показателей по кубу
   2-й символ - предварительный пересчет всех аналитик по кубу
  ************************************/
    StarExpand(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER,inMask VARCHAR2 DEFAULT '00',inDaemonId NUMBER DEFAULT NULL,inParallelJobs NUMBER DEFAULT 30); -- ОБЕРТКА разворачивание звезды за период
  PROCEDURE StarCompress(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER); -- ОБЕРТКА сжатие таблиц детального слоя звезды за период
  PROCEDURE StarAggrsCompress(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER); -- ОБЕРТКА сжатие таблиц - предагрегатов звезды за период
  PROCEDURE StarGatherStats(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER); -- ОБЕРТКА сбор статистики по таблицам детального слоя звезды за период
  PROCEDURE StarAggrsGatherStats(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER); -- ОБЕРТКА сбор статистики по таблицам - предагрегатам звезды за период
  PROCEDURE StarDropOldParts(inDate IN DATE,inGroupID IN NUMBER); -- ОБЕРТКА удаление старых партиций с данными звезды (тех которые превысили запланированные сроки хранения)
  FUNCTION StarFixEmptyComments(inGroupID IN NUMBER) RETURN VARCHAR2; -- По возможности, заполняет отсутствующие комментарии таблиц звезды значениями по умолчанию. Возвращает строку (сообщение о количестве заполненных комментариев)
  FUNCTION StarFieldCommentsAsHTML(inGroupID IN NUMBER) RETURN CLOB; -- Возвращает CLOB (Готовое описание в виде HTML)
  PROCEDURE StarAggrRecalcOnDate(inDate IN DATE, inAggrID IN NUMBER); -- ОБЕРТКА осуществляет полный перерасчет отдельного агрегата за дату (с подготовкой,очисткой,наполнением,сжатием и сбором статистики)
  /********************************************************************/
  /************************ Техническое обслуживание ******************/
  PROCEDURE HistTableService(inTableName IN VARCHAR2,inMask IN VARCHAR2,inSign IN VARCHAR2 DEFAULT NULL,inParallelJobs IN NUMBER DEFAULT 30); -- ОБЕРТКА обслуживание таблиц ХД, хранящих данные периодами
  PROCEDURE ServiceTables(inAgeDays NUMBER,inDaemonId NUMBER DEFAULT NULL); -- ОБЕРТКА запуск сервисного обслуживания таблиц
  /***************************************************************
   * Расшифровка маски:                                          *
   *  1-й символ: Сжатие данных                                  *
   *  2-й символ: Перестроение индексов                          *
   *  3-й символ: Сбор статистики                                *
   ***************************************************************/
  /********************************************************************/
  FUNCTION GetVarCLOBValue(inVarName VARCHAR2) RETURN CLOB DETERMINISTIC; -- Возвращает значение переменной приложения как есть
  FUNCTION GetVarValue(inVarName VARCHAR2) RETURN VARCHAR2 DETERMINISTIC; -- Умный возврат значения переменной приложения (для простых возвращает как есть, для вычисляемых - выполняет PLSQL блок и возвращает результат выполнения)
  /********************************************************************/
  FUNCTION call_hist(inTable IN VARCHAR2, inID IN VARCHAR2,inAction VARCHAR2) RETURN VARCHAR2; --Включение хранения истории изменений на таблице с метаданными
  FUNCTION CanHaveHistory(inTable IN VARCHAR2) RETURN BOOLEAN; -- Проверяет возможно ли включение истории изменений на таблице (не дает включить историю изменений если таблица хранит не метаданные)
  PROCEDURE SetFlag(inName IN VARCHAR2,inDate IN DATE,inVal IN VARCHAR2 DEFAULT NULL,inAction NUMBER DEFAULT 1); --Обработка флагов -- 1 - UPSERT, 0 - DELETE 
  FUNCTION GetFlag(inFlagName IN VARCHAR2, inDate IN DATE) RETURN VARCHAR2; -- Вернуть значение флага
  PROCEDURE LastFlag(inFlagName IN VARCHAR2,inValue IN VARCHAR2,inDate IN OUT DATE); -- Записывает во входящую переменную последнюю дату указанного флага с указанным значением. Поиск осуществляется за период >= изначально указанной в даты
  FUNCTION HaveFlagReady(inFlagName IN VARCHAR2,inFlagValue IN VARCHAR2 DEFAULT NULL,inStartDate DATE DEFAULT NULL) RETURN BOOLEAN; -- Возвращает TRUE если по указанному наименованию флага есть хотя бы один с указанным значением (по умолчанию проверяется значение READY), иначе возвращает FALSE
  FUNCTION SQLasHTML(inSQL IN CLOB,inColNames IN VARCHAR2,inColAliases IN VARCHAR2,inStyle IN VARCHAR2 DEFAULT NULL,inShowLogo BOOLEAN DEFAULT FALSE,inTabHeader VARCHAR2 DEFAULT NULL) RETURN CLOB; -- Возвращает результат запроса в виде HTML
  /*********************** РЕПЛИКАЦИИ ***********************************/
  FUNCTION ReplGetImpScript(inGroupID IN NUMBER,inMask IN VARCHAR2 DEFAULT '11111111') RETURN CLOB; -- !!!СЛЕДУЕТ ИСПОЛЬЗОВАТЬ ЧЕРЕЗ ОБЕРТКУ ReplStart!!! Динамическое формирование DML - скрипта для реплики метаданных звезд
  PROCEDURE ReplAnltOnDate(inDate IN DATE,inGroupID IN NUMBER,inGroupIdOld IN NUMBER,inDBLink IN VARCHAR2,inAnltCode IN VARCHAR2); -- Реплика иерархического измерения
  PROCEDURE ReplDimOnDate(inDate IN DATE,inGroupID IN NUMBER,inGroupIdOld IN NUMBER,inDBLink IN VARCHAR2,inEntityID IN NUMBER); -- Реплика простого измерения
  PROCEDURE ReplAggrOnDate(inDate IN DATE,inGroupID IN NUMBER,inGroupIdOld IN NUMBER,inDBLink IN VARCHAR2,inAggrID IN NUMBER); -- Реплика агрегата
  PROCEDURE ReplAggrsOnDate(inDate IN DATE,inGroupID IN NUMBER,inGroupIdOld IN NUMBER,inDBLink IN VARCHAR2); --ОБЕРТКА реплика всех агрегатов указанной группы
  PROCEDURE ReplStarDataOld(inDate IN DATE,inGroupID IN NUMBER,inGroupIdOld IN NUMBER,inFctGroupIdOld IN NUMBER,inDBLink IN VARCHAR2); -- !!!СЛЕДУЕТ ИСПОЛЬЗОВАТЬ ЧЕРЕЗ ОБЕРТКУ ReplStart!!! Запуск реплики данных звезд
  PROCEDURE ReplStart(inOPTP IN NUMBER,inDBLink IN VARCHAR2,inOPID NUMBER DEFAULT NULL); -- ОБЕРТКА Запуск полной реплики звезд
  PROCEDURE ReplAggrsStart(inOPTP IN NUMBER,inDBLink IN VARCHAR2,inOPID NUMBER DEFAULT NULL); -- ОБЕРТКА Запуск реплики агрегатов звезд
  FUNCTION Daemon(inCondition IN CLOB,inExecute IN CLOB,inCondParams IN VARCHAR2,inExecParams IN VARCHAR2,inComment IN VARCHAR2,inForce NUMBER DEFAULT 0) RETURN VARCHAR2; -- Функция - ДЕМОН. Выполняет EXECUTE - блок, если выполнены условия CONDITION - блока
  PROCEDURE ExecuteDaemon(inIdentifier IN VARCHAR2,inCondParams IN VARCHAR2 DEFAULT NULL,inExecParams IN VARCHAR2 DEFAULT NULL,inForce NUMBER DEFAULT 0); -- ОБЕРТКА -- Запуск демона
  PROCEDURE DaemonsRun; --ОБЕРТКА Запуск всех демонов (для использования в JOB'e)
  /*******************************************************************************************/
  PROCEDURE DSPrepareTable(inModelName IN VARCHAR2,inDate IN DATE,inTableType IN VARCHAR2 DEFAULT 'MD'); -- Подготавливает таблицу для хранения данных, связанных с моделью
  FUNCTION DSGetFtr(inDate IN DATE,inModelName IN VARCHAR2,inFtrName IN VARCHAR2) RETURN TTab PIPELINED; -- Возвращает набор данных для фичи (на основе SQL, написанного разработчиком для указанной фичи)
  PROCEDURE DSFtrOnDate(inDate IN DATE,inModelName IN VARCHAR2,inFtrName IN VARCHAR2); -- Вставка подготовленного набора размеченных данных за дату для фичи
  FUNCTION DSFitGetFtrSQL(inDate IN DATE,inModelName IN VARCHAR2,inFtrName IN VARCHAR2,inTop INTEGER DEFAULT 3) RETURN VARCHAR2; -- Возвращает SQL для дообучения модели по одной фиче
  PROCEDURE DSFitFtrOnDate(inDate IN DATE,inModelName IN VARCHAR2,inFtrName IN VARCHAR2,inTopPrdCou INTEGER DEFAULT 3); -- Вставка набора - результата обучения за дату для фичи
  PROCEDURE DSCompressTable(inModelName VARCHAR2,inDate IN DATE,inTableType IN VARCHAR2 DEFAULT 'MD'); --ОБЕРТКА сжатие таблицы хранения размеченных данных модели
  PROCEDURE DSFitFtrSetWeight(inDate IN DATE,inModelName IN VARCHAR2,inFtrName VARCHAR2); -- Устанавливает вес фичи по доле угаданных
  PROCEDURE DSMDOnDate(inDate IN DATE,inModelName IN VARCHAR2); --ОБЕРТКА Подготовка размеченных данных модели
  PROCEDURE DSFitOnDate(inDate IN DATE,inModelName IN VARCHAR2,inTop IN INTEGER DEFAULT 3); --ОБЕРТКА Обучение модели

  TYPE TrecRegrData IS RECORD(x NUMBER,y NUMBER);
  TYPE TTabRegrData IS TABLE OF TrecRegrData;
  
  FUNCTION DSRegrGetData(inSQL CLOB) RETURN TTabRegrData PIPELINED; --Возвращает набор данных для регрессии в виде коллекции (x,y)
  FUNCTION DSRegrGetAvg(inSQL IN VARCHAR2,inDeep IN INTEGER,inIter IN INTEGER DEFAULT 2) RETURN TTabRegrData PIPELINED; -- Возвращает усредненный набор данных на основе заданного
  FUNCTION DSRegrLTRGetData(inSQL CLOB) RETURN TTabRegrData PIPELINED; --Возвращает набор данных - линейный тренд (по начальному набору для регрессии)
  FUNCTION DSRegrLTRGetKoef(inSQL IN CLOB,inDeep IN INTEGER DEFAULT 3,inIter IN INTEGER DEFAULT 3) RETURN TrecRegrData DETERMINISTIC; --Возвращает коэффициенты для линейного тренда в виде записи (rec.x = A, rec.y = B)
  PROCEDURE DSRegrLTRGetKoef(inSQL IN CLOB,outM OUT NUMBER,outB OUT NUMBER); --Возвращает через разделитель коэффициенты для линейного тренда
  FUNCTION DSRegrLTRPredict(inSQL IN CLOB,inX IN NUMBER,inDeep IN INTEGER DEFAULT 3,inIter IN INTEGER DEFAULT 3) RETURN NUMBER; -- ОБКРТКА Прогнозирует Y по X на основе линейного тренда
  /*******************************************************************************************/
  FUNCTION RegistryCreateObjDDL(inTableName IN VARCHAR2,inTableComment IN VARCHAR2,inCreatingPart IN NUMBER DEFAULT 0) RETURN VARCHAR2; -- Формирование DDL создания по метаданным таблиц реестра
END pkg_etl_signs;
/
CREATE OR REPLACE PACKAGE BODY DM_SKB.pkg_etl_signs
  IS
FUNCTION GetLabels(inOSUser VARCHAR2) RETURN TTabLabels PIPELINED
  IS
    Rec TRecLabels;
BEGIN
  FOR idx IN (
    WITH
      rol AS (
        SELECT r.id
          FROM tb_role_registry r
        CONNECT BY PRIOR r.id = r.parent_id
        START WITH r.id IN (SELECT ur.role_id
                              FROM tb_urole_registry ur
                                   INNER JOIN tb_labrole_registry lr ON lr.role_id = ur.role_id
                                   INNER JOIN tb_user_registry u ON u.id = ur.user_id AND LOWER(u.user_name) = LOWER(inOSUser)
                           )
      )
    SELECT DISTINCT l.id,l.parent_id,l.caption,l.ord,l.form_id
      FROM tb_label_registry l
    CONNECT BY PRIOR l.id = l.parent_id
    START WITH l.id IN (SELECT lr.label_id FROM tb_labrole_registry lr
                          WHERE lr.label_id = l.id
                            AND lr.role_id IN (SELECT ID FROM rol))
  ) LOOP
    Rec.id := idx.id;
    Rec.parent_id := idx.parent_id;
    Rec.caption := idx.caption;
    Rec.ord := idx.ord;
    Rec.form_id := idx.form_id;
    PIPE ROW(Rec);
  END LOOP;
END;

FUNCTION GetReports(inOSUser VARCHAR2,inFormID NUMBER) RETURN TTabReports PIPELINED
  IS
    Rec TRecReports;
BEGIN
  FOR idx IN (
    WITH
      rol AS (
        SELECT r.id
          FROM tb_role_registry r
        CONNECT BY PRIOR r.id = r.parent_id
        START WITH r.id IN (SELECT ur.role_id
                              FROM tb_urole_registry ur
                                   INNER JOIN tb_user_registry u ON u.id = ur.user_id AND LOWER(u.user_name) = LOWER(inOSUser)
                           )
      )
    SELECT q.id,q.query_name,q.query_descr,q.ord
      FROM tb_query_registry q
           INNER JOIN tb_repform_registry rf
             ON rf.query_id = q.id
                AND rf.form_id = inFormID
      WHERE q.id IN (SELECT qr.query_id FROM tb_qrole_registry qr
                          WHERE qr.query_id = q.id
                            AND qr.role_id IN (SELECT ID FROM rol))
        AND q.is_report = 1
  ) LOOP
    Rec.id := idx.id;
    Rec.query_name := idx.query_name;
    Rec.query_descr := idx.query_descr;
    Rec.ord := idx.ord;
    PIPE ROW(Rec);
  END LOOP;
END;

FUNCTION DescribeColumns(inSQL IN CLOB) RETURN tabCols PIPELINED
  IS
    c NUMBER;
    ret NUMBER;
    col_cnt INTEGER;
    col_num NUMBER;
    rec_tab DBMS_SQL.desc_tab;
    rec recCols;
BEGIN
  c := dbms_sql.open_cursor;
  dbms_sql.parse(c,'SELECT * FROM ('||CHR(10)||inSQL||CHR(10)||') WHERE ROWNUM < 1',dbms_sql.native);
  BEGIN
    dbms_sql.bind_variable_char(c,'inDate','31.12.9999');
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
  ret := dbms_sql.execute(c);
  dbms_sql.describe_columns(c,col_cnt,rec_tab);
  col_num := rec_tab.first;
  IF (col_num IS NOT NULL) THEN
    LOOP
      rec.col_num := col_num;
      rec.col_name := rec_tab(col_num).col_name;
      rec.col_type := CASE rec_tab(col_num).col_type WHEN 1 THEN 'VARCHAR2' WHEN 96 THEN 'VARCHAR2' WHEN 2 THEN 'NUMBER' WHEN 112 THEN 'CLOB' WHEN 12 THEN 'DATE' ELSE to_char(rec_tab(col_num).col_type) END;
      rec.col_len := rec_tab(col_num).col_max_len;
      PIPE ROW(rec);
      col_num := rec_tab.next(col_num);
      EXIT WHEN (col_num IS NULL);
    END LOOP;
  END IF;
  dbms_sql.close_cursor(c);
END;

PROCEDURE pr_log_write(inUnit IN VARCHAR2,inMessage IN VARCHAR2)
  IS
    vBuff VARCHAR2(32700);
    PRAGMA AUTONOMOUS_TRANSACTION;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
   vBuff :=
   'BEGIN'||CHR(10)||
   'INSERT INTO '||lower(vOwner)||'.tb_signs_log (dat, unit, message) VALUES (SYSDATE,:1,:2);'||CHR(10)||
   'END;';
   EXECUTE IMMEDIATE vBuff USING IN inUnit, IN inMessage;
   COMMIT;
END pr_log_write;

PROCEDURE send_message_about_project(inProjID NUMBER,inTheme VARCHAR2,inMessage CLOB)
  IS
    vOwner VARCHAR2(256) := GetVarValue('vOwner');
    v_list_recipients       VARCHAR2(1000);
    v_project_owner_email   VARCHAR2(100);
    v_body_message CLOB;
    v_headers VARCHAR2(32700);
    v_project_name VARCHAR2(1000);
    vTheme VARCHAR2(32700);
    --
    vMes VARCHAR2(32700);
    errNoProj EXCEPTION;
    errNoRecipients EXCEPTION;
BEGIN
  BEGIN
    SELECT project_name,project_main_contact_mail
      INTO v_project_name,v_project_owner_email
      FROM fv_notification_proj_list
      WHERE proj_id = inProjID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errNoProj;
  END;
  
  BEGIN
    SELECT LISTAGG(user_email,';') WITHIN GROUP (ORDER BY subscribe_id)
      INTO v_list_recipients
      FROM (
        SELECT user_email
              ,MAX(subscribe_id) AS subscribe_id   
            FROM fv_notification_subscribers
            WHERE project_id = inProjID
              AND is_active = 1
        GROUP BY user_email
    );
    /*SELECT LISTAGG(user_email,';') WITHIN GROUP (ORDER BY subscribe_id)
      INTO v_list_recipients
      FROM fv_notification_subscribers
      WHERE project_id = inProjID
        AND is_active = 1;*/
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errNoRecipients;
  END;
  vTheme := 'Оповещение от проекта '||v_project_name||' ('||inTheme||')';
  v_headers := '<h3>' || vTheme ||'</h3>';
  v_body_message :=
    v_headers||'<p><b>Текст сообщения:</b></p><p>'||inMessage||'</p>'||
    '<p>Вы получили это сообщение т.к. подписаны на рассылку уведомлений в системе нотификаций.<p>'||GetVarValue('vStopNotification');
                 
  INSERT INTO iskra.atb_emails( mail_user, mail_to, mail_to_copy, mail_subject, mail_body, mail_status ) 
     VALUES ('single_base_sales_kb'
            ,v_project_owner_email
            ,v_list_recipients
            ,vTheme
            ,v_body_message
            ,1);
  COMMIT;  
EXCEPTION
  WHEN errNoProj THEN 
    vMes := 'ERROR :: Проекта с номером '||inProjID ||' не существует в системе нотификаций';
    pr_log_write(LOWER(vOWner)||'.pkg_etl_signs.send_message_about_project',vMes);
  WHEN errNoRecipients THEN 
    vMes := 'ERROR :: Отсутствует список получателей рассылки от проекта ('||inProjID||') "'||v_project_name||'"';
    pr_log_write(LOWER(vOWner)||'.pkg_etl_signs.send_message_about_project',vMes);
  WHEN OTHERS THEN
    vMes := 'ERROR :: '||SQLERRM;
    pr_log_write(LOWER(vOWner)||'.pkg_etl_signs.send_message_about_project',vMes);
END send_message_about_project;

PROCEDURE SendMainLogs(inOPID NUMBER)
  IS
    vPars VARCHAR2(32700) := 'SELECT pnam,CASE ptyp WHEN ''S'' THEN ''Строка'' WHEN ''D'' THEN ''Дата'' ELSE ''Число'' END AS ptyp,pval FROM lg_pars WHERE opid = '||inOPID||' AND stid = 0';
    vBuff VARCHAR2(32700) :=
    'SELECT CASE WHEN to_char(l.mstx) LIKE ''%ERROR ::%'' THEN ''<span style="color: red">''||to_char(p.stid)||''</span>'' ELSE to_char(p.stid) END AS stid
           ,CASE WHEN to_char(l.mstx) LIKE ''%ERROR ::%'' THEN ''<span style="color: red">''||to_char(p.snam)||''</span>'' ELSE to_char(p.snam) END AS snam
           ,CASE WHEN to_char(l.mstx) LIKE ''%ERROR ::%'' THEN ''<span style="color: red">''||to_char(p.stdt,''DD.MM.RRRR HH24:MI:SS'')||''</span>'' ELSE to_char(p.stdt,''DD.MM.RRRR HH24:MI:SS'') END AS stdt
           ,CASE WHEN to_char(l.mstx) LIKE ''%ERROR ::%'' THEN ''<span style="color: red">''||to_char(p.eddt,''DD.MM.RRRR HH24:MI:SS'')||''</span>'' ELSE to_char(p.eddt,''DD.MM.RRRR HH24:MI:SS'') END AS eddt
           ,CASE WHEN to_char(l.mstx) LIKE ''%ERROR ::%'' THEN ''<span style="color: red">''||'||LOWER(GetVarValue('vOwner'))||'.pkg_etl_signs.get_ti_as_hms(p.eddt - p.stdt)||''</span>'' ELSE '||LOWER(GetVarValue('vOwner'))||'.pkg_etl_signs.get_ti_as_hms(p.eddt - p.stdt) END AS ti
           ,CASE WHEN to_char(l.mstx) LIKE ''%ERROR ::%'' THEN ''<span style="color: red">''||to_char(l.mstx)||''</span>'' ELSE to_char(l.mstx) END AS mstx
       FROM lg_phase p
            LEFT JOIN lg_logs l
              ON l.stid = p.stid
       WHERE p.opid = '||inOPID;
    vName VARCHAR2(1000);
BEGIN
  SELECT opnm INTO vName FROM lg_oper WHERE opid = inOPID;
  send_message_about_project(to_number(GetVarValue('vProjectID')), vName||': OPID = '||inOPID,
     SQLasHTML(vPars,'PNAM#!#PTYP#!#PVAL','Наименование параметра:#!#Тип параметра:#!#Значение параметра:',NULL,FALSE,'<span class="TabHeaderText">Значения параметров:</span>')||'<br/>'||
    SQLasHTML(vBuff,'STID#!#SNAM#!#STDT#!#EDDT#!#TI#!#MSTX'
                    ,'Ид этапа:#!#Наименование этапа:#!#Время старта:#!#Время окончания:#!#Время выполнения:#!#Результат:',' ',FALSE,'<span class="TabHeaderText">Результаты логирования:</span>'));
  
  --dbms_output.put_line(SQLasHTML(vBuff,'STID#!#SNAM#!#STDT#!#EDDT#!#TI#!#MSTX','Ид этапа:#!#Наименование этапа:#!#Время старта:#!#Время окончания:#!#Время выполнения:#!#Результат:'));
EXCEPTION WHEN OTHERS THEN
  NULL;
  send_message_about_project(to_number(GetVarValue('vProjectID')), 'OPID = '||inOPID, SQLERRM);
  
  --dbms_output.put_line(SQLERRM);
END SendMainLogs;

PROCEDURE pr_stat_write(inSignName IN VARCHAR2,inAnltCode IN VARCHAR2,inSec NUMBER,inAction VARCHAR2)
  IS
    vBuff VARCHAR2(32700);
    PRAGMA AUTONOMOUS_TRANSACTION;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
   vBuff :=
   'BEGIN'||CHR(10)||
   'INSERT INTO '||lower(vOwner)||'.tb_signs_calc_stat (sign_name,anlt_code,sec,action) VALUES (:1,:2,:3,:4);'||CHR(10)||
   'END;';
   EXECUTE IMMEDIATE vBuff USING IN inSignName,IN inAnltCode,IN inSec,IN inAction;
   COMMIT;
END pr_stat_write;

FUNCTION get_ti_as_hms (inInterval IN NUMBER /*интервал в днях*/) RETURN VARCHAR2
  IS
BEGIN
  RETURN LPAD(TO_CHAR(TRUNC(inInterval*24*60*60/3600)),3,' ')||'h '||LPAD(TO_CHAR(TRUNC(MOD(inInterval*24*60*60,3600)/60)),2,' ')||'m '||LPAD(TO_CHAR(ROUND(MOD(MOD(inInterval*24*60*60,3600),60),0)),2,' ')||'s';
END get_ti_as_hms;

FUNCTION parse_str(inStr VARCHAR2,inSeparator IN VARCHAR2) RETURN tabStr PIPELINED
  IS
    rec recStr;
    vExpr VARCHAR2(4000) := inSeparator||inStr||inSeparator;
    vPartCount INTEGER := (LENGTH(inStr) - LENGTH(REPLACE(inStr,inSeparator,'')))/LENGTH(inSeparator) + 1;
BEGIN
  FOR idx IN (
    SELECT LEVEL AS ord
          ,SUBSTR(
             SUBSTR(vExpr
                   ,INSTR(vExpr,inSeparator,1,LEVEL) + LENGTH(inSeparator)
                   ,LENGTH(vExpr))
                 ,1,INSTR(SUBSTR(vExpr
                   ,INSTR(vExpr,inSeparator,1,LEVEL) + LENGTH(inSeparator)
                   ,LENGTH(vExpr)),inSeparator,1,1) - 1) AS a
      FROM dual
    CONNECT BY LEVEL <= vPartCount
  ) LOOP
    rec.ord := idx.ord;
    rec.Str := idx.a;
    PIPE ROW(rec);
  END LOOP;
END parse_str;

FUNCTION split_clob(inCLOB IN CLOB,inStrLen IN INTEGER) RETURN tabStr PIPELINED
  IS
    rec recStr;
    vLen NUMBER;
BEGIN
  vLen := CEIL(dbms_lob.getlength(inCLOB)/inStrLen);
  FOR idx IN (
    SELECT ROWNUM AS ord,dbms_lob.substr(inCLOB,inStrLen,(LEVEL-1)*inStrLen+1) AS str FROM dual CONNECT BY LEVEL <= vLen
  ) LOOP
    rec.ord := idx.ord;
    rec.str := idx.str;
    PIPE ROW(rec);
  END LOOP;
END split_clob;

FUNCTION gather_clob(inTable IN SYS_REFCURSOR) RETURN CLOB
  IS
    ord NUMBER;
    str VARCHAR2(4000);
    res CLOB;
BEGIN
  LOOP
    FETCH inTable INTO ord,str;
    EXIT WHEN inTable%NOTFOUND;
    BEGIN
      res := res||str;
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;
  RETURN res;
END gather_clob;

FUNCTION isEqual(n1 IN NUMBER,n2 IN NUMBER) RETURN NUMBER
  IS
BEGIN
  IF n1 = n2 OR n1 IS NULL AND n2 IS NULL THEN RETURN 1; ELSE RETURN 0; END IF;
END isEqual;

FUNCTION isEqual(v1 IN VARCHAR2,v2 IN VARCHAR2) RETURN NUMBER
  IS
BEGIN
  IF v1 = v2 OR v1 IS NULL AND v2 IS NULL THEN RETURN 1; ELSE RETURN 0; END IF;
END isEqual;

FUNCTION isEqual(d1 IN DATE,d2 IN DATE) RETURN NUMBER
  IS
BEGIN
  IF d1 = d2 OR d1 IS NULL AND d2 IS NULL THEN RETURN 1; ELSE RETURN 0; END IF;
END isEqual;

FUNCTION isEqual(c1 IN CLOB,c2 IN CLOB) RETURN NUMBER
  IS
BEGIN
  IF dbms_lob.compare(c1,c2) = 0 THEN RETURN 1; ELSE RETURN 0; END IF;
END isEqual;

FUNCTION DBLinkReady(inDBLinkName VARCHAR2) RETURN BOOLEAN
  IS
    vRes NUMBER := 0;
BEGIN
  EXECUTE IMMEDIATE 'SELECT 1 FROM dual@'||inDBLinkName INTO vRes;
  RETURN TRUE;
EXCEPTION WHEN OTHERS THEN
  RETURN FALSE;
END DBLinkReady;

FUNCTION TableNotEmpty(inTbl VARCHAR2) RETURN BOOLEAN
  IS
    vBuff VARCHAR2(32700) := 'SELECT COUNT(1) FROM '||CHR(10)||inTbl||CHR(10)||' WHERE rownum <= 1';
    vRes INTEGER;
BEGIN
  EXECUTE IMMEDIATE vBuff  INTO vRes;
  IF vRes > 0 THEN RETURN TRUE; ELSE RETURN FALSE; END IF;
EXCEPTION WHEN OTHERS THEN
  pr_log_write(LOWER(GetVarValue('vOWner'))||'.pkg_etl_signs.TableNotEmpty','ERROR :: '||SQLERRM);
  RETURN FALSE;
END;

FUNCTION GetConditionResult(inCondition IN CLOB,inParams VARCHAR2 DEFAULT NULL,inComment VARCHAR2 DEFAULT NULL) RETURN NUMBER
  IS
    vResult NUMBER;
    vCond CLOB;
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
BEGIN
  IF inCondition IS NULL THEN
    RETURN 1;
  ELSE
    vCond := 'DECLARE vRes BOOLEAN; BEGIN vRes := '||inCondition||'; IF vRes THEN :1 := 1; ELSE :1 := 0; END IF; END;';
    IF inParams IS NOT NULL THEN
      FOR idx IN (
        SELECT ord
              ,SUBSTR(str,1,INSTR(str,' ',1,1) - 1) AS pname
              ,SUBSTR(str,INSTR(str,' ',1,1) + 1,INSTR(str,' ',1,2) - INSTR(str,' ',1,1) - 1) AS ptype
              ,SUBSTR(str,INSTR(str,' ',1,2) + 1) AS pval
          FROM TABLE(pkg_etl_signs.parse_str(inParams,'#!#'))
      ) LOOP
        vCond := REPLACE(vCond,':'||idx.pname,CASE idx.ptype WHEN 'DATE' THEN 'to_date('''||idx.pval||''',''DD.MM.RRRR HH24:MI:SS'')' WHEN 'VARCHAR2' THEN 'q''['||idx.pval||']''' ELSE idx.pval END);
      END LOOP;
    END IF;      
    BEGIN
      EXECUTE IMMEDIATE vCond USING OUT vResult;
      --dbms_output.put_line(vCond);
    EXCEPTION WHEN OTHERS THEN
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.GetConditionResult','ERROR :: Comment: '||inComment||' :: Возможно не заданы значения входящих параметров: '||CHR(10)||'-------------------'||CHR(10)||vCond);
    END;
    --dbms_output.put_line('DECLARE vRes BOOLEAN; BEGIN vRes := '||vCond||'; IF vRes THEN :1 := 1; ELSE :1 := 0; END IF; END;');
    --dbms_output.put_line(sign_name)
    RETURN vResult;
    --RETURN SQLERRM;
  END IF;
END GetConditionResult;

FUNCTION GetGroupIdByName(inGroupName IN VARCHAR2) RETURN NUMBER
  IS
    vRes NUMBER;
BEGIN
  SELECT group_id INTO vRes FROM tb_signs_group WHERE group_name = inGroupName;
  RETURN vRes;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RETURN NULL;  
END GetGroupIdByName;

PROCEDURE mass_load_parallel_by_date_pe(inBeg IN DATE, inEnd IN DATE, inUnit IN VARCHAR2 DEFAULT NULL
  ,inParams IN VARCHAR2 DEFAULT NULL)
  IS
    vMes VARCHAR2(2000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vPLev NUMBER;
    vTry NUMBER;
    vStatus NUMBER;
    vTask VARCHAR2(255) := dbms_parallel_execute.generate_task_name;
    vParams VARCHAR2(32700);
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
  BEGIN
    vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_date_pe" started.';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_date_pe',vMes);

    -- Формирование строки доп. параметров
    IF inParams IS NOT NULL THEN
      FOR idx IN (
        SELECT SUBSTR(str,1,INSTR(str,' ',1,1)-1) AS param_type
              ,SUBSTR(str,INSTR(str,' ',1,1)+1) AS param_value
        FROM TABLE(parse_str(inParams,'::'))
      ) LOOP
        vParams := vParams||
          CASE idx.param_type
            WHEN 'VARCHAR2' THEN ''''''||idx.param_value||''''''
            WHEN 'DATE' THEN 'to_date('''''||idx.param_value||''''',''''DD.MM.YYYY'''')'
          ELSE idx.param_value END||',';
      END LOOP;
      vParams := SUBSTR(vParams,1,LENGTH(vParams) - 1)  ;
    END IF;
    --Вычисление количества потоков
    SELECT TRUNC(to_number(VALUE)/5*4) INTO vPLev FROM v$parameter WHERE NAME = 'job_queue_processes';

    -- Создание временной таблицы
    EXECUTE IMMEDIATE 'CREATE TABLE '||lower(vOwner)||'.tmp_'||vTask||' (id NUMBER,exec_sql VARCHAR2(2000))';
    FOR idx IN (SELECT rownum AS id
               ,'begin '||inUnit||'(to_date('''''||to_char(inBeg+rownum-1,'DD.MM.YYYY')||''''',''''DD.MM.YYYY''''),to_date('''''||to_char(inBeg+rownum-1,'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')'||NVL2(vParams,','||vParams,'')||'); end;' AS vSQL
      FROM dual CONNECT BY ROWNUM <= inEnd - inBeg + 1)
    LOOP
      EXECUTE IMMEDIATE
      --dbms_output.put_line(
      'INSERT INTO '||lower(vOwner)||'.tmp_'||vTask||' (id,exec_sql)
        VALUES ('||idx.id||','''||idx.vsql||''')'
      --)
      ;
      --dbms_output.put_line(idx.vSQL);
    END LOOP;


      --Наименование задачи
      DBMS_PARALLEL_EXECUTE.CREATE_TASK(task_name => vTask);

      --Раскладка по потокам
      DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_SQL
        (task_name => vTask
        ,sql_stmt =>'SELECT id,id FROM '||lower(vOwner)||'.tmp_'||vTask||' ORDER BY 1'
        ,by_rowid => FALSE
        );
      --Запуск задачи на выполнение
      DBMS_PARALLEL_EXECUTE.RUN_TASK (task_name => vTask,
         sql_stmt => 'declare
                        vSQL VARCHAR2(4000);
                      begin
                         SELECT exec_sql INTO vSQL
                           FROM '||lower(vOwner)||'.tmp_'||vTask||'
                           WHERE id = :start_id AND id = :end_id
                         ;
                        execute immediate vSQL;
                        commit;
                      end;'
         ,language_flag => DBMS_SQL.NATIVE
         , parallel_level => vPLev );

      --Финишный контроль и удаление задачи
      vTry := 0;
      vStatus := DBMS_PARALLEL_EXECUTE.task_status(vTask);

      WHILE(vTry < 2 and vStatus != DBMS_PARALLEL_EXECUTE.FINISHED)
      LOOP
        vTry := vTry + 1;
        DBMS_PARALLEL_EXECUTE.resume_task(vTask);
        vStatus := DBMS_PARALLEL_EXECUTE.task_status(vTask);
      END LOOP;

      DBMS_PARALLEL_EXECUTE.drop_task(vTask);

      -- Удаление временной таблицы
      EXECUTE IMMEDIATE 'DROP TABLE '||lower(vOwner)||'.tmp_'||vTask;

    vEndTime := SYSDATE;
    vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_date_pe" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully.';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_date_pe',vMes);
  EXCEPTION WHEN OTHERS THEN
    DBMS_PARALLEL_EXECUTE.drop_task(vTask);
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_date_pe',vMes);
    vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_date_pe" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors.';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_date_pe',vMes);
  END mass_load_parallel_by_date_pe;

PROCEDURE mass_load_parallel_by_month (inBegDate IN DATE, inEndDate IN DATE, inProcedure IN VARCHAR2
  ,inParams VARCHAR2 DEFAULT NULL)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  FOR idx IN (SELECT MIN(TRUNC(InEndDate,'DD') - ROWNUM +1) min_dt
                    ,MAX(TRUNC(InEndDate,'DD') - ROWNUM +1) max_dt
                    ,'
                      BEGIN
                        '||lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_date_pe(to_date('''||TO_CHAR(MIN(TRUNC(InEndDate,'DD') - ROWNUM +1),'DD.MM.YYYY')||''',''DD.MM.YYYY'')
                                                           ,to_date('''||TO_CHAR(MAX(TRUNC(InEndDate,'DD') - ROWNUM +1),'DD.MM.YYYY')||''',''DD.MM.YYYY'')
                                                           ,'''||inProcedure||''','''||inParams||''');
                      END;
                    ' as exec_sql
                  FROM DUAL CONNECT BY ROWNUM < TRUNC(InEndDate,'DD') - TRUNC(inBegDate,'DD') + 2
                  GROUP BY TRUNC(TRUNC(InEndDate,'DD')- ROWNUM +1,'MM')
              ORDER BY 1
             )
  LOOP
    EXECUTE IMMEDIATE idx.exec_sql;
  END LOOP;
END mass_load_parallel_by_month;

PROCEDURE mass_load_parallel_by_ydate_pe
  (inBegDate IN DATE, inEndDate IN DATE, inUnit IN VARCHAR2
  ,inParams IN VARCHAR2 DEFAULT NULL
  ,inLastDay BOOLEAN DEFAULT TRUE
  ,inMonthlyDay VARCHAR2 DEFAULT NULL)
  IS
    vMes VARCHAR2(2000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vPLev NUMBER;
    vTry NUMBER;
    vStatus NUMBER;
    vTask VARCHAR2(255) := dbms_parallel_execute.generate_task_name;
    vParams VARCHAR2(4000);
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_ydate_pe" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_ydate_pe',vMes);

  -- Формирование строки доп. параметров
  IF inParams IS NOT NULL THEN
    FOR idx IN (
      SELECT SUBSTR(str,1,INSTR(str,' ',1,1)-1) AS param_type
            ,SUBSTR(str,INSTR(str,' ',1,1)+1) AS param_value
      FROM TABLE(parse_str(inParams,'::'))
    ) LOOP
      vParams := vParams||
        CASE idx.param_type
          WHEN 'VARCHAR2' THEN ''''''||idx.param_value||''''''
          WHEN 'DATE' THEN 'to_date('''''||idx.param_value||''''',''''DD.MM.YYYY'''')'
        ELSE idx.param_value END||',';
    END LOOP;
    vParams := SUBSTR(vParams,1,LENGTH(vParams) - 1)  ;
  END IF;

  --Создание временной таблицы
  EXECUTE IMMEDIATE 'CREATE TABLE '||lower(vOwner)||'.tmp_'||vTask||' (id NUMBER,exec_sql VARCHAR2(2000))';

  IF inLastDay AND NVL(to_number(inMonthlyDay, 'FM99', 'nls_numeric_characters='', '''),0) = 0 THEN
    FOR idx IN (SELECT ROWNUM AS ID
                      ,'BEGIN
                       '||inUnit||'(to_date('''''||TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(InEndDate,'MM'),-ROWNUM + 1)),'DD.MM.YYYY')||''''',''''DD.MM.YYYY''''),to_date('''''||TO_CHAR(LAST_DAY(ADD_MONTHS(TRUNC(InEndDate,'MM'),-ROWNUM + 1)),'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')'||NVL2(vParams,','||vParams,'')||'); END;' as exec_sql
                  FROM DUAL CONNECT BY ROWNUM <= MONTHS_BETWEEN(TRUNC(InEndDate,'MM'),TRUNC(inBegDate,'MM')) + 1
                ORDER BY 1
               )
    LOOP
      EXECUTE IMMEDIATE
      --dbms_output.put_line(
      'INSERT INTO '||lower(vOwner)||'.tmp_'||vTask||' (id,exec_sql)
        VALUES ('||idx.id||','''||idx.exec_sql||''')'
      --)
      ;
    END LOOP;
  ELSIF NOT inLastDay AND NVL(to_number(inMonthlyDay, 'FM99', 'nls_numeric_characters='', '''),0) = 0 THEN
    FOR idx IN (SELECT ROWNUM AS ID
                      ,'BEGIN
                         '||inUnit||'(to_date('''''||TO_CHAR(ADD_MONTHS(TRUNC(InEndDate,'MM'),-ROWNUM + 1),'DD.MM.YYYY')||''''',''''DD.MM.YYYY''''),to_date('''''||TO_CHAR(ADD_MONTHS(TRUNC(InEndDate,'MM'),-ROWNUM + 1),'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')'||NVL2(vParams,','||vParams,'')||'); END;' AS exec_sql
                    FROM DUAL CONNECT BY ROWNUM <= MONTHS_BETWEEN(TRUNC(InEndDate,'MM'),TRUNC(inBegDate,'MM')) + 1
                ORDER BY 1
               )
    LOOP
      EXECUTE IMMEDIATE
      --dbms_output.put_line(
      'INSERT INTO '||lower(vOwner)||'.tmp_'||vTask||' (id,exec_sql)
        VALUES ('||idx.id||','''||idx.exec_sql||''')'
      --)
      ;
    END LOOP;
  ELSE
    FOR idx IN (SELECT ROWNUM AS ID
                      ,CASE WHEN EXTRACT(MONTH FROM ADD_MONTHS(TRUNC(InEndDate,'MM'),-ROWNUM + 1)) = EXTRACT(MONTH FROM ADD_MONTHS(TRUNC(InEndDate,'MM'),-ROWNUM + 1) + NVL(to_number(inMonthlyDay, 'FM99', 'nls_numeric_characters='', '''),0) - 1) THEN
                         'BEGIN
                         '||inUnit||'(to_date('''''||TO_CHAR(ADD_MONTHS(TRUNC(InEndDate,'MM'),-ROWNUM + 1) + NVL(to_number(inMonthlyDay, 'FM99', 'nls_numeric_characters='', '''),0) - 1,'DD.MM.YYYY')||''''',''''DD.MM.YYYY''''),to_date('''''||TO_CHAR(ADD_MONTHS(TRUNC(InEndDate,'MM'),-ROWNUM + 1) + NVL(to_number(inMonthlyDay, 'FM99', 'nls_numeric_characters='', '''),0) - 1,'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')'||NVL2(vParams,','||vParams,'')||'); END;'
                       ELSE 'BEGIN '||lower(vOwner)||'.pkg_etl_signs.pr_log_write('''''||inUnit||''''',''''INFORMATION :: "'||inMonthlyDay||'.'||TRIM(to_char(EXTRACT(MONTH FROM ADD_MONTHS(TRUNC(InEndDate,'MM'),-ROWNUM + 1)),'00')||'.'||EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(InEndDate,'MM'),-ROWNUM + 1)))||'" - дата отсутствует в указанном месяце. Расчет не требуется''''); END;'
                       END AS exec_sql
                    FROM DUAL CONNECT BY ROWNUM <= MONTHS_BETWEEN(TRUNC(InEndDate,'MM'),TRUNC(inBegDate,'MM')) + 1
                ORDER BY 1
               )
    LOOP
      BEGIN
      vMes :=
      --dbms_output.put_line(
      'INSERT INTO '||lower(vOwner)||'.tmp_'||vTask||' (id,exec_sql)
        VALUES ('||idx.id||','''||idx.exec_sql||''')'
      --)
      ;
      EXECUTE IMMEDIATE vMes;
      EXCEPTION WHEN OTHERS THEN
        pr_log_write(inUnit,SQLERRM||Chr(10)||vMes);
      END;
    END LOOP;
  END IF;

  --Вычисление количества потоков
  SELECT TRUNC(to_number(VALUE)/5*4) INTO vPLev FROM v$parameter WHERE NAME = 'job_queue_processes';

  --Наименование задачи
  DBMS_PARALLEL_EXECUTE.CREATE_TASK(task_name => vTask);
  --Раскладка по потокам
  DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_SQL
    (task_name => vTask
    ,sql_stmt =>'SELECT id,id FROM '||lower(vOwner)||'.tmp_'||vTask||' ORDER BY 1'
    ,by_rowid => FALSE
    );
  --Запуск задачи на выполнение
  DBMS_PARALLEL_EXECUTE.RUN_TASK (task_name => vTask,
     sql_stmt => 'declare
                    vSQL VARCHAR2(4000);
                  begin
                     SELECT exec_sql INTO vSQL
                       FROM '||lower(vOwner)||'.tmp_'||vTask||'
                       WHERE id = :start_id AND id = :end_id
                     ;
                    execute immediate vSQL;
                    commit;
                  end;'
     ,language_flag => DBMS_SQL.NATIVE
     , parallel_level => vPLev );

  --Финишный контроль и удаление задачи
  vTry := 0;
  vStatus := DBMS_PARALLEL_EXECUTE.task_status(vTask);

  WHILE(vTry < 2 and vStatus != DBMS_PARALLEL_EXECUTE.FINISHED)
  LOOP
    vTry := vTry + 1;
    DBMS_PARALLEL_EXECUTE.resume_task(vTask);
    vStatus := DBMS_PARALLEL_EXECUTE.task_status(vTask);
  END LOOP;

  DBMS_PARALLEL_EXECUTE.drop_task(vTask);

  -- Удаление временной таблицы
  EXECUTE IMMEDIATE 'DROP TABLE '||lower(vOwner)||'.tmp_'||vTask;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_ydate_pe" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_ydate_pe',vMes);
EXCEPTION WHEN OTHERS THEN
  DBMS_PARALLEL_EXECUTE.drop_task(vTask);
  vEndTime := SYSDATE;
  vMes := 'ERROR :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_ydate_pe',vMes);
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_ydate_pe" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load_parallel_by_ydate_pe',vMes);
END mass_load_parallel_by_ydate_pe;

PROCEDURE mass_load_parallel_by_year
  (inBegDate IN DATE, inEndDate IN DATE, inProcedure IN VARCHAR2
  ,inParams VARCHAR2 DEFAULT NULL
  ,inLastDay BOOLEAN DEFAULT TRUE
  ,inMonthlyDay VARCHAR2 DEFAULT NULL
  ,inYearParallel BOOLEAN DEFAULT FALSE
  ,inHeadJobName IN VARCHAR2 DEFAULT NULL)
  IS
    vLstDay VARCHAR2(5);
    vTask VARCHAR2(256);
    vTry NUMBER;
    vStatus NUMBER;
    vPLev NUMBER;
    vSQL_stmt VARCHAR2(32700);
BEGIN
  IF inLastDay THEN vLstDay := 'TRUE'; ELSE vLstDay := 'FALSE'; END IF;
  IF inYearParallel THEN
    --Наименование задачи
    vTask := dbms_parallel_execute.generate_task_name;
    --Создание задачи
    DBMS_PARALLEL_EXECUTE.CREATE_TASK(task_name => vTask);

   -- Вычисление количества потоков
    SELECT TRUNC(to_number(VALUE)/5*4) INTO vPLev FROM v$parameter WHERE NAME = 'job_queue_processes';

    --Раскладка по потокам
    DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_SQL
      (task_name => vTask
      ,sql_stmt =>
        'SELECT ROWNUM AS ID,ROWNUM as ID FROM (
          SELECT EXTRACT(YEAR FROM to_date('''||to_char(inEndDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') - ROWNUM + 1) as y
            FROM dual CONNECT BY ROWNUM <= to_date('''||to_char(inEndDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') - to_date('''||to_char(inBegDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') + 1
          GROUP BY EXTRACT(YEAR FROM to_date('''||to_char(inEndDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') - ROWNUM + 1)
          ORDER BY 1
        )'
      ,by_rowid => FALSE
      );

     vSql_stmt := 'declare
                    vSQL VARCHAR2(4000);
                  begin
                    WITH
                      y as (
                        SELECT ROWNUM AS ID,y_beg,y_end FROM (
                          SELECT MIN(to_date('''||to_char(inEndDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') - ROWNUM + 1) AS y_beg
                                ,MAX(to_date('''||to_char(inEndDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') - ROWNUM + 1) AS y_end
                            FROM dual CONNECT BY ROWNUM <= to_date('''||to_char(inEndDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') - to_date('''||to_char(inBegDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') + 1
                          GROUP BY EXTRACT(YEAR FROM to_date('''||to_char(inEndDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') - ROWNUM + 1)
                          ORDER BY 1
                        )
                      )
                     SELECT ''
                      BEGIN
                        mass_load_parallel_by_ydate_pe(to_date(''''''||to_char(y.y_beg,''DD.MM.YYYY'')||'''''',''''DD.MM.YYYY'''')
                                                             ,to_date(''''''||to_char(y.y_end,''DD.MM.YYYY'')||'''''',''''DD.MM.YYYY'''')
                                                             ,'''''||inProcedure||'''''
                                                             ,'||CASE WHEN inParams IS NOT NULL THEN ''''''||inParams||'''''' ELSE 'NULL' END||'
                                                             ,'||vLstDay||CASE WHEN inMonthlyDay IS NOT NULL THEN ','''''||inMonthlyDay||'''''' ELSE NULL END||'
                                                             ,'''''||inHeadJobName||''''');
                      END;
                    '' as exec_sql
                       INTO vSQL
                       FROM y
                       WHERE id = :start_id AND id = :end_id
                     ;
                    execute immediate vSQL;
                    commit;
                  end;';

    --Запуск задачи на выполнение
    DBMS_PARALLEL_EXECUTE.RUN_TASK (task_name => vTask
       ,sql_stmt => vSql_stmt
       ,language_flag => DBMS_SQL.NATIVE
       , parallel_level => vPLev );

    --Финишный контроль и удаление задачи
    vTry := 0;
    vStatus := DBMS_PARALLEL_EXECUTE.task_status(vTask);

    WHILE(vTry < 2 and vStatus != DBMS_PARALLEL_EXECUTE.FINISHED)
    LOOP
      vTry := vTry + 1;
      DBMS_PARALLEL_EXECUTE.resume_task(vTask);
      vStatus := DBMS_PARALLEL_EXECUTE.task_status(vTask);
    END LOOP;

    DBMS_PARALLEL_EXECUTE.drop_task(vTask);

  ELSE
    FOR idx IN (SELECT GREATEST(TRUNC(add_months(inEndDate,-(ROWNUM-1)*12),'YYYY'),inBegDate) AS min_dt
                      ,LEAST(add_months(TRUNC(add_months(inEndDate,-(ROWNUM-1)*12),'YYYY'),12) - 1,inEndDate) AS max_dt
                      ,'BEGIN'||Chr(10)||
                       '   mass_load_parallel_by_ydate_pe(to_date('''||TO_CHAR(GREATEST(TRUNC(add_months(inEndDate,-(ROWNUM-1)*12),'YYYY'),inBegDate),'DD.MM.YYYY')||''',''DD.MM.YYYY'')'||Chr(10)||
                       '                                       ,to_date('''||TO_CHAR(LEAST(add_months(TRUNC(add_months(inEndDate,-(ROWNUM-1)*12),'YYYY'),12) - 1,inEndDate),'DD.MM.YYYY')||''',''DD.MM.YYYY'')'||Chr(10)||
                       '                                       ,'''||inProcedure||''''||Chr(10)||
                       '                                       ,'||CASE WHEN inParams IS NOT NULL THEN ''''||inParams||'''' ELSE 'NULL' END||Chr(10)||
                       '                                       ,'||vLstDay||NVL2(inMonthlyDay,','''||inMonthlyDay||'''',NULL)||'
                                                               ,'''''||inHeadJobName||''''');'||Chr(10)||
                       'END;' AS exec_sql

                    FROM DUAL CONNECT BY ROWNUM <= CEIL(MONTHS_BETWEEN(inEndDate+1,TRUNC(inBegDate,'YYYY'))/12)
                ORDER BY 1
               )
    LOOP

      EXECUTE IMMEDIATE idx.exec_sql;
    END LOOP;
  END IF;
EXCEPTION WHEN OTHERS THEN
  BEGIN DBMS_PARALLEL_EXECUTE.drop_task(vTask); EXCEPTION WHEN OTHERS THEN NULL; END;
END mass_load_parallel_by_year;

PROCEDURE MyExecute(inScript IN VARCHAR2)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  EXECUTE IMMEDIATE inScript;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.MyExecute','SUCESSFULLY :: '||inScript);
EXCEPTION WHEN OTHERS THEN
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.MyExecute',SQLERRM||CHR(10)||'------------'||CHR(10)||inScript);
END MyExecute;

FUNCTION AnyExecute(inScript IN CLOB,inParams IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2
IS
  vOwner VARCHAR2(4000) := GetVarValue('vOwner');
  vStmt CLOB;
  vParams VARCHAR2(32700);
  vDeclParams VARCHAR2(32700);
  outRes VARCHAR2(32700);
BEGIN
  IF inParams IS NOT NULL THEN
    FOR idx IN (
      SELECT str,ROWNUM AS ord FROM (
        SELECT str
          FROM TABLE(pkg_etl_signs.parse_str(inParams,'#!#'))
      )
    ) LOOP
      vDeclParams := vDeclParams||'  p'||idx.ord||' VARCHAR2(32700) := q''['||idx.str||']'';'||CHR(10);
      vParams := vParams||' IN p'||idx.ord||',';
    END LOOP;
  END IF;
  vDeclParams := vDeclParams||'  outRes VARCHAR2(32700);';
  vParams := vParams||' OUT outRes';
  vStmt := 'DECLARE'||CHR(10)||vDeclParams||CHR(10)||'BEGIN'||CHR(10)||'EXECUTE IMMEDIATE q''['||inScript||']'' USING '||vParams||';'||CHR(10)||'  :1 := outRes;'||CHR(10)||'END;';
  
  --RETURN vStmt;
  EXECUTE IMMEDIATE vStmt USING OUT outRes;
  RETURN outRes;
  --dbms_output.put_line(vStmt);
EXCEPTION WHEN OTHERS THEN
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.AnyExecute',SQLERRM||CHR(10)||'-----------------------'||CHR(10)||vStmt);
  RETURN NULL;
END AnyExecute;

PROCEDURE prepare_entity(inId IN NUMBER,outRes OUT CLOB)
  IS
    vBuff                     VARCHAR2(32700);
    vRes                      VARCHAR2(32700);
    --
    vEntityId                 NUMBER;
    vFctTableName             VARCHAR2(256);
    vHistTableName            VARCHAR2(256);
    vHistIdxName              VARCHAR2(256);
    vTmpTableName             VARCHAR2(256);
    vTmpIdxName               VARCHAR2(256);
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  -- Получение и сохранение в переменные метаданных сущности
  BEGIN
    SELECT id
          ,fct_table_name
          ,hist_table_name
          ,tmp_table_name
      INTO vEntityID
          ,vFctTableName
          ,vHistTableName
          ,vTmpTableName
      FROM tb_entity
      WHERE id = inId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20000,'Сущность ID = '||inId||' не найдена в таблице "'||lower(vOwner)||'.tb_entity"');
  END;
  -- Создание таблицы для хранения показателей по датам
  dbms_lob.createtemporary(outRes,FALSE);

  vBuff :=
  'CREATE TABLE '||lower(vOwner)||'.'||lower(vFctTableName)||' ('||CHR(10)||
  '  AS_OF_DATE DATE'||CHR(10)||
  ' ,OBJ_GID VARCHAR2(256)'||CHR(10)||
  ' ,SOURCE_SYSTEM_ID VARCHAR2(30)'||CHR(10)||
  ' ,SIGN_NAME VARCHAR2(256)'||CHR(10)||
  ' ,SIGN_VAL VARCHAR2(4000)'||CHR(10)||
  ') PARTITION BY LIST (SIGN_NAME)'||CHR(10)||
  '  SUBPARTITION BY LIST (AS_OF_DATE)'||CHR(10)||
  '  (PARTITION EMPTY_SIGN VALUES (''EMPTY_SIGN'') STORAGE(INITIAL 64K NEXT  8M) NOLOGGING'||CHR(10)||
  '     (SUBPARTITION SPEMPTY_19700101 VALUES(to_date(''01.01.1970'',''DD.MM.YYYY'')))) NOLOGGING';

  BEGIN
    EXECUTE IMMEDIATE vBuff;
    vRes := 'Table '||lower(vOwner)||'.'||lower(vFctTableName)||' created successfully';
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  EXCEPTION WHEN OTHERS THEN
    vRes := SQLERRM||CHR(10)||vBuff;
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  END;

  -- Создание таблицы для хранения показателей периодами
  vBuff :=
  'CREATE TABLE '||lower(vOwner)||'.'||lower(vHistTableName)||' ('||CHR(10)||
  '  EFFECTIVE_START DATE'||CHR(10)||
  ' ,EFFECTIVE_END DATE'||CHR(10)||
  ' ,OBJ_GID VARCHAR2(256)'||CHR(10)||
  ' ,SOURCE_SYSTEM_ID VARCHAR2(30)'||CHR(10)||
  ' ,SIGN_NAME VARCHAR2(256)'||CHR(10)||
  ' ,SIGN_VAL VARCHAR2(4000)'||CHR(10)||
  ') PARTITION BY LIST (SIGN_NAME)'||CHR(10)||
  '  (PARTITION EMPTY_SIGN VALUES (''EMPTY_SIGN'') STORAGE(INITIAL 64K NEXT 4M) NOLOGGING) NOLOGGING';

  BEGIN
    EXECUTE IMMEDIATE vBuff;
    vRes := /*outRes||*/CHR(10)||'-----------------------'||CHR(10)||'Table '||lower(vOwner)||'.'||lower(vHistTableName)||' created successfully';
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  EXCEPTION WHEN OTHERS THEN
    vRes := /*outRes||*/CHR(10)||'-----------------------'||CHR(10)||SQLERRM||CHR(10)||vBuff;
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  END;

  -- Создание промежуточной таблицы
  vBuff :=
  'CREATE TABLE '||lower(vOwner)||'.'||lower(vTmpTableName)||' ('||CHR(10)||
  '  EFFECTIVE_START DATE'||CHR(10)||
  ' ,EFFECTIVE_END DATE'||CHR(10)||
  ' ,OBJ_GID VARCHAR2(256)'||CHR(10)||
  ' ,SOURCE_SYSTEM_ID VARCHAR2(30)'||CHR(10)||
  ' ,SIGN_NAME VARCHAR2(256)'||CHR(10)||
  ' ,SIGN_VAL VARCHAR2(4000)'||CHR(10)||
  ') PARTITION BY LIST (SIGN_NAME)'||CHR(10)||
  '  SUBPARTITION BY RANGE (EFFECTIVE_END)'||CHR(10)||
  '  (PARTITION EMPTY_SIGN VALUES (''EMPTY_SIGN'') STORAGE(INITIAL 64K NEXT 4M) NOLOGGING'||CHR(10)||
  '     (SUBPARTITION SPEMPTY_POTHERS VALUES LESS THAN (MAXVALUE))) NOLOGGING';

  BEGIN
    EXECUTE IMMEDIATE vBuff;
    vRes := /*outRes||*/CHR(10)||'-----------------------'||CHR(10)||'Table '||lower(vOwner)||'.'||lower(vTmpTableName)||' created successfully';
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  EXCEPTION WHEN OTHERS THEN
    vRes := /*outRes||*/CHR(10)||'-----------------------'||CHR(10)||SQLERRM||CHR(10)||vBuff;
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  END;

  -- Создание уникальных индексов
  -- Формирование наименований индексов
  BEGIN
    SELECT 'uix_'||object_id INTO vHistIdxName
      FROM all_objects
      WHERE owner = UPPER(vOwner)
        AND object_name = UPPER(vHistTableName)
        AND object_type = 'TABLE';
  EXCEPTION WHEN NO_DATA_FOUND THEN
    vRes := /*outRes||*/CHR(10)||'-----------------------'||CHR(10)||'Объект '||lower(vOwner)||'.'||lower(vHistTableName)||' не найден'||CHR(10)||vBuff;
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  END;

  BEGIN
    SELECT 'uix_'||object_id INTO vTmpIdxName
      FROM all_objects
      WHERE owner = UPPER(vOwner)
        AND object_name = UPPER(vTmpTableName)
        AND object_type = 'TABLE';
  EXCEPTION WHEN NO_DATA_FOUND THEN
    vRes := /*outRes||*/CHR(10)||'-----------------------'||CHR(10)||'Объект '||lower(vOwner)||'.'||lower(vTmpTableName)||' не найден'||CHR(10)||vBuff;
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  END;
  -- Формирование набора ключевых колонок, входящих в индекс
  --SELECT LISTAGG(SUBSTR(Str,1,INSTR(Str,' ') - 1),',') WITHIN GROUP (ORDER BY rownum) INTO vKeyIdxColumns
  --  FROM TABLE(parse_str(vKeyColumns,','));

  -- Формирование и запуск DDL
  vBuff := 'CREATE UNIQUE INDEX '||lower(vOwner)||'.'||vHistIdxName||' ON '||lower(vHistTableName)||CHR(10)||
           '  (SIGN_NAME,OBJ_GID,SOURCE_SYSTEM_ID,EFFECTIVE_END)'||CHR(10)||
           'LOCAL COMPRESS NOLOGGING';
  BEGIN
    EXECUTE IMMEDIATE vBuff;
    vRes := /*outRes||*/CHR(10)||'-----------------------'||CHR(10)||'Unique index '||lower(vOwner)||'.'||lower(vHistIdxName)||' created successfully';
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  EXCEPTION WHEN OTHERS THEN
    vRes := /*outRes||*/CHR(10)||'-----------------------'||CHR(10)||SQLERRM||CHR(10)||vBuff;
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  END;

  vBuff := 'CREATE UNIQUE INDEX '||lower(vOwner)||'.'||vTmpIdxName||' ON '||lower(vTmpTableName)||CHR(10)||
           '  (SIGN_NAME,EFFECTIVE_END,OBJ_GID,SOURCE_SYSTEM_ID)'||CHR(10)||
           'LOCAL COMPRESS NOLOGGING';
  BEGIN
    EXECUTE IMMEDIATE vBuff;
    vRes := /*outRes||*/CHR(10)||'-----------------------'||CHR(10)||'Unique index '||lower(vOwner)||'.'||lower(vTmpIdxName)||' created successfully';
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  EXCEPTION WHEN OTHERS THEN
    vRes := /*outRes||*/CHR(10)||'-----------------------'||CHR(10)||SQLERRM||CHR(10)||vBuff;
    dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
  END;
EXCEPTION WHEN OTHERS THEN
  vRes := /*outRes||*/CHR(10)||SQLERRM;
  dbms_lob.writeappend(outRes,LENGTH(vRes),vRes);
END prepare_entity;

FUNCTION get_sign(inSign IN VARCHAR2,inDate IN DATE, inSQL IN VARCHAR2 DEFAULT NULL) RETURN TTab PIPELINED
  IS
    vSQL CLOB;
    rec TRec;
    cur INTEGER;       -- хранит идентификатор (ID) курсора
    ret INTEGER;       -- хранит возвращаемое по вызову значение
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  -- Сохранение метаданных показателя в переменные
  SELECT p.sign_sql
    INTO vSQL
    FROM tb_signs_pool p
    WHERE p.sign_name = UPPER(inSign);

  IF inSQL IS NOT NULL THEN vSQL := inSQL; END IF;

  --dbms_output.put_line(vAnltSQL);

  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur, vSQL, dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.obj_gid,256);
  dbms_sql.define_column(cur,2,rec.source_system_id,30);
  dbms_sql.define_column(cur,3,rec.sign_name,256);
  dbms_sql.define_column(cur,4,rec.sign_val,4000);

  IF inSQL IS NULL THEN
    dbms_sql.bind_variable_char(cur,'inDate',to_char(inDate,'DD.MM.YYYY'));
  END IF;

  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.obj_gid);
    dbms_sql.column_value(cur,2,rec.source_system_id);
    dbms_sql.column_value(cur,3,rec.sign_name);
    dbms_sql.column_value(cur,4,rec.sign_val);
    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.get_sign','ERROR :: "'||UPPER(inSign)||'"  - Показатель не найден в таблице "'||lower(vOwner)||'.tb_signs_pool"');
  WHEN OTHERS THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.get_sign','ERROR :: "'||UPPER(inSign)||'"  - '||SQLERRM||CHR(10)||'----------'||CHR(10)||vSQL);
END get_sign;

FUNCTION get_sign_anlt(inSign IN VARCHAR2, inDate IN DATE, inAnltCode IN VARCHAR2, inReverse NUMBER DEFAULT 0) RETURN TTab PIPELINED
  IS
    rec TRec;
    cur INTEGER;       -- хранит идентификатор (ID) курсора
    ret INTEGER;       -- хранит возвращаемое по вызову значение
    vSQL CLOB;
    vAnltSQL CLOB;
    vAnltID NUMBER;
    vBuff VARCHAR2(32700);
    vWhere VARCHAR2(32700);
    vCou INTEGER;
    --
    vFctTable VARCHAR2(256);
    vHistTable VARCHAR2(256);
    vHistFlg NUMBER;
    vReverse BOOLEAN := inReverse = 1;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
-- Сохранение метаданных показателя в переменные
SELECT a.anlt_sql,a.id,lower(vOwner)||'.'||e.fct_table_name,lower(vOwner)||'.'||e.hist_table_name,p.hist_flg
      ,(SELECT COUNT(1) FROM tb_signs_anlt_spec WHERE anlt_id = a.id) AS cou
  INTO vSQL,vAnltID,vFctTable,vHistTable,vHistFlg,vCou
  FROM tb_signs_pool p
       INNER JOIN tb_entity e ON e.id = p.entity_id
       INNER JOIN tb_signs_anlt a
          ON a.anlt_code = inAnltCode
            AND inDate BETWEEN a.effective_start AND a.effective_end
  WHERE p.sign_name = UPPER(inSign)
    AND a.archive_flg = 0;

  IF vSQL IS NULL THEN
    vSQL := 'SELECT null AS obj_gid,null AS source_system_id,null AS sign_name FROM dual';
  END IF;

  dbms_lob.createtemporary(vAnltSQL,FALSE);
  vBuff :=
  'SELECT /*+ no_index(sgn) */'||CHR(10)||
  '       '||CASE WHEN NOT(vReverse) THEN 'sgn' ELSE 'anlt' END||'.obj_gid'||CHR(10)||
  '      ,'||CASE WHEN NOT(vReverse) THEN 'sgn' ELSE 'anlt' END||'.source_system_id'||CHR(10)||
  '      ,UPPER(:inSign) AS sign_name'||CHR(10);
  dbms_lob.writeappend(vAnltSQL,LENGTH(vBuff),vBuff);

  IF vCou > 0 THEN
      vBuff :=
      '      ,CASE'||CHR(10);
      dbms_lob.writeappend(vAnltSQL,LENGTH(vBuff),vBuff);
      FOR idx IN (
        SELECT ID,anlt_spec_name,LEVEL AS lev
              --,'WHEN '||SUBSTR(REPLACE(sys_connect_by_path('('||NVL(condition,CASE WHEN inReverse = 0 THEN 'sgn' ELSE 'anlt' END||'.sign_name = '''||UPPER(inSign)||'''')||')','-=#=-'),'-=#=-',' AND '),6)||' THEN '''||anlt_spec_val||'''' AS cond
              ,NVL2(condition,'WHEN '||condition||' THEN '''||anlt_spec_val||'''',NULL) AS cond
              ,condition
          FROM tb_signs_anlt_spec
          WHERE anlt_id = vAnltID
        CONNECT BY PRIOR anlt_spec_val = parent_val
        START WITH parent_val IS NULL AND anlt_id = vAnltID
         ORDER BY connect_by_isleaf DESC,lev DESC
      ) LOOP
        IF idx.lev > 1 THEN
          vBuff := idx.cond||CHR(10);
          dbms_lob.writeappend(vAnltSQL,LENGTH(vBuff),vBuff);
        ELSE
          vWhere := CASE WHEN idx.condition IS NOT NULL THEN ' WHERE '||idx.condition ELSE NULL END||CHR(10);
        END IF;

      END LOOP;
      vBuff :=
      'ELSE NULL END AS sign_val'||CHR(10);
      dbms_lob.writeappend(vAnltSQL,LENGTH(vBuff),vBuff);
  ELSE
    vBuff := ',anlt.sign_val'||CHR(10);
    dbms_lob.writeappend(vAnltSQL,LENGTH(vBuff),vBuff);
  END IF;

  vBuff :=
  'FROM '||CASE WHEN NOT(vReverse) THEN CASE vHistFlg WHEN 1 THEN vHistTable ELSE vFctTable END||' sgn LEFT JOIN' END||' ('||CHR(10);

  dbms_lob.writeappend(vAnltSQL,LENGTH(vBuff),vBuff);
  dbms_lob.writeappend(vAnltSQL,LENGTH(vSQL),vSQL);
  IF vWhere IS NOT NULL THEN dbms_lob.writeappend(vAnltSQL,LENGTH(vWhere),vWhere); END IF;

  vBuff :=
  CHR(10)||') anlt'||CASE WHEN NOT(vReverse) THEN
  CHR(10)||'  ON anlt.sign_name = sgn.sign_name AND anlt.obj_gid = sgn.obj_gid AND anlt.source_system_id = sgn.source_system_id'||CHR(10)||
  'WHERE sgn.sign_name = UPPER(:inSign)'||CHR(10)||
  '     AND '||CASE vHistFlg WHEN 1 THEN 'to_date(:inDate,''DD.MM.YYYY'') BETWEEN sgn.effective_start AND sgn.effective_end'
               ELSE 'to_date(:inDate,''DD.MM.YYYY'') = sgn.as_of_date' END||CHR(10) END;
  dbms_lob.writeappend(vAnltSQL,LENGTH(vBuff),vBuff);
  
  --dbms_output.put_line(vAnltSQL);
  
  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur, vAnltSQL, dbms_sql.native);

  dbms_sql.define_column(cur,1,rec.obj_gid,256);
  dbms_sql.define_column(cur,2,rec.source_system_id,30);
  dbms_sql.define_column(cur,3,rec.sign_name,256);
  dbms_sql.define_column(cur,4,rec.sign_val,4000);

  dbms_sql.bind_variable_char(cur,'inDate',to_char(inDate,'DD.MM.YYYY'));
  dbms_sql.bind_variable_char(cur,'inSign',UPPER(inSign));
  
  --dbms_output.put_line(cur);
  
  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.obj_gid);
    dbms_sql.column_value(cur,2,rec.source_system_id);
    dbms_sql.column_value(cur,3,rec.sign_name);
    dbms_sql.column_value(cur,4,rec.sign_val);
    PIPE ROW(rec);
  END LOOP;
  --dbms_sql.close_cursor(cur);

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.get_sign_anlt','ERROR :: "'||UPPER(inSign)||'"  - Показатель не найден в таблице "'||lower(vOwner)||'.tb_signs_pool"');
  WHEN OTHERS THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.get_sign_anlt','ERROR :: "'||UPPER(inSign)||'"  - '||SQLERRM);

    dbms_output.put_line(SQLERRM||CHR(10)||'-----------'||CHR(10)||vAnltSQL);
END get_sign_anlt;

FUNCTION get_anlt_spec_imp(inDate IN DATE, inAnltCode IN VARCHAR2) RETURN TTabAnltSpecImp PIPELINED
  IS
    vSQL CLOB;
    rec TRecAnltSpecImp;
    cur INTEGER;       -- хранит идентификатор (ID) курсора
    ret INTEGER;       -- хранит возвращаемое по вызову значение
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  SELECT spec_import_sql
    INTO vSQL
    FROM tb_signs_anlt
    WHERE anlt_code = UPPER(inAnltCode)
      AND inDate BETWEEN effective_start AND effective_end;

  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur, vSQL, dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.val,4000);
  dbms_sql.define_column(cur,2,rec.parent_val,4000);
  dbms_sql.define_column(cur,3,rec.name,4000);
  dbms_sql.define_column(cur,4,rec.condition/*,32700*/);

  dbms_sql.bind_variable_char(cur,'inDate',to_char(inDate,'DD.MM.YYYY'));

  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.val);
    dbms_sql.column_value(cur,2,rec.parent_val);
    dbms_sql.column_value(cur,3,rec.name);
    dbms_sql.column_value(cur,4,rec.condition);
    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.get_anlt_spec_imp','ERROR :: "'||UPPER(inAnltCode)||'"  - Аналитика не найдена в таблице "'||lower(vOwner)||'.tb_signs_anlt"');
  WHEN OTHERS THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.get_anlt_spec_imp','ERROR :: "'||UPPER(inAnltCode)||'"  - '||SQLERRM||CHR(10)||'----------'||CHR(10)||vSQL);
END get_anlt_spec_imp;

FUNCTION get_sign_mass(inSign IN VARCHAR2,inDate IN DATE) RETURN TTabMass PIPELINED
  IS
    vSQL CLOB;
    rec TRecMass;
    cur INTEGER;       -- хранит идентификатор (ID) курсора
    ret INTEGER;       -- хранит возвращаемое по вызову значение
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  SELECT mass_sql INTO vSQL FROM tb_signs_pool WHERE sign_name = UPPER(inSign);

  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur, vSQL, dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.effective_start);
  dbms_sql.define_column(cur,2,rec.effective_end);
  dbms_sql.define_column(cur,3,rec.obj_gid,256);
  dbms_sql.define_column(cur,4,rec.source_system_id,30);
  dbms_sql.define_column(cur,5,rec.sign_name,256);
  dbms_sql.define_column(cur,6,rec.sign_val,4000);

  dbms_sql.bind_variable_char(cur,'inDate',to_char(inDate,'DD.MM.YYYY'));

  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.effective_start);
    dbms_sql.column_value(cur,2,rec.effective_end);
    dbms_sql.column_value(cur,3,rec.obj_gid);
    dbms_sql.column_value(cur,4,rec.source_system_id);
    dbms_sql.column_value(cur,5,rec.sign_name);
    dbms_sql.column_value(cur,6,rec.sign_val);
    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.get_sign','ERROR :: "'||UPPER(inSign)||'"  - Показатель не найден в таблице "'||lower(vOwner)||'.tb_signs_pool"');
  WHEN OTHERS THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.get_sign','ERROR :: "'||UPPER(inSign)||'"  - '||SQLERRM);
END get_sign_mass;

FUNCTION CheckSubpartition(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2) RETURN VARCHAR2
  IS
    vMes VARCHAR2(2000);
    vSPCode VARCHAR2(30);
    vHistFlg NUMBER;
    vFCTTable VARCHAR2(256);
    vHistTable VARCHAR2(256);
    vFCTATable VARCHAR2(256);
    vHistATable VARCHAR2(256);
    vBuff VARCHAR2(32700);
    vDML CLOB;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  -- Получение кода сабпартиции
  BEGIN
    SELECT p.sp_code,p.hist_flg
          ,lower(vOwner)||'.'||e.fct_table_name AS fct_table_name
          ,lower(vOwner)||'.'||e.hist_table_name AS hist_table_name
          ,lower(vOwner)||'.'||ae.fct_table_name AS fct_a_table_name
          ,lower(vOwner)||'.'||ae.hist_table_name AS hist_a_table_name
      INTO vSPCode,vHistFlg,vFCTTable,vHistTable,vFCTATable,vHistATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inEndDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;
  -- Очистка или создание
  IF vHistFlg = 0 THEN
    dbms_lob.createtemporary(vDML,FALSE);
    vBuff := 'BEGIN'||CHR(10);
    dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
    FOR idx IN (
      SELECT inEndDate - LEVEL + 1 AS dt FROM dual
      CONNECT BY LEVEL <= inEndDate - inBegDate + 1
      ORDER BY 1
    ) LOOP
      vBuff :=
      '  BEGIN'||CHR(10)||
      '    EXECUTE IMMEDIATE ''alter table '||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||' truncate subpartition '||vSPCode||'_'||to_char(idx.dt,'YYYYMMDD')||'''; '||CHR(10)||
      '    pkg_etl_signs.pr_log_write('''||lower(vOwner)||'.pkg_etl_signs.CheckSubpartition'',''SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'" altered. Partition '||inSign||': Subpartition '||vSPCode||'_'||to_char(idx.dt,'YYYYMMDD')||' truncated'');'||CHR(10)||
      '  EXCEPTION WHEN OTHERS THEN'||CHR(10)||
      '    BEGIN'||CHR(10)||
      '      EXECUTE IMMEDIATE ''alter table '||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||
      ' MODIFY PARTITION '||inSign||' ADD SUBPARTITION '||vSPCode||'_'||to_char(idx.dt,'YYYYMMDD')||' VALUES (to_date('''''||to_char(idx.dt,'DD.MM.YYYY')||''''',''''DD.MM.YYYY''''))'';'||CHR(10)||
      '      pkg_etl_signs.pr_log_write('''||lower(vOwner)||'.pkg_etl_signs.CheckSubpartition'',''SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'" altered. Partition '||inSign||': Subpartition '||vSPCode||'_'||to_char(idx.dt,'YYYYMMDD')||' added''); '||CHR(10)||
      '    EXCEPTION WHEN OTHERS THEN'||CHR(10)||
      '      EXECUTE IMMEDIATE ''alter table '||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||
      ' ADD PARTITION '||inSign||' VALUES('''''||inSign||''''') STORAGE (INITIAL 64k NEXT 4M) NOLOGGING (SUBPARTITION '||vSPCode||'_'||to_char(idx.dt,'YYYYMMDD')||' VALUES (to_date('''''||to_char(idx.dt,'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')))''; '||CHR(10)||
      '      pkg_etl_signs.pr_log_write('''||lower(vOwner)||'.pkg_etl_signs.CheckSubpartition'',''SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'" altered. Partition '||inSign||' added. Subpartition '||vSPCode||'_'||to_char(idx.dt,'YYYYMMDD')||' added'');'||CHR(10)||
      '    END;'||CHR(10)||
      '  END;'||CHR(10);
      dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
    END LOOP;
    vBuff := 'END;';
    dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
    EXECUTE IMMEDIATE vDML;
    vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" altered. Partition '||UPPER(inSign)||': Subpartitions "'||to_char(inBegDate,'YYYYMMDD')||' - '||to_char(inEndDate,'YYYYMMDD')||'" prepared';
  ELSE
    BEGIN
      EXECUTE IMMEDIATE
        'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' ADD PARTITION '||inSign||' VALUES('''||UPPER(inSign)||''') STORAGE(INITIAL 64K NEXT 4M) NOLOGGING';
      vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" altered. Partition '||UPPER(inSign)||' added.';
    EXCEPTION WHEN OTHERS THEN
      vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'". Partition '||UPPER(inSign)||'. Clearing of historical subpartition not required.';
    END;
  END IF;


  RETURN vMes;
END CheckSubpartition;

PROCEDURE CheckSubpartition(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2)
  IS
    vMes VARCHAR2(2000);
    vSPCode VARCHAR2(30);
    vHistFlg NUMBER;
    vFCTTable VARCHAR2(256);
    vHistTable VARCHAR2(256);
    vFCTATable VARCHAR2(256);
    vHistATable VARCHAR2(256);
    vDays INTEGER;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vDays := inEndDate - inBegDate;
  -- Получение кода сабпартиции
  BEGIN
    SELECT p.sp_code,p.hist_flg
          ,lower(vOwner)||'.'||e.fct_table_name AS fct_table_name
          ,lower(vOwner)||'.'||e.hist_table_name AS hist_table_name
          ,lower(vOwner)||'.'||ae.fct_table_name AS fct_a_table_name
          ,lower(vOwner)||'.'||ae.hist_table_name AS hist_a_table_name
      INTO vSPCode,vHistFlg,vFCTTable,vHistTable,vFCTATable,vHistATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inEndDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;
  -- Очистка или создание
  FOR idx IN 0..vDays LOOP
    IF vHistFlg = 0 THEN
      BEGIN
        EXECUTE IMMEDIATE 'alter table '||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'
                           ADD PARTITION '||inSign||' VALUES('''||inSign||''') storage (INITIAL 64k NEXT 4M) NOLOGGING (SUBPARTITION '||vSPCode||'_'||to_char(inBegDate+idx,'YYYYMMDD')||' VALUES (to_date('''||to_char(inBegDate+idx,'DD.MM.YYYY')||''',''DD.MM.YYYY'')))';
        vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'" altered. Partition '||inSign||' added. Subpartition '||vSPCode||'_'||to_char(inBegDate+idx,'YYYYMMDD')||' added.';
      EXCEPTION WHEN OTHERS THEN
        BEGIN
          EXECUTE IMMEDIATE 'alter table '||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'
                             MODIFY PARTITION '||inSign||' ADD SUBPARTITION '||vSPCode||'_'||to_char(inBegDate+idx,'YYYYMMDD')||' VALUES (to_date('''||to_char(inBegDate+idx,'DD.MM.YYYY')||''',''DD.MM.YYYY''))';
          vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'" altered. Partition '||inSign||' modified. Subpartition '||vSPCode||'_'||to_char(inBegDate+idx,'YYYYMMDD')||' added.';
          EXCEPTION WHEN OTHERS THEN
            EXECUTE IMMEDIATE 'alter table '||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||' truncate subpartition '||vSPCode||'_'||to_char(inBegDate+idx,'YYYYMMDD');
            vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'" altered. Partition '||inSign||': Subpartition '||vSPCode||'_'||to_char(inBegDate+idx,'YYYYMMDD')||' truncated';
          END;
      END;
    ELSE
      BEGIN
        EXECUTE IMMEDIATE
          'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' ADD PARTITION '||inSign||' VALUES('''||UPPER(inSign)||''') STORAGE(INITIAL 64K NEXT 4M) NOLOGGING';
        vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" altered. Partition '||UPPER(inSign)||' added.';
      EXCEPTION WHEN OTHERS THEN
        vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'". Partition '||UPPER(inSign)||'. Clearing of historical subpartition not required.';
      END;
    END IF;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.CheckSubpartition',vMes);
  END LOOP;
EXCEPTION WHEN OTHERS THEN
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.CheckSubpartition',SQLERRM);
END CheckSubpartition;

FUNCTION CompressSubpartition(inDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2) RETURN VARCHAR2
  IS
    vMes VARCHAR2(2000);
    vSPCode VARCHAR2(6);
    vTIBegin DATE;
    vEndTime DATE;
    vHistFlg NUMBER;
    vFCTTable VARCHAR2(256);
    vHistTable VARCHAR2(256);
    vFCTATable VARCHAR2(256);
    vHistATable VARCHAR2(256);
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  -- Получение кода сабпартиции
  BEGIN
    SELECT p.sp_code,p.hist_flg
          ,lower(vOwner)||'.'||e.fct_table_name AS fct_table_name
          ,lower(vOwner)||'.'||e.hist_table_name AS hist_table_name
          ,lower(vOwner)||'.'||ae.fct_table_name AS fct_a_table_name
          ,lower(vOwner)||'.'||ae.hist_table_name AS hist_a_table_name
      INTO vSPCode,vHistFlg,vFCTTable,vHistTable,vFCTATable,vHistATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;
  -- Сжатие
  vTIBegin := SYSDATE;
  IF vHistFlg = 0 THEN
    EXECUTE IMMEDIATE 'alter table '||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||' move subpartition '||vSPCode||'_'||to_char(inDate,'YYYYMMDD')||' compress';
    vEndTime := SYSDATE;
    vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'" Partition '||inSign||': Subpartition '||vSPCode||'_'||to_char(inDate,'YYYYMMDD')||' compressed in '||get_ti_as_hms(vEndTime - vTIBegin);
  ELSE
    vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" Partition '||inSign||'. Compressing of historical partition not required';
  END IF;
  RETURN vMes;
EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'" Partition '||inSign||': Subpartition '||vSPCode||'_'||to_char(inDate,'YYYYMMDD')||' :: '||SQLERRM;
  RETURN vMes;
END CompressSubpartition;

PROCEDURE CompressSubpartition(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2)
  IS
    vMes VARCHAR2(2000);
    vSPCode VARCHAR2(6);
    vTIBegin DATE;
    vEndTime DATE;
    vHistFlg NUMBER;
    vFCTTable VARCHAR2(256);
    vHistTable VARCHAR2(256);
    vFCTATable VARCHAR2(256);
    vHistATable VARCHAR2(256);
    vDays INTEGER;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vDays := inEndDate - inBegDate;
  -- Получение кода сабпартиции
  BEGIN
    SELECT p.sp_code,p.hist_flg
          ,lower(vOwner)||'.'||e.fct_table_name AS fct_table_name
          ,lower(vOwner)||'.'||e.hist_table_name AS hist_table_name
          ,lower(vOwner)||'.'||ae.fct_table_name AS fct_a_table_name
          ,lower(vOwner)||'.'||ae.hist_table_name AS hist_a_table_name
      INTO vSPCode,vHistFlg,vFCTTable,vHistTable,vFCTATable,vHistATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inEndDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;
  -- Сжатие
    IF vHistFlg = 0 THEN
     FOR idx IN 0..vDays LOOP
      BEGIN
        vTIBegin := SYSDATE;
        EXECUTE IMMEDIATE 'alter table '||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||' move subpartition '||vSPCode||'_'||to_char(inBegDate+idx,'YYYYMMDD')||' compress';
        vEndTime := SYSDATE;
        vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'" Partition '||inSign||': Subpartition '||vSPCode||'_'||to_char(inBegDate+idx,'YYYYMMDD')||' compressed in '||get_ti_as_hms(vEndTime - vTIBegin);
        pr_log_write(lower(vOwner)||'.pkg_etl_signs.CompressSubpartition',vMes);
      EXCEPTION WHEN OTHERS THEN
        vMes := 'ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||'" Partition '||inSign||': Subpartition '||vSPCode||'_'||to_char(inBegDate+idx,'YYYYMMDD')||' :: '||SQLERRM;
        pr_log_write(lower(vOwner)||'.pkg_etl_signs.CompressSubpartition',vMes);
      END;
    END LOOP;
    ELSE
      vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" Partition '||inSign||'. Compressing of historical partition not required';
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.CompressSubpartition',vMes);
    END IF;
END CompressSubpartition;

PROCEDURE tb_load_daily(inBegDate IN DATE,inEndDate IN DATE,inSign VARCHAR2,inAnltCode IN VARCHAR2)
  IS
    vDays INTEGER;
    vMes VARCHAR2(32700);
    vBuff VARCHAR2(32700);
    vSQL CLOB;
    vCou INTEGER := 0;
    vHistTable VARCHAR2(256);
    vHistATable VARCHAR2(256);
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vDays := inEndDate - inBegDate;
  -- Получение наименования таблицы для загрузки
  BEGIN
    SELECT UPPER(vOwner||'.'||e.hist_table_name) AS hist_table_name
          ,lower(vOwner)||'.'||ae.hist_table_name AS hist_a_table_name
      INTO vHistTable,vHistATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inEndDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;

  FOR days IN 0..vDays LOOP
    dbms_lob.createtemporary(vSQL,FALSE);
    vBuff :=
    'DECLARE'||CHR(10)||
    '  vStr VARCHAR2(4000);'||CHR(10)||
    '  vCou INTEGER := 0;'||CHR(10)||
    '  vLogged BOOLEAN := FALSE;'||CHR(10)||
    'BEGIN'||CHR(10)||
    'EXECUTE IMMEDIATE ''ALTER SESSION SET nls_date_format = ''''DD.MM.RRRR HH24:MI:SS'''''';'||CHR(10)||
    'FOR idx IN ('||CHR(10)||
    '  WITH'||CHR(10)||
    '    ch AS ('||CHR(10)||
    '      SELECT /*+ MATERIALIZE LEADING(SRC) NO_INDEX(DEST)*/'||CHR(10)||
    '             :1 AS SRC_EFFECTIVE_START,'||CHR(10)||
    '             to_date(''31.12.5999'',''DD.MM.YYYY'') AS SRC_EFFECTIVE_END,'||CHR(10)||
    '             SRC.OBJ_GID AS SRC_OBJ_GID,'||CHR(10)||
    '             SRC.SOURCE_SYSTEM_ID AS SRC_SOURCE_SYSTEM_ID,'||CHR(10)||
    '             SRC.SIGN_NAME AS SRC_SIGN_NAME,'||CHR(10)||
    '             SRC.SIGN_VAL AS SRC_SIGN_VAL,'||CHR(10)||
    '             DEST.SIGN_NAME AS D_SIGN_NAME,'||CHR(10)||
    '             DEST.EFFECTIVE_START AS D_EFFECTIVE_START,'||CHR(10)||
    '             DEST.SIGN_VAL AS D_SIGN_VAL'||CHR(10)||
    CASE WHEN inAnltCode IS NULL THEN
    '       FROM TABLE('||lower(vOwner)||'.pkg_etl_signs.get_sign(:2,:1)) src'||CHR(10)
    ELSE
    '       FROM TABLE('||lower(vOwner)||'.pkg_etl_signs.get_sign_anlt(:2,:1,:3,'||CASE WHEN UPPER(inSign) = UPPER(inAnltCode) THEN '1' ELSE '0' END||')) src'||CHR(10)
    END||
    '            LEFT JOIN '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' PARTITION('||UPPER(inSign)||') DEST'||CHR(10)||
    '              ON DEST.SIGN_NAME = :2'||CHR(10)||
    '                 AND DEST.OBJ_GID = SRC.OBJ_GID'||CHR(10)||
    '                 AND DEST.SOURCE_SYSTEM_ID = SRC.SOURCE_SYSTEM_ID'||CHR(10)||
    '                 AND :1 BETWEEN DEST.EFFECTIVE_START AND DEST.EFFECTIVE_END'||CHR(10)||
    '       WHERE '||UPPER(vOwner)||'.PKG_ETL_SIGNS.ISEQUAL(DEST.SIGN_VAL, SRC.SIGN_VAL) = 0)'||CHR(10)||
    ' ,s AS ('||CHR(10)||
    '  SELECT obj_gid'||CHR(10)||
    '         ,source_system_id'||CHR(10)||
    '         ,MIN(EFFECTIVE_START) AS VNEXTEFF'||CHR(10)||
    '         ,MIN(SIGN_VAL) KEEP(DENSE_RANK FIRST ORDER BY SIGN_NAME,EFFECTIVE_START) AS VNEXTVAL'||CHR(10)||
    '     FROM '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' PARTITION('||UPPER(inSign)||')'||CHR(10)||
    '     WHERE EFFECTIVE_START > :1'||CHR(10)||
    '       AND (obj_gid,source_system_id) IN (SELECT SRC_OBJ_GID,SRC_SOURCE_SYSTEM_ID FROM ch)'||CHR(10)||
    '   GROUP BY obj_gid,source_system_id)'||CHR(10)||
    ' ,p AS ('||CHR(10)||
    '   SELECT obj_gid'||CHR(10)||
    '         ,source_system_id'||CHR(10)||
    '         ,MAX(EFFECTIVE_END) AS VPREVEFF'||CHR(10)||
    '         ,MAX(SIGN_VAL) KEEP(DENSE_RANK LAST ORDER BY SIGN_NAME,EFFECTIVE_START) AS VPREVVAL'||CHR(10)||
    '     FROM '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' PARTITION('||UPPER(inSign)||')'||CHR(10)||
    '     WHERE EFFECTIVE_END < :1'||CHR(10)||
    '       AND (obj_gid,source_system_id) IN (SELECT SRC_OBJ_GID,SRC_SOURCE_SYSTEM_ID FROM CH WHERE D_SIGN_NAME IS NULL)'||CHR(10)||
    '   GROUP BY obj_gid,source_system_id)'||CHR(10)||
    'SELECT'||CHR(10)||
    '   CH.SRC_EFFECTIVE_START,'||CHR(10)||
    '   CH.SRC_EFFECTIVE_END,'||CHR(10)||
    '   CH.SRC_OBJ_GID,'||CHR(10)||
    '   CH.SRC_SOURCE_SYSTEM_ID,'||CHR(10)||
    '   CH.SRC_SIGN_NAME,'||CHR(10)||
    '   CH.SRC_SIGN_VAL,'||CHR(10)||
    '   CH.D_SIGN_NAME,'||CHR(10)||
    '   CH.D_EFFECTIVE_START,'||CHR(10)||
    '   CH.D_SIGN_VAL,'||CHR(10)||
    '   p.VPREVEFF,'||CHR(10)||
    '   p.VPREVVAL,'||CHR(10)||
    '   s.VNEXTEFF,'||CHR(10)||
    '   s.VNEXTVAL'||CHR(10)||
    '  FROM CH'||CHR(10)||
    '  LEFT JOIN S'||CHR(10)||
    '    ON S.OBJ_GID = CH.SRC_OBJ_GID'||CHR(10)||
    '       AND S.SOURCE_SYSTEM_ID = CH.SRC_SOURCE_SYSTEM_ID'||CHR(10)||
    '  LEFT JOIN P'||CHR(10)||
    '    ON P.OBJ_GID = CH.SRC_OBJ_GID'||CHR(10)||
    '       AND P.SOURCE_SYSTEM_ID = CH.SRC_SOURCE_SYSTEM_ID'||CHR(10)||
    ') LOOP';
    dbms_lob.writeappend(vSQL,LENGTH(vBuff),vBuff);
    vBuff :=
    '  BEGIN'||CHR(10)||
    '    IF idx.src_effective_start = idx.d_effective_start THEN'||CHR(10)||
    '      vStr := ''DDel_1'';'||CHR(10)||
    '      DELETE FROM /*+ index(a) */ '||lower(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END)||' a'||CHR(10)||
    '        WHERE sign_name = UPPER(idx.src_sign_name)'||CHR(10)||
    '          AND obj_gid = idx.src_obj_gid'||CHR(10)||
    '          AND source_system_id = idx.src_source_system_id'||CHR(10)||
    '          AND idx.src_effective_start BETWEEN effective_start AND effective_end;'||CHR(10)||
    '    ELSE'||CHR(10)||
    '      vStr := ''DUpd_1'';'||CHR(10)||
    '      UPDATE /*+ index(a) */ '||lower(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END)||' a'||CHR(10)||
    '        SET effective_end = idx.src_effective_start - 1'||CHR(10)||
    '        WHERE sign_name = UPPER(idx.src_sign_name)'||CHR(10)||
    '          AND obj_gid = idx.src_obj_gid'||CHR(10)||
    '          AND source_system_id = idx.src_source_system_id'||CHR(10)||
    '          AND idx.src_effective_start BETWEEN effective_start AND effective_end;'||CHR(10)||
    '    END IF; '||CHR(10)||
        --
    '    IF idx.vNextEff < to_date(''31.12.5999'',''DD.MM.YYYY'') AND '||lower(vOwner)||'.pkg_etl_signs.isEqual(idx.src_sign_val,idx.vNextVal) = 1 THEN'||CHR(10)||
    '      vStr := ''DUpd_2'';'||CHR(10)||
    '      UPDATE /*+ index(a) */'||lower(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END)||' a SET effective_start = idx.src_effective_start'||CHR(10)||
    '        WHERE sign_name = UPPER(idx.src_sign_name)'||CHR(10)||
    '          AND obj_gid = idx.src_obj_gid'||CHR(10)||
    '          AND source_system_id = idx.src_source_system_id'||CHR(10)||
    '          AND idx.vNextEff BETWEEN effective_start AND effective_end;'||CHR(10)||
    '    ELSIF idx.src_effective_start - idx.vPrevEff = 1  AND '||lower(vOwner)||'.pkg_etl_signs.isEqual(idx.src_sign_val,idx.vPrevVal) = 1 THEN'||CHR(10)||
    '      vStr := ''DUpd_4'';'||CHR(10)||
    '      UPDATE /*+ index(a) */'||lower(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END)||' a SET effective_end = NVL(idx.vNextEff - 1,idx.src_effective_end)'||CHR(10)||
    '        WHERE sign_name = UPPER(idx.src_sign_name)'||CHR(10)||
    '          AND obj_gid = idx.src_obj_gid'||CHR(10)||
    '          AND source_system_id = idx.src_source_system_id'||CHR(10)||
    '          AND idx.vPrevEff BETWEEN effective_start AND effective_end;'||CHR(10)||
    '    ELSE'||CHR(10)||
    '      IF idx.src_sign_val IS NOT NULL THEN'||CHR(10)||
    '        vStr := ''DIns_2'';'||CHR(10)||
    '        INSERT INTO '||lower(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END)||CHR(10)||
    '          (effective_start,effective_end,obj_gid,source_system_id,sign_name,sign_val)'||CHR(10)||
    '          VALUES (idx.src_effective_start'||CHR(10)||
    '                 ,NVL(idx.vNextEff - 1,idx.src_effective_end)'||CHR(10)||
    '                 ,idx.src_obj_gid'||CHR(10)||
    '                 ,idx.src_source_system_id'||CHR(10)||
    '                 ,UPPER(idx.src_sign_name)'||CHR(10)||
    '                 ,idx.src_sign_val);'||CHR(10)||
    '      END IF;  '||CHR(10)||
    '    END IF;'||CHR(10)||
    '  EXCEPTION WHEN OTHERS THEN'||CHR(10)||
    '    IF NOT vLogged THEN'||CHR(10)||
    '      vStr := ''ERROR :: "'||UPPER(inSign)||'" - "''||to_char(idx.src_effective_start,''DD.MM.YYYY'')||''" - OBJ_SID = ''||idx.src_obj_gid||''#!#''||idx.src_source_system_id||'' :: ''||SQLERRM||Chr(10)||vStr;'||CHR(10)||
    '      '||lower(vOwner)||'.pkg_etl_signs.pr_log_write('''||lower(vOwner)||'.pkg_etl_signs.tb_load_daily'',vStr);'||CHR(10)||
    '      vLogged := TRUE;'||CHR(10)||
    '    END IF;'||CHR(10)||
    '  END;'||CHR(10)||
    '  vCou := vCou + 1;'||CHR(10)||
    'END LOOP;'||CHR(10)||
    CASE WHEN inAnltCode IS NULL THEN
      ':3 := vCou;' ELSE ':4 := vCou;'
    END||CHR(10)||
    'END;';
    dbms_lob.writeappend(vSQL,LENGTH(vBuff),vBuff);
      IF inAnltCode IS NULL THEN
        EXECUTE IMMEDIATE vSQL USING IN inBegDate+days
               ,IN UPPER(inSign)
               ,OUT vCou;
      ELSE
        EXECUTE IMMEDIATE vSQL USING IN inBegDate+days
               ,IN UPPER(inSign)
               ,IN UPPER(inAnltCode)
               ,OUT vCou;
      END IF;
    --dbms_output.put_line(vSQL);
    COMMIT;
    vMes := 'SUCCESSFULLY :: "'||UPPER(inSign)||'" - "'||to_char(inBegDate + days,'DD.MM.YYYY')||'" - '||vCou||' rows proccessed in table "'||lower(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END)||'"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_daily',vMes);
    dbms_lob.freetemporary(vSQL);
  END LOOP;
EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||UPPER(inSign)||'" - "'||to_char(inBegDate,'DD.MM.YYYY')||'" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_daily',vMes);
  vMes := dbms_lob.substr(vSQL,32700,1);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_daily',vMes);
END tb_load_daily;

PROCEDURE ptb_load_daily(inBegDate IN DATE,inEndDate IN DATE,inSign VARCHAR2,inAnltCode IN VARCHAR2)
  IS
    vDays INTEGER;
    vMes VARCHAR2(32700);
    vBuff VARCHAR2(32700);
    vCou INTEGER := 0;
    vFctTable VARCHAR2(256);
    vFctATable VARCHAR2(256);
    vSPCode VARCHAR2(256);
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  EXECUTE IMMEDIATE 'alter session set "_FIX_CONTROL" = "11814428:0"';
  vDays := inEndDate - inBegDate;
  -- Получение наименования таблицы для загрузки
  BEGIN
    SELECT UPPER(vOwner||'.'||e.fct_table_name) AS fct_table_name
          ,lower(vOwner)||'.'||ae.fct_table_name AS fct_a_table_name
          ,p.sp_code
      INTO vFctTable,vFctATable,vSPCode
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inEndDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;

  FOR days IN 0..vDays LOOP
    vBuff :=
    'DECLARE'||CHR(10)||
    '  vCou INTEGER := 0;'||CHR(10)||
    'BEGIN'||CHR(10)||
    'FOR rw IN ('||CHR(10)||
    '   SELECT  :1 as as_of_date,obj_gid,source_system_id,sign_name,sign_val'||CHR(10)||
    CASE WHEN inAnltCode IS NULL THEN
    '       FROM TABLE('||lower(vOwner)||'.pkg_etl_signs.get_sign(:2,:1))'||CHR(10)
    ELSE
    '       FROM TABLE('||lower(vOwner)||'.pkg_etl_signs.get_sign_anlt(:2,:1,:3,'||CASE WHEN UPPER(inSign) = UPPER(inAnltCode) THEN '1' ELSE '0' END||'))'||CHR(10)
    END||
    'WHERE sign_val IS NOT NULL'||CHR(10)||
    ') LOOP'||CHR(10)||
    '  INSERT INTO '||CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END||' subpartition('||vSPCode||'_'||to_char(inBegDate+days,'YYYYMMDD')||') (as_of_date,obj_gid,source_system_id,sign_name,sign_val)'||CHR(10)||
    '    VALUES(rw.as_of_date,rw.obj_gid,rw.source_system_id,rw.sign_name,rw.sign_val);'||CHR(10)||
    '  vCou := vCou + 1;'||CHR(10)||
    'END LOOP;'||CHR(10)||
    CASE WHEN inAnltCode IS NULL THEN ':3 := vCou;' ELSE ':4 := vCou;' END||CHR(10)||
    'END;';
    IF inAnltCode IS NULL THEN
      EXECUTE IMMEDIATE vBuff USING IN inBegDate+days
             ,IN UPPER(inSign)
             ,OUT vCou;
    ELSE
      EXECUTE IMMEDIATE vBuff USING IN inBegDate+days
             ,IN UPPER(inSign)
             ,IN UPPER(inAnltCode)
             ,OUT vCou;
    END IF;
    COMMIT;
    vMes := 'SUCCESSFULLY :: "'||UPPER(inSign)||'" - "'||to_char(inBegDate + days,'DD.MM.YYYY')||'" - '||vCou||' rows inserted into table "'||lower(CASE WHEN inAnltCode IS NULL THEN vFCTTable ELSE vFCTATable END)||'"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ptb_load_daily',vMes);
  END LOOP;
EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||UPPER(inSign)||'" - '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ptb_load_daily',vMes);
END ptb_load_daily;

PROCEDURE load_sign(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2,inPrepareSegments NUMBER)
  IS
    vDays INTEGER;
    vMes VARCHAR2(2000);
    vTIBegin DATE;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vHistFlg NUMBER;
    vCond NUMBER;
    vBuff VARCHAR2(32700);
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vDays := inEndDate - inBegDate;
  vMes := 'START :: "'||inSign||'" "'||to_char(inBegDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.load_sign" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_sign',vMes);
  --BEGIN
    SELECT p.hist_flg,1/*GetConditionResult(p.condition)*/ AS vCond
      INTO vHistFlg,vCond
      FROM tb_signs_pool p
      WHERE p.sign_name = UPPER(inSign);
  --EXCEPTION WHEN NO_DATA_FOUND THEN
  --  RAISE_APPLICATION_ERROR(-20000,'Показатель "'||UPPER(inSign)||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  --END;

  IF vCond = 1 THEN
    FOR idx IN 0..vDays
    LOOP
      IF inPrepareSegments = 1 THEN
        -- Подготовка субпартиций
        vTIBegin := SYSDATE;
        vMes := CheckSubpartition(inBegDate+idx,inBegDate+idx,UPPER(inSign),inAnltCode);
        -- Сохранение времени  подготовки в таблицу статистики расчетов
        vEndTime := SYSDATE;
        pr_stat_write(inSign,inAnltCode,(vEndTime - vTIBegin)*24*60*60,'PREPARE');
      END IF;
      -- Вставка данных в таблицу
      vTIBegin := SYSDATE;
      IF vHistFlg = 0 THEN -- Для "FCT" показателей
        vBuff :=
        'BEGIN'||CHR(10)||
        lower(vOwner)||'.pkg_etl_signs.ptb_load_daily(:1,:2,:3,:4);'||CHR(10)||
        'END;';
        EXECUTE IMMEDIATE vBuff USING IN inBegDate+idx,IN inBegDate+idx,IN UPPER(inSign),IN inAnltCode;
      ELSE -- Для "HIST" показателей
        vBuff :=
        'BEGIN'||CHR(10)||
           lower(vOwner)||'.pkg_etl_signs.tb_load_daily(:1,:2,:3,:4);'||CHR(10)||
        'END;';
        EXECUTE IMMEDIATE vBuff USING IN inBegDate+idx,IN inBegDate+idx,IN UPPER(inSign),IN inAnltCode;
      END IF;

      -- Сохранение времени расчета в таблицу статистики расчетов
      vEndTime := SYSDATE;
      pr_stat_write(inSign,inAnltCode,(vEndTime - vTIBegin)*24*60*60,'CALC');
      --Сжатие субпартиций
      vTIBegin := SYSDATE;
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_sign',CompressSubpartition(inBegDate+idx,UPPER(inSign),inAnltCode));
      -- Сохранение времени сжатия в таблицу статистики расчетов
      vEndTime := SYSDATE;
      pr_stat_write(inSign,inAnltCode,(vEndTime - vTIBegin)*24*60*60,'COMPRESS');
    END LOOP;
  ELSE
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_sign','ERROR :: "'||inSign||'" - Не выполнено доп.условие запуска расчета показателя, расчет не может быть запущен');
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||inSign||'" "'||to_char(inBegDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.load_sign" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_sign',vMes);
EXCEPTION
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := SUBSTR('ERROR :: "'||UPPER(inSign)||'" - '||SQLERRM,1,4000);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_sign',vMes);
    vMes := 'FINISH :: "'||inSign||'" "'||to_char(inBegDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.load_sign" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_sign',vMes);
END load_sign;

PROCEDURE load_new(inSQL IN CLOB,inJobName IN VARCHAR2 DEFAULT NULL)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := NVL(inJobName,UPPER(vOwner)||'.'||'LOADJOB_'||tb_signs_job_id_seq.nextval);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  MERGE INTO tb_signs_job dest
    USING (SELECT vJobName AS job_name,vBegTime AS start_time,SUBSTR(inSQL,1,4000) AS action_sql FROM dual) src
      ON (src.job_name = dest.job_name)
  WHEN NOT MATCHED THEN INSERT (job_name,start_time,action_sql,head_job_name) VALUES (src.job_name,src.start_time,src.action_sql,src.job_name)
  WHEN MATCHED THEN UPDATE SET dest.start_time = src.start_time,dest.action_sql = src.action_sql;
  COMMIT;

  EXECUTE IMMEDIATE 'ALTER SESSION SET nls_date_format = ''DD.MM.RRRR HH24:MI:SS''';
  ChainKiller(ChainStarter(ChainBuilder(inSQL),vJobName));

  vEndTime := SYSDATE;
  UPDATE tb_signs_job j SET j.elapsed_time = get_ti_as_hms(vEndTime - vBegTime), state = 'FINISHED', last_update = vEndTime
    WHERE job_name = vJobName AND state IS NULL;
  COMMIT;
EXCEPTION WHEN OTHERS THEN
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.load_new',SQLERRM);
END load_new;

PROCEDURE load(inBegDate IN DATE,inEndDate IN DATE)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.RRRR');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.RRRR');
    vBuff VARCHAR2(32700);
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'LOADALLJOB_'||tb_signs_job_id_seq.nextval;
    vCou INTEGER := 0;
BEGIN
  vBuff :=
    q'{SELECT mess FROM (SELECT CASE WHEN REGEXP_LIKE(p.condition,'/*SENDMESSAGE_IF_FALSE .+*/') AND }'||vOwner||q'{.pkg_etl_signs.GetConditionResult(p.condition,'INBEGDATE DATE }'||vBegDate||'#!#INENDDATE DATE '||vEndDate||q'{',p.sign_name) = 0 AND p.archive_flg = 0 THEN SUBSTR(p.condition,24,INSTR(p.condition,'*/') - 24) ELSE NULL END AS mess
      FROM tb_signs_pool p) WHERE mess IS NOT NULL}';
  EXECUTE IMMEDIATE 'DECLARE vCou INTEGER; BEGIN SELECT COUNT(1) INTO vCou FROM ('||CHR(10)||vBuff||CHR(10)||'); :1 := vCou; EXCEPTION WHEN OTHERS THEN :1 := 9999999999; END;' USING OUT vCou;
  --dbms_output.put_line(vCou);
  IF vCou BETWEEN 1 AND 9999999998 THEN
    send_message_about_project(to_number(GetVarValue('vProjectID')),'Период: '||vBegDate||' - '||vEndDate||' :: Не выполнены условия ежедневного расчета для следующих показателей:',SQLasHTML(vBuff,'MESS','Описание:'));
  END IF;
  
  vBuff :=
    q'{SELECT p.sign_name AS ID
          ,s.prev_sign_name AS parent_id
          ,'}'||vOwner||q'{.pkg_etl_signs.'||CASE WHEN p.sign_sql IS NOT NULL THEN 'load_sign' ELSE 'mass_load' END AS unit
          ,'}'||vBegDate||'#!#'||vEndDate||q'{#!#'||p.sign_name||'#!##!#1' AS params
          ,CASE WHEN (p.condition IS NULL OR }'||vOwner||q'{.pkg_etl_signs.GetConditionResult(p.condition,'INBEGDATE DATE }'||vBegDate||'#!#INENDDATE DATE '||vEndDate||q'{',p.sign_name) = 1) AND p.archive_flg = 0 THEN 0 ELSE 1 END AS skip
      FROM tb_signs_pool p
           LEFT JOIN tb_sign_2_sign s
            ON s.sign_name = p.sign_name}';
   --pr_log_write(lower(vOwner)||'.pkg_etl_ctr_signs.load',vBuff);        
   load_new(vBuff,vJobName);
   --dbms_output.put_line(vBuff);
END load;

PROCEDURE load_rel_asc(inBegDate IN DATE,inEndDate IN DATE,inSigns IN VARCHAR2,inUnit IN VARCHAR2,inAdvFilter VARCHAR2 DEFAULT NULL)
IS
  vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.RRRR');
  vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.RRRR');
  vOwner VARCHAR2(256) := pkg_etl_signs.GetVarValue('vOwner');
  vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'LOADRELASCJOB_'||tb_signs_job_id_seq.nextval;
  vBuff VARCHAR2(32700) :=
q'{SELECT sign_name AS ID
      ,prev_name AS parent_id
      ,'}'||inUnit||q'{' AS unit
      ,'}'||vBegDate||'#!#'||vEndDate||q'{#!#'||sign_name||'#!##!#1' AS params
      ,CASE WHEN archive_flg = 0 AND condition = 1 THEN 0 ELSE 1 END AS SKIP
  FROM (
    SELECT DISTINCT s2s.sign_name,s2s.prev_sign_name AS prev_name,p.archive_flg,}'||vOwner||q'{.pkg_etl_signs.GetConditionResult(p.condition,'INBEGDATE DATE }'||vBegDate||'#!#INENDDATE DATE '||vEndDate||q'{',s2s.sign_name) AS condition
      FROM }'||vOwner||q'{.tb_sign_2_sign s2s
           LEFT JOIN }'||vOwner||q'{.tb_signs_pool p ON p.sign_name = s2s.sign_name
    CONNECT BY PRIOR s2s.sign_name = s2s.prev_sign_name  
    START WITH  s2s.prev_sign_name IN (SELECT str FROM TABLE(}'||vOwner||q'{.pkg_etl_signs.parse_str('}'||inSigns||q'{',',')))
    UNION
    SELECT sign_name,NULL,archive_flg,}'||vOwner||q'{.pkg_etl_signs.GetConditionResult(condition,'INBEGDATE DATE }'||vBegDate||'#!#INENDDATE DATE '||vEndDate||q'{',sign_name)
      FROM }'||vOwner||q'{.tb_signs_pool
      WHERE sign_name IN (SELECT str FROM TABLE(}'||vOwner||q'{.pkg_etl_signs.parse_str('}'||inSigns||q'{',',')))) a }'||inAdvFilter;
BEGIN
   load_new(vBuff,vJobName);
   --dbms_output.put_line(vBuff);
END load_rel_asc;

PROCEDURE load_rel_desc(inBegDate IN DATE,inEndDate IN DATE,inSigns IN VARCHAR2,inUnit IN VARCHAR2,inAdvFilter VARCHAR2 DEFAULT NULL)
IS
  vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.RRRR');
  vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.RRRR');
  vOwner VARCHAR2(256) := pkg_etl_signs.GetVarValue('vOwner');
  vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'LOADRELDESCJOB_'||tb_signs_job_id_seq.nextval;
  vBuff VARCHAR2(32700) :=
q'{SELECT sign_name AS ID
      ,prev_name AS parent_id
      ,'}'||inUnit||q'{' AS unit
      ,'}'||vBegDate||'#!#'||vEndDate||q'{#!#'||sign_name||'#!##!#1' AS params
      ,CASE WHEN archive_flg = 0 AND condition = 1 THEN 0 ELSE 1 END AS SKIP
  FROM (
    SELECT DISTINCT
           p.sign_name
          ,s2s.prev_sign_name AS prev_name
          ,p.archive_flg
          ,}'||vOwner||q'{.pkg_etl_signs.GetConditionResult(p.condition,'INBEGDATE DATE }'||vBegDate||'#!#INENDDATE DATE '||vEndDate||q'{',p.sign_name) AS condition
      FROM }'||vOwner||q'{.tb_signs_pool p
              LEFT JOIN }'||vOwner||q'{.tb_sign_2_sign s2s
                ON p.sign_name = s2s.sign_name
       CONNECT BY NOCYCLE PRIOR s2s.prev_sign_name = p.sign_name
       START WITH p.sign_name IN (SELECT str FROM TABLE(}'||vOwner||q'{.pkg_etl_signs.parse_str('}'||inSigns||q'{',',')))) a }'||inAdvFilter;
BEGIN
   load_new(vBuff,vJobName);
   --dbms_output.put_line(vBuff);
END load_rel_desc;


PROCEDURE load_all_anlts(inBegDate IN DATE,inEndDate IN DATE)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vBuff VARCHAR2(32700);
    vJobName VARCHAR2(256);
    vDays INTEGER;
    vDate VARCHAR2(30);
    --
    vMes VARCHAR2(2000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vDays := inEndDate - inBegDate;
  vMes := 'START :: "'||to_char(inBegDate,'DD.MM.YYYY')||'" - "'||to_char(inEndDate,'DD.MM.YYYY')||'" :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.load_all_anlts" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_sign',vMes);
  
  FOR idx IN 0..vDays
  LOOP
    vDate := to_char(inBegDate + idx,'DD.MM.RRRR');
    vBuff :=
    q'{SELECT ID,parent_id,unit,params,SKIP FROM (
    SELECT s2a.sign_name||';'||s2a.anlt_code AS id
          ,NULL AS parent_id
          ,'}'||vOwner||q'{.pkg_etl_signs.load_sign' AS unit
          ,'}'||vDate||'#!#'||vDate||q'{#!#'||s2a.sign_name||'#!#'||s2a.anlt_code||'#!#1' AS params
          ,CASE WHEN (p.condition IS NULL OR }'||vOwner||q'{.pkg_etl_signs.GetConditionResult(p.condition,'INBEGDATE DATE }'||vDate||'#!#INENDDATE DATE '||vDate||q'{',s2a.sign_name) = 1) AND p.archive_flg = 0 AND p.sign_name IS NOT NULL THEN 0 ELSE 1 END AS SKIP
          ,p.sign_name
      FROM }'||vOwner||q'{.tb_sign_2_anlt s2a
           LEFT JOIN }'||vOwner||q'{.tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND to_date('}'||vDate||q'{','DD.MM.RRRR') BETWEEN a.effective_start AND a.effective_end
                AND a.archive_flg = 0
           LEFT JOIN }'||vOwner||q'{.tb_signs_pool p
             ON p.sign_name = s2a.sign_name
    ) WHERE skip = 0}';
    vJobName := UPPER(vOwner)||'.'||'LOADALLANLTSJOB_'||tb_signs_job_id_seq.nextval;
    load_new(vBuff,vJobName);
  END LOOP;
  
  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inBegDate,'DD.MM.YYYY')||'" - "'||to_char(inEndDate,'DD.MM.YYYY')||'" :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.load_all_anlts" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_all_anlts',vMes);
EXCEPTION
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: "'||vDate||'" - '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_all_anlts',vMes);
    vMes := 'FINISH :: "'||to_char(inBegDate,'DD.MM.YYYY')||'" - "'||to_char(inEndDate,'DD.MM.YYYY')||'" :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.load_all_anlts" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_all_anlts',vMes);
END load_all_anlts;

PROCEDURE mass_load(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2,inPrepareSegments NUMBER)
  IS
    vMes VARCHAR2(2000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vTIBegin DATE;
    vHistFlg NUMBER;
    vMassSQL CLOB;
    vMassDDL CLOB;
    --
    vHistTable VARCHAR2(256);
    vFctTable VARCHAR2(256);
    vTmpTable VARCHAR2(256);
    vHistATable VARCHAR2(256);
    vFctATable VARCHAR2(256);
    vTmpATable VARCHAR2(256);
    vSPCode VARCHAR2(30);
    vBuff VARCHAR2(32700);
    vMAsk VARCHAR2(30);
    --
    vRowCount INTEGER := 0;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_ctr_signs.mass_load" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_ctr_signs.mass_load',vMes);

  --EXECUTE IMMEDIATE 'alter session set "_FIX_CONTROL" = "11814428:0"';

  BEGIN
    SELECT p.hist_flg,p.mass_sql,p.sp_code
          ,UPPER(vOwner||'.'||e.hist_table_name) AS hist_table_name
          ,UPPER(vOwner||'.'||e.fct_table_name) AS fct_table_name
          ,UPPER(vOwner||'.'||e.tmp_table_name) AS tmp_table_name
          ,UPPER(vOwner||'.'||ae.fct_table_name) AS fct_a_table_name
          ,UPPER(vOwner||'.'||ae.hist_table_name) AS hist_a_table_name
          ,UPPER(vOwner||'.'||ae.tmp_table_name) AS tmp_a_table_name
      INTO vHistFlg,vMassSQL,vSPCode,vHistTable,vFctTable,vTmpTable,vFctATable,vHistATable,vTmpATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inEndDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||UPPER(inSign)||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;

  -- Установка архивного флага в таблице показателей (чтобы не было пересечения с ежедневной
  -- загрузкой. Ежедневка смотрит на этот флаг и если 1, то не расчитывает показатель)
  vBuff :=
  'BEGIN'||CHR(10)||
  '  UPDATE '||lower(vOwner)||'.tb_signs_pool SET archive_flg = 1 WHERE sign_name = '''||UPPER(inSign)||''';'||CHR(10)||
  '  COMMIT;'||CHR(10)||
  'END;';
  BEGIN
    EXECUTE IMMEDIATE vBuff;
  EXCEPTION WHEN OTHERS THEN
    RAISE_APPLICATION_ERROR(-20000,'ERROR :: UPD_1');
  END;

 -- Загрузка данных
 IF inPrepareSegments = 1 AND vMassSQL IS NOT NULL AND vHistFlg = 1 AND inAnltCode IS NULL THEN
   -- Если в "HIST" показателе заполнено поле MASS_SQL, то используем его для быстрой заливки

   -- Подготовка субпартиций в промежуточной таблице
   vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' DROP PARTITION '||UPPER(inSign);
   BEGIN
     EXECUTE IMMEDIATE vBuff;
   EXCEPTION WHEN OTHERS THEN
     NULL;
   END;

   vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' ADD PARTITION '||UPPER(inSign)||' VALUES('''||UPPER(inSign)||''') NOLOGGING STORAGE(INITIAL 64K NEXT 4M) (SUBPARTITION '||vSPCode||'_NEW VALUES LESS THAN (MAXVALUE))';
   EXECUTE IMMEDIATE vBuff;
   -- Окончание подготовки субпартиций в промежуточной таблице

   -- Вставка данных в промежуточную таблицу
   vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - вставка данных во временную таблицу --------';
   pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load',vMes);

   vTIBegin := SYSDATE;
   dbms_lob.createtemporary(vMassDDL,FALSE);
   vBuff :=
   'BEGIN'||CHR(10)||
   '  INSERT INTO '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||CHR(10)||
   '   (effective_start,effective_end,obj_gid,source_system_id,sign_name,sign_val)'||CHR(10);
   dbms_lob.writeappend(vMassDDL,LENGTH(vBuff),vBuff);

   vMassDDL := vMassDDL||vMassSQL||';'||CHR(10);

   vBuff :=
   ' :1 := SQL%ROWCOUNT;'||CHR(10)||
   'COMMIT;'||CHR(10)||'END;';
   dbms_lob.writeappend(vMassDDL,LENGTH(vBuff),vBuff);

   BEGIN
     EXECUTE IMMEDIATE vMassDDL USING OUT vRowCount;
     --dbms_output.put_line(vMassDDL);
     vEndTime := SYSDATE;
     vMes := 'SUCCESSFULLY :: "'||UPPER(inSign)||'" - '||vRowCount||' rows inserted into "'||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||'" in '||get_ti_as_hms(vEndTime - vTIBegin);
     pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load',vMes);
   EXCEPTION WHEN OTHERS THEN
     vEndTime := SYSDATE;
     vMes := 'ERROR :: "'||UPPER(inSign)||'" not inserted into "'||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||'" in '||get_ti_as_hms(vEndTime - vTIBegin)||' with errors :: '||SQLERRM||CHR(10)||'------'||CHR(10)||dbms_lob.substr(vMassDDL,1000);
     pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load',vMes);
   END;

   vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - окончание вставки данных во временную таблицу. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
   pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load',vMes);

   -- Склеивание в целевую таблицу
   --***--
   pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load',CheckSubpartition(inBegDate,inEndDate,UPPER(inSign),inAnltCode));
   --***--
   sign_gluing(UPPER(inSign),UPPER(inAnltCode),'011');
   

  ELSE
    IF vHistFlg = 0 THEN -- для "FCT" показателей
      vTIBegin := SYSDATE;
      vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - загрузка данных --------';
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load',vMes);

      vMes := CheckSubpartition(inBegDate,inEndDate,UPPER(inSign),UPPER(inAnltCode));
      -- Сохранение времени  подготовки в таблицу статистики расчетов
      /*vEndTime := SYSDATE;
      INSERT INTO tb_signs_calc_stat (sign_name,anlt_code,action,sec)
        VALUES(inSign,inAnltCode,'PREPARE',ROUND((vEndTime - vTIBegin)*24*60*60/(inEndDate - inBegDate),1));*/

      --mass_load_parallel_by_date_pe(inBegDate,inEndDate,lower(vOwner)||'.pkg_etl_signs.load_sign','VARCHAR2 '||UPPER(inSign)||'::VARCHAR2 '||inAnltCode);
      mass_load_parallel_by_month(inBegDate,inEndDate,lower(vOwner)||'.pkg_etl_signs.load_sign','VARCHAR2 '||UPPER(inSign)||'::VARCHAR2 '||UPPER(inAnltCode)||'::NUMBER 0');

      vEndTime := SYSDATE;
      vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - окончание загрузки данных. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load',vMes);
    ELSE -- для "HIST" показателей
      vMask := '11'||'0'||'100';
      tb_load_mass(inBegDate,inEndDate,UPPER(inSign),inAnltCode,vMask);
    END IF;
  END IF;
  -- Возврат архивного флага в исходную
  vBuff :=
  'BEGIN'||CHR(10)||
  '  UPDATE tb_signs_pool SET archive_flg = 0 WHERE sign_name = '''||UPPER(inSign)||''';'||CHR(10)||
  '  COMMIT;'||CHR(10)||
  'END;';
  EXECUTE IMMEDIATE vBuff;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.mass_load" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load',vMes);
EXCEPTION WHEN OTHERS THEN
  vEndTime := SYSDATE;
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.mass_load" :: '||SQLERRM||Chr(10)||vBuff;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load',vMes);
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.mass_load" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.mass_load',vMes);
END mass_load;

PROCEDURE sign_gluing(inSign IN VARCHAR2,inAnltCode IN VARCHAR2,inMask IN VARCHAR2 DEFAULT '111')
 IS
   vBegTime DATE := SYSDATE;
   vEndTime DATE;
   vTIBegin DATE;
   vMes VARCHAR2(32700);
   vSPCode VARCHAR2(256);
   vHistTable VARCHAR2(256);
   vTmpTable VARCHAR2(256);
   vHistFlg NUMBER;
   vHistATable VARCHAR2(256);
   vTmpATable VARCHAR2(256);
   vBuff VARCHAR2(32700);
   vCou INTEGER := 0;
   vMask VARCHAR2(256) := NVL(inMask,'111');
   vTmpStage BOOLEAN := SUBSTR(vMask,1,1) = '1';
   vTargetStage BOOLEAN := SUBSTR(vMask,2,1) = '1';
   vTargetTruncate BOOLEAN := SUBSTR(vMask,3,1) = '1';
   vOwner VARCHAR2(4000) := GetVarValue('vOwner');
   vGluingStatus VARCHAR2(4000) := 'SUCCESSFULLY';
   vTmpStatus VARCHAR2(4000) := 'SUCCESSFULLY';
BEGIN
  vMes := 'START :: "'||inSign||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.sign_gluing" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);
  -- Получение метаданных
  BEGIN
    SELECT p.sp_code
          ,UPPER(vOwner||'.'||e.tmp_table_name) AS tmp_table_name
          ,UPPER(vOwner||'.'||e.hist_table_name) AS hist_table_name
          ,p.hist_flg
          ,UPPER(vOwner||'.'||ae.tmp_table_name) AS tmp_a_table_name
          ,UPPER(vOwner||'.'||ae.hist_table_name) AS hist_a_table_name
      INTO vSPCode
          ,vTmpTable
          ,vHistTable
          ,vHistFlg
          ,vTmpATable
          ,vHistATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND SYSDATE BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;

  IF vTmpStage THEN
    vTIBegin := SYSDATE;
    vMes := 'CONTINUE :: ------ Вставка '||UPPER(inSign)||' во временную таблицу ------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);

    vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' TRUNCATE PARTITION '||UPPER(inSign);
    BEGIN
      EXECUTE IMMEDIATE vBuff;
      --dbms_output.put_line(vBuff);

      vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||'" - Partition '||UPPER(inSign)||' truncated';
    EXCEPTION WHEN OTHERS THEN
      vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' ADD PARTITION '||UPPER(inSign)||' VALUES('''||UPPER(inSign)||''') STORAGE (INITIAL 64K NEXT 4M) NOLOGGING';
      BEGIN
        EXECUTE IMMEDIATE vBuff;
        vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||'" - Partition '||UPPER(inSign)||' added';
      EXCEPTION WHEN OTHERS THEN
       vMes := 'ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||'" - Partition '||UPPER(inSign)||' not proccessed :: '||SQLERRM;
      END;
    END;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);

    vBuff :=
    'BEGIN'||CHR(10)||
    '  INSERT INTO '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' (effective_start,effective_end,obj_gid,source_system_id,sign_name,sign_val)'||CHR(10)||
    '    SELECT effective_start,effective_end,obj_gid,source_system_id,sign_name,sign_val'||CHR(10)||
    '      FROM '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' WHERE sign_name = :1;'||CHR(10)||
    '  :2 := SQL%ROWCOUNT;'||CHR(10)||
    '  COMMIT;'||CHR(10)||
    'END;';
    BEGIN
      EXECUTE IMMEDIATE vBuff USING IN UPPER(inSign),OUT vCou;
      --dbms_output.put_line(vBuff);

      vEndTime := SYSDATE;
      vMes := 'SUCCESSFULLY :: "'||UPPER(inSign)||'" - '||vCou||' rows inserted into table "'||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||'" in '||get_ti_as_hms(vEndTime - vTIBegin);
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);
    EXCEPTION WHEN OTHERS THEN
      vEndTime := SYSDATE;
      vMes := 'ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||'" - "'||UPPER(inSign)||'" not inserted :: '||SQLERRM;
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);
    END;
    vTmpStatus := SUBSTR(vMes,1,INSTR(vMes,' ') - 1);

    vMes := 'CONTINUE :: ------ Окончание вставки '||UPPER(inSign)||' во временную таблицу. Время выполнения '||get_ti_as_hms(vEndTime - vTIBegin)||' ------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);
  END IF;
  -------
  IF vTargetStage AND vTmpStatus = 'SUCCESSFULLY' THEN
    vTIBegin := SYSDATE;
    vMes := 'CONTINUE :: ------ Склеивание '||UPPER(inSign)||' в целевую таблицу ------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);

    IF vTargetTruncate THEN
      vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' TRUNCATE PARTITION '||UPPER(inSign);
      BEGIN
        EXECUTE IMMEDIATE vBuff;
        --dbms_output.put_line(vBuff);

        vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" - Partition '||UPPER(inSign)||' truncated';
      EXCEPTION WHEN OTHERS THEN
        vMes := 'ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" - Partition '||UPPER(inSign)||' not truncated :: '||SQLERRM;
      END;
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);
    END IF;

    vBuff :=
    'BEGIN'||CHR(10)||
    '  INSERT INTO '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' (effective_start,effective_end,obj_gid,source_system_id,sign_name,sign_val)'||CHR(10)||
    '    SELECT MIN(effective_start) AS effective_start'||CHR(10)||
    '          ,effective_end    '||CHR(10)||
    '          ,obj_gid'||CHR(10)||
    '          ,source_system_id'||CHR(10)||
    '          ,sign_name'||CHR(10)||
    '          ,sign_val '||CHR(10)||
    '      FROM ('||CHR(10)||
    '        SELECT effective_start'||CHR(10)||
    '              ,NVL2(NVL2(LEAD(next_start) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start)'||CHR(10)||
    '                        ,LEAD(effective_start) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start) - 1'||CHR(10)||
    '                        ,LEAD(effective_end) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start))'||CHR(10)||
    '                   ,CASE WHEN next_start - effective_end = 1 THEN'||CHR(10)||
    '                      CASE WHEN '||lower(vOwner)||'.pkg_etl_signs.isEqual(LEAD(sign_val) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start), sign_val) = 1'||CHR(10)||
    '                             THEN LEAD(effective_end) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start)'||CHR(10)||
    '                        ELSE LEAD(effective_start) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start) - 1'||CHR(10)||
    '                      END'||CHR(10)||
    '                      ELSE effective_end'||CHR(10)||
    '                    END'||CHR(10)||
    '                   ,effective_end'||CHR(10)||
    '                   ) AS effective_end'||CHR(10)||
    '              ,obj_gid'||CHR(10)||
    '              ,source_system_id'||CHR(10)||
    '              ,sign_name'||CHR(10)||
    '              ,sign_val'||CHR(10)||
    '          FROM (SELECT /*+ no_index(s) */'||CHR(10)||
    '                       obj_gid'||CHR(10)||
    '                      ,source_system_id'||CHR(10)||
    '                      ,effective_start'||CHR(10)||
    '                      ,effective_end'||CHR(10)||
    '                      ,LEAD(effective_start) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start) AS next_start'||CHR(10)||
    '                      ,sign_name'||CHR(10)||
    '                      ,sign_val'||CHR(10)||
    '                      ,CASE WHEN '||lower(vOwner)||'.pkg_etl_signs.isEqual(LAG(sign_val) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start), sign_val) = 0'||CHR(10)||
    '                                 OR effective_start - LAG(effective_end) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start) > 1'||CHR(10)||
    '                                 OR LEAD(effective_start) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start) - effective_end > 1'||CHR(10)||
    '                                 OR NVL(LEAD(effective_start) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start),to_date(''31.12.5999'',''DD.MM.YYYY'')) - effective_end > 1'||CHR(10)||
    '                                 OR effective_start - NVL(LAG(effective_end) OVER (PARTITION BY obj_gid,source_system_id ORDER BY effective_start),to_date(''01.01.0001'',''DD.MM.YYYY'')) > 1'||CHR(10)||
    '                                 OR effective_end = to_date(''31.12.5999'',''DD.MM.YYYY'')'||CHR(10)||
    '                              THEN 1'||CHR(10)||
    '                       ELSE 0'||CHR(10)||
    '                       END AS flg'||CHR(10)||
    '                  FROM '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' s'||CHR(10)||
    '                  WHERE sign_name = :1'||CHR(10)||
    '          ) WHERE flg = 1'||CHR(10)||
    '    ) WHERE effective_end IS NOT NULL AND sign_val IS NOT NULL'||CHR(10)||
    '  GROUP BY obj_gid'||CHR(10)||
    '          ,source_system_id'||CHR(10)||
    '          ,effective_end'||CHR(10)||
    '          ,sign_name'||CHR(10)||
    '          ,sign_val;'||CHR(10)||
    '  :2 := SQL%ROWCOUNT;'||CHR(10)||
    '  COMMIT;'||CHR(10)||
    ' END;';
    BEGIN
      EXECUTE IMMEDIATE vBuff USING IN UPPER(inSign),OUT vCou;
      --RAISE_APPLICATION_ERROR(-20000,'ERROR :: ТЕСТОВАЯ ОШИБКА');
      --dbms_output.put_line(vBuff);
    
      -- Сохранение времени расчета в таблицу статистики расчетов
      /*
      INSERT INTO tb_signs_calc_stat (sign_name,anlt_code,action,sec)
        VALUES(inSign,inAnltCode,'GLUING',(vEndTime - vTIBegin)*24*60*60);*/

      vEndTime := SYSDATE;
      vMes := 'SUCCESSFULLY :: "'||UPPER(inSign)||'" - '||vCou||' rows inserted into table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" in '||get_ti_as_hms(vEndTime - vTIBegin);
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);
    EXCEPTION WHEN OTHERS THEN
      vEndTime := SYSDATE;
      vMes := 'ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" - "'||UPPER(inSign)||'" not inserted :: '||SQLERRM;
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);
    END;
    vGluingStatus := SUBSTR(vMes,1,INSTR(vMes,' ') - 1);
    
    IF vGluingStatus = 'SUCCESSFULLY' THEN
      -- Удаление временной партиции
      vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' DROP PARTITION '||UPPER(inSign);
      BEGIN
        EXECUTE IMMEDIATE vBuff;
        vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||'" - Partition '||UPPER(inSign)||' dropped';
      EXCEPTION WHEN OTHERS THEN
        vMes := 'ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||'" - Partition '||UPPER(inSign)||' not dropped :: '||SQLERRM;
      END;
    ELSE
      vMes := 'ERROR :: GLUING STAGE ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||'" - Partition '||UPPER(inSign)||' not dropped';
    END IF;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);

    IF vTargetTruncate THEN
      IF vGluingStatus = 'SUCCESSFULLY' THEN
        HistTableService(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END,'111',inSign);
      ELSE
        vMes := 'ERROR :: GLUING STAGE ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" - Partition '||UPPER(inSign)||' service not required';
        pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);
      END IF;
    END IF;


    vMes := 'CONTINUE :: ------ Окончание склеивания '||UPPER(inSign)||' в целевую таблицу. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||'------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);

  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||inSign||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.sign_gluing" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.sign_gluing',vMes);
EXCEPTION WHEN OTHERS THEN
  vEndTime := SYSDATE;
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_ctr_signs.sign_gluing" :: '||SQLERRM||Chr(10)||vBuff;
  pr_log_write(lower(vOwner)||'.pkg_etl_ctr_signs.sign_gluing',vMes);
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_ctr_signs.sign_gluing" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors.';
  pr_log_write(lower(vOwner)||'.pkg_etl_ctr_signs.sign_gluing',vMes);
END sign_gluing;

PROCEDURE tmp_load_prev(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2)
  IS
    vDays INTEGER;
    vMes VARCHAR2(4000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    --
    vTmpTable VARCHAR2(256);
    vTmpATable VARCHAR2(256);
    vCou INTEGER := 0;
    vBuff VARCHAR2(32700);
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vDays := inEndDate - inBegDate;
  BEGIN
    SELECT UPPER(vOwner||'.'||e.tmp_table_name) AS tmp_table_name
          ,UPPER(vOwner||'.'||ae.tmp_table_name) AS tmp_a_table_name
      INTO vTmpTable,vTmpATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inEndDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||UPPER(inSign)||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;

  FOR idx IN 0..vDays
  LOOP
    vMes := 'CONTINUE :: "'||to_char(inBegDate+idx,'DD.MM.YYYY')||'" - "'||UPPER(inSign)||'" - loading started';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tmp_load_prev',vMes);

    vBuff :=
    'BEGIN'||CHR(10)||
    '  INSERT INTO '||lower(CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END)||' (effective_start,effective_end,obj_gid,source_system_id,sign_name,sign_val)'||CHR(10)||
    '     SELECT :1,last_day(:1),obj_gid,source_system_id,sign_name,sign_val'||CHR(10)||
    CASE WHEN inAnltCode IS NULL THEN
    '       FROM TABLE('||lower(vOwner)||'.pkg_etl_signs.get_sign(:2,:1)) WHERE sign_val IS NOT NULL;'||CHR(10)
    ELSE
    '       FROM TABLE('||lower(vOwner)||'.pkg_etl_signs.get_sign_anlt(:2,:1,:3,'||CASE WHEN UPPER(inSign) = UPPER(inAnltCode) THEN '1' ELSE '0' END||')) WHERE sign_val IS NOT NULL;'||CHR(10)
    END||
    CASE WHEN inAnltCode IS NULL THEN ':3 := SQL%ROWCOUNT;' ELSE ':4 := SQL%ROWCOUNT;' END||CHR(10)||
    'COMMIT;'||CHR(10)||
    'END;';

    BEGIN
      IF inAnltCode IS NULL THEN
        EXECUTE IMMEDIATE vBuff USING IN inBegDate+idx
               ,IN UPPER(inSign)
               ,OUT vCou;
      ELSE
        EXECUTE IMMEDIATE vBuff USING IN inBegDate+idx
               ,IN UPPER(inSign)
               ,IN UPPER(inAnltCode)
               ,OUT vCou;
      END IF;
      --dbms_output.put_line(vBuff);
    EXCEPTION WHEN OTHERS THEN
      vMes := 'ERROR :: "'||to_char(inBegDate+idx,'DD.MM.YYYY')||'" :: '||SQLERRM||Chr(10);
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.tmp_load_prev',vMes);
    END;

    vEndTime := SYSDATE;
    vMes := 'SUCCESSFULLY :: "'||to_char(inBegDate+idx,'DD.MM.YYYY')||'" - "'||UPPER(inSign)||'" '||vCou||' rows inserted into table "'||lower(CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END)||'" in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tmp_load_prev',vMes);
  END LOOP;
END tmp_load_prev;

PROCEDURE tmp_load_daily(inBegDate IN DATE,inEndDate IN DATE,inSign VARCHAR2,inAnltCode IN VARCHAR2)
  IS
    vDays INTEGER;
    vMes VARCHAR2(32700);
    vEndTime DATE;
    vTIBegin DATE;
    --
    vTmpTable VARCHAR2(256);
    vTmpATable VARCHAR2(256);
    vBuff VARCHAR2(32700);
    vSQL CLOB;
    vCou INTEGER := 0;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vDays := inEndDate - inBegDate;
  -- Получение наименования таблицы для загрузки
  BEGIN
    SELECT UPPER(vOwner||'.'||e.tmp_table_name) AS tmp_table_name
          ,UPPER(vOwner||'.'||ae.tmp_table_name) AS tmp_a_table_name
      INTO vTmpTable,vTmpATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inEndDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;

  FOR days IN 0..vDays LOOP
    vTIBegin := SYSDATE;
    dbms_lob.createtemporary(vSQL,FALSE);
    vBuff :=
    'DECLARE'||CHR(10)||
    '  vStr VARCHAR2(4000);'||CHR(10)||
    '  vCou INTEGER := 0;'||CHR(10)||
    '  vLogged BOOLEAN := FALSE;'||CHR(10)||
    'BEGIN'||CHR(10)||
    'EXECUTE IMMEDIATE ''ALTER SESSION SET nls_date_format = ''''DD.MM.RRRR HH24:MI:SS'''''';'||CHR(10)||
    'FOR idx IN ('||CHR(10)||
    '  SELECT /*+ LEADING(SRC) NO_INDEX(DEST)*/'||CHR(10)||
    '         :1 AS SRC_EFFECTIVE_START,'||CHR(10)||
    '         last_day(:1) AS SRC_EFFECTIVE_END,'||CHR(10)||
    '         SRC.OBJ_GID AS SRC_OBJ_GID,'||CHR(10)||
    '         SRC.SOURCE_SYSTEM_ID AS SRC_SOURCE_SYSTEM_ID,'||CHR(10)||
    '         SRC.SIGN_NAME AS SRC_SIGN_NAME,'||CHR(10)||
    '         SRC.SIGN_VAL AS SRC_SIGN_VAL,'||CHR(10)||
    '         DEST.SIGN_NAME AS D_SIGN_NAME,'||CHR(10)||
    '         DEST.EFFECTIVE_START AS D_EFFECTIVE_START,'||CHR(10)||
    '         DEST.SIGN_VAL AS D_SIGN_VAL'||CHR(10)||
    CASE WHEN inAnltCode IS NULL THEN
    '     FROM TABLE('||lower(vOwner)||'.pkg_etl_signs.get_sign(:2,:1)) src'||CHR(10)
    ELSE
    '     FROM TABLE('||lower(vOwner)||'.pkg_etl_signs.get_sign_anlt(:2,:1,:3,'||CASE WHEN UPPER(inSign) = UPPER(inAnltCode) THEN '1' ELSE '0' END||')) src'||CHR(10)
    END||
    '          LEFT JOIN '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' DEST'||CHR(10)||
    '            ON DEST.SIGN_NAME = :2'||CHR(10)||
    '               AND last_day(:1) = DEST.EFFECTIVE_END'||CHR(10)||
    '               AND DEST.OBJ_GID = SRC.OBJ_GID'||CHR(10)||
    '               AND DEST.SOURCE_SYSTEM_ID = SRC.SOURCE_SYSTEM_ID'||CHR(10)||
    '     WHERE '||UPPER(vOwner)||'.PKG_ETL_SIGNS.ISEQUAL(DEST.SIGN_VAL, SRC.SIGN_VAL) = 0'||CHR(10)||
    ') LOOP';
    dbms_lob.writeappend(vSQL,LENGTH(vBuff),vBuff);
    vBuff :=
    '  BEGIN'||CHR(10)||
    '      vStr := ''Upd|''||idx.src_sign_name;'||CHR(10)||
    '      UPDATE '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||CHR(10)||
    '        SET effective_end = idx.src_effective_start - 1'||CHR(10)||
    '        WHERE sign_name = UPPER(idx.src_sign_name)'||CHR(10)||
    '          AND effective_end = idx.src_effective_end'||CHR(10)||
    '          AND obj_gid = idx.src_obj_gid'||CHR(10)||
    '          AND source_system_id = idx.src_source_system_id;'||CHR(10)||
    '      IF idx.src_sign_val IS NOT NULL THEN'||CHR(10)||
    '        vStr := ''Ins|''||idx.src_sign_name;'||CHR(10)||
    '        INSERT INTO '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' (effective_start,effective_end,obj_gid,source_system_id,sign_name,sign_val)'||CHR(10)||
    '          VALUES (idx.src_effective_start'||CHR(10)||
    '                 ,idx.src_effective_end'||CHR(10)||
    '                 ,idx.src_obj_gid'||CHR(10)||
    '                 ,idx.src_source_system_id'||CHR(10)||
    '                 ,UPPER(idx.src_sign_name)'||CHR(10)||
    '                 ,idx.src_sign_val);'||CHR(10)||
    '      END IF;'||CHR(10)||
    '  EXCEPTION WHEN OTHERS THEN'||CHR(10)||
    '    IF NOT vLogged THEN'||CHR(10)||
    '      vStr := ''ERROR :: "'||UPPER(inSign)||'" - "''||to_char(idx.src_effective_start,''DD.MM.YYYY'')||''" - OBJ_SID = ''||idx.src_obj_gid*10+idx.src_source_system_id||'' :: ''||SQLERRM||Chr(10)||vStr;'||CHR(10)||
    '      '||lower(vOwner)||'.pkg_etl_signs.pr_log_write('''||lower(vOwner)||'.pkg_etl_signs.tmp_load_daily'',vStr);'||CHR(10)||
    '      vLogged := TRUE;'||CHR(10)||
    '    END IF;'||CHR(10)||
    '  END;'||CHR(10)||
    '  vCou := vCou + 1;'||CHR(10)||
    'END LOOP;'||CHR(10)||
    CASE WHEN inAnltCode IS NULL THEN ':3 := vCou;' ELSE ':4 := vCou;' END||CHR(10)||
    'COMMIT;'||CHR(10)||
    'END;';
    dbms_lob.writeappend(vSQL,LENGTH(vBuff),vBuff);
    IF inAnltCode IS NULL THEN
      EXECUTE IMMEDIATE vSQL USING IN inBegDate+days
             ,IN UPPER(inSign)
             ,OUT vCou;
    ELSE
      EXECUTE IMMEDIATE vSQL USING IN inBegDate+days
             ,IN UPPER(inSign)
             ,IN UPPER(inAnltCode)
             ,OUT vCou;
    END IF;
    vEndTime := SYSDATE;
    vMes := 'SUCCESSFULLY :: "'||UPPER(inSign)||'" - "'||to_char(inBegDate + days,'DD.MM.YYYY')||'" - '||vCou||' rows proccessed in table "'||lower(CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END)||'" in '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tmp_load_daily',vMes);
    dbms_lob.freetemporary(vSQL);
  END LOOP;
EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||UPPER(inSign)||'" - "'||to_char(inBegDate,'DD.MM.YYYY')||'" :: '||SQLERRM||Chr(10);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tmp_load_daily',vMes);
END tmp_load_daily;

PROCEDURE tb_upd_eff_end(inSign IN VARCHAR2,inAnltCode IN VARCHAR2,inDate IN DATE DEFAULT NULL)
  IS
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    --
    vHistTable VARCHAR2(256);
    vHistATable VARCHAR2(256);
    vBuff VARCHAR2(32700);
    vCou INTEGER := 0;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vMes := 'START :: "'||UPPER(inSign)||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.tb_upd_eff_end" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.load_sign',vMes);
  -- Получение наименования таблицы для апдейта
  BEGIN
    SELECT UPPER(vOwner||'.'||e.hist_table_name) AS hist_table_name
          ,UPPER(vOwner||'.'||ae.hist_table_name) AS hist_a_table_name
      INTO vHistTable,vHistATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND SYSDATE BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;
  vBuff :=
  'DECLARE'||CHR(10)||
  '  vCou INTEGER := 0;'||CHR(10)||
  'BEGIN'||CHR(10)||
  '  FOR idx IN ('||CHR(10)||
  CASE WHEN inDate IS NULL THEN
    '    SELECT sign_name,obj_gid,source_system_id,MAX(effective_end) AS effective_end'||CHR(10)||
    '      FROM '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||CHR(10)||
    '      WHERE sign_name = :1'||CHR(10)||
    '    GROUP BY sign_name,obj_gid,source_system_id'||CHR(10)||
    '    HAVING MAX(effective_end) != to_date(''31.12.5999'',''DD.MM.YYYY'')'||CHR(10)
  ELSE
    'WITH'||CHR(10)||
    '  a AS ('||CHR(10)||
    '    SELECT /*+ no_index(c)*/'||CHR(10)||
    '           obj_gid,source_system_id'||CHR(10)||
    '      FROM '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' c'||CHR(10)||
    '      WHERE sign_name = :1'||CHR(10)||
    '        AND effective_end = to_date(:2,''DD.MM.RRRR'')'||CHR(10)||
    '    MINUS'||CHR(10)||
    '    SELECT obj_gid,source_system_id'||CHR(10)||
    '      FROM '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||CHR(10)||
    '      WHERE sign_name = :1'||CHR(10)||
    '        AND effective_start > to_date(:2,''DD.MM.RRRR''))'||CHR(10)||
    'SELECT /*+ no_index(s) */'||CHR(10)||
    '       sign_name,obj_gid,source_system_id,effective_end'||CHR(10)||
    '  FROM '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' s'||CHR(10)||
    '  WHERE sign_name = :1'||CHR(10)||
    '    AND effective_end = to_date(:2,''DD.MM.RRRR'')'||CHR(10)||
    '    AND (obj_gid,source_system_id) IN (SELECT obj_gid,source_system_id FROM a)'||CHR(10)

  END||
  '  ) LOOP'||CHR(10)||
  '    UPDATE '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||CHR(10)||
  '      SET effective_end = to_date(''31.12.5999'',''DD.MM.YYYY'')'||CHR(10)||
  '      WHERE sign_name = idx.sign_name'||CHR(10)||
  '        AND obj_gid = idx.obj_gid'||CHR(10)||
  '        AND source_system_id = idx.source_system_id'||CHR(10)||
  '        AND effective_end = idx.effective_end;'||CHR(10)||
  '    vCou := vCou + 1;'||CHR(10)||
  '  END LOOP;'||CHR(10)||
  '  COMMIT;'||CHR(10)||
  CASE WHEN inDate IS NOT NULL THEN '  :3 := vCou;' ELSE '  :2 := vCou;' END||CHR(10)||
  'END;';

  IF inDate/*inAnltCode*/ IS NULL THEN
    EXECUTE IMMEDIATE vBuff USING IN UPPER(inSign),OUT vCou;
  ELSE
    EXECUTE IMMEDIATE vBuff USING IN UPPER(inSign),IN to_char(inDate,'DD.MM.RRRR'),OUT vCou;
  END IF;

  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: "'||UPPER(inSign)||'" - EFFECTIVE_END -> "31.12.5999" - '||vCou||' rows proccessed in table "'||lower(vHistTable)||'" in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_upd_eff_end',vMes);
  vMes := 'FINISH :: "'||inSign||'" - EFFECTIVE_END -> "31.12.5999" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.tb_upd_eff_end" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_upd_eff_end',vMes);
EXCEPTION WHEN OTHERS THEN
  vEndTime := SYSDATE;
  vMes := 'ERROR :: "'||UPPER(inSign)||'"  - EFFECTIVE_END -> "31.12.5999" :: '||SQLERRM||Chr(10)||vBuff;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_upd_eff_end',vMes);
  vMes := 'FINISH :: "'||inSign||'" - EFFECTIVE_END -> "31.12.5999" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.tb_upd_eff_end" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_upd_eff_end',vMes);
END tb_upd_eff_end;

PROCEDURE tb_load_mass(inBegDate IN DATE,inEndDate IN DATE,inSign IN VARCHAR2,inAnltCode IN VARCHAR2
  ,inMask IN VARCHAR2 DEFAULT '111111')
  IS
    vMes VARCHAR2(4000);
    vTIBegin DATE;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    --
    vCou INTEGER;
    vSPCode VARCHAR2(30);
    vTmpTable VARCHAR2(256);
    vHistTable VARCHAR2(256);
    vTmpATable VARCHAR2(256);
    vHistATable VARCHAR2(256);
    vIdx VARCHAR2(256);
    vBuff VARCHAR2(32700);
    vPrev BOOLEAN := SUBSTR(inMask,1,1) = '1';
    vDaily BOOLEAN := SUBSTR(inMask,2,1) = '1';
    vTruncateTarget BOOLEAN := SUBSTR(inMask,3,1) = '1';
    vLoadTarget BOOLEAN := SUBSTR(inMask,4,1) = '1';
    vCompress BOOLEAN := SUBSTR(inMask,5,1) = '1';
    vStats BOOLEAN := SUBSTR(inMask,6,1) = '1';
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.tb_load_mass" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

  -- Получение метаданных
  BEGIN
    SELECT p.sp_code
          ,UPPER(vOwner||'.'||e.tmp_table_name) AS tmp_table_name
          ,UPPER(vOwner||'.'||e.hist_table_name) AS hist_table_name
          ,UPPER(vOwner||'.'||ae.tmp_table_name) AS tmp_a_table_name
          ,UPPER(vOwner||'.'||ae.hist_table_name) AS hist_a_table_name
      INTO vSPCode
          ,vTmpTable
          ,vHistTable
          ,vTmpATable
          ,vHistATable
      FROM tb_signs_pool p
           INNER JOIN tb_entity e
             ON e.id = p.entity_id
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = p.sign_name
                AND s2a.anlt_code = UPPER(inAnltCode)
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inEndDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity ae
             ON ae.id = a.entity_id
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;

  -- Подготовка субпартиций в промежуточной таблице
  IF vPrev THEN
    vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' DROP PARTITION '||UPPER(inSign);
    BEGIN
      EXECUTE IMMEDIATE vBuff;
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;

    vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' ADD PARTITION '||UPPER(inSign)||' VALUES('''||UPPER(inSign)||''') STORAGE(INITIAL 64K NEXT 4M) NOLOGGING (SUBPARTITION '||vSPCode||'_OLD VALUES LESS THAN (to_date('''||to_char(TRUNC(inBegDate,'MM'),'DD.MM.YYYY')||''',''DD.MM.YYYY'')))';
    EXECUTE IMMEDIATE vBuff;

    FOR dt IN (
      SELECT TRUNC(dt,'MM') AS dt FROM (
      SELECT TRUNC(inEndDate,'MM') - ROWNUM + 1 AS dt FROM dual CONNECT BY ROWNUM <= TRUNC(inEndDate,'MM') - TRUNC(inBegDate,'MM') + 1
      ) GROUP BY TRUNC(dt,'MM') ORDER BY 1
    ) LOOP
      vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' MODIFY PARTITION '||UPPER(inSign)||' ADD SUBPARTITION '||vSPCode||'_'||to_char(dt.dt,'YYYYMM')||' VALUES LESS THAN (to_date('''||to_char(last_day(dt.dt)+1,'DD.MM.YYYY')||''',''DD.MM.YYYY''))';
      EXECUTE IMMEDIATE vBuff;
    END LOOP;
    vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vTmpTable ELSE vTmpATable END||' MODIFY PARTITION '||UPPER(inSign)||' ADD SUBPARTITION '||vSPCode||'_NEW VALUES LESS THAN (MAXVALUE)';
    EXECUTE IMMEDIATE vBuff;

    vTIBegin := SYSDATE;
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - загрузка первых чисел месяца в промежуточную таблицу --------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    -- Вставка данных в промежуточную таблицу за первые числа каждого месяца
    /*mass_load_parallel_by_year(TRUNC(inBegDate,'DD'),inEndDate
      ,lower(vOwner)||'.pkg_etl_signs.tmp_load_prev'
      ,'VARCHAR2 '||UPPER(inSign),FALSE,'01',inHeadJobName);*/
    mass_load_parallel_by_ydate_pe(TRUNC(inBegDate,'DD'),inEndDate
      ,lower(vOwner)||'.pkg_etl_signs.tmp_load_prev'
      ,'VARCHAR2 '||UPPER(inSign)||'::VARCHAR2 '||inAnltCode,FALSE,'01'/*,inHeadJobName*/);

    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - окончание загрузки первых чисел месяца в промежуточную таблицу. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
  END IF;

  IF vDaily THEN
    vTIBegin := SYSDATE;
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - прогрузка всех чисел месяца в промежуточную таблицу (распараллеливание по месяцам) --------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    FOR idx IN 2..31
      LOOP
        -- Прогрузка всех чисел месяца в промежуточную таблицу (распараллеливание по месяцам)
        /*mass_load_parallel_by_year(TRUNC(inBegDate,'DD'),inEndDate
          ,lower(vOwner)||'.pkg_etl_signs.tmp_load_daily'
          ,'VARCHAR2 '||UPPER(inSign),FALSE,to_char(idx,'00'),inHeadJobName);*/
        mass_load_parallel_by_ydate_pe(TRUNC(inBegDate,'DD'),inEndDate
          ,lower(vOwner)||'.pkg_etl_signs.tmp_load_daily'
          ,'VARCHAR2 '||UPPER(inSign)||'::VARCHAR2 '||inAnltCode,FALSE,to_char(idx,'00')/*,inHeadJobName*/);
      END LOOP;

    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - окончание прогрузки всех чисел месяца в промежуточную таблицу. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
  END IF;


  vTIBegin := SYSDATE;
  vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - Подготовка существующих данных в целевой таблице --------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

  IF vTruncateTarget THEN
    vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' TRUNCATE PARTITION '||UPPER(inSign);
    BEGIN
      EXECUTE IMMEDIATE vBuff;
      vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" - Partition '||UPPER(inSign)||' truncated';
    EXCEPTION WHEN OTHERS THEN
      vMes := 'ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" - Partition '||UPPER(inSign)||' not truncated :: '||SQLERRM;
    END;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
  ELSE
    IF vLoadTarget THEN
      -- Установка effective_end у записей, соответствующих дате начала периода
      vBuff :=
      'BEGIN'||CHR(10)||
      '  UPDATE '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' SET effective_end = to_date('''||to_char(inBegDate - 1,'DD.MM.YYYY')||''',''DD.MM.YYYY'')'||CHR(10)||
      '    WHERE sign_name = '''||UPPER(inSign)||''''||CHR(10)||
      '      AND to_date('''||to_char(inBegDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') BETWEEN effective_start AND effective_end'||CHR(10)||
      '      AND effective_start < to_date('''||to_char(inBegDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'');'||CHR(10)||
      '  :1 := SQL%ROWCOUNT;'||CHR(10)||
      '  COMMIT;'||CHR(10)||
      'END;';
      EXECUTE IMMEDIATE vBuff USING OUT vCou;
      vMes := 'SUCCESSFULLY :: ------- "'||UPPER(inSign)||'" - "effective_end" - '||vCou||' rows updated in table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'"';
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
      -- Установка effective_start у записей, соответствующих дате окончания периода
      vBuff :=
      'BEGIN'||CHR(10)||
      '  UPDATE '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' SET effective_start = to_date('''||to_char(last_day(inEndDate) + 1,'DD.MM.YYYY')||''',''DD.MM.YYYY'')'||CHR(10)||
      '    WHERE sign_name = '''||UPPER(inSign)||''''||CHR(10)||
      '      AND to_date('''||to_char(last_day(inEndDate),'DD.MM.YYYY')||''',''DD.MM.YYYY'') BETWEEN effective_start AND effective_end'||CHR(10)||
      '      AND effective_end > to_date('''||to_char(inEndDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'');'||CHR(10)||
      '  :1 := SQL%ROWCOUNT;'||CHR(10)||
      '  COMMIT;'||CHR(10)||
      'END;';
      EXECUTE IMMEDIATE vBuff USING OUT vCou;
      vMes := 'SUCCESSFULLY :: ------- "'||UPPER(inSign)||'" - "effective_start" - '||vCou||' rows updated in table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'"';
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
      -- Удаление записей, с effective_start больше или равно даты начала периода
      vBuff :=
      'BEGIN'||CHR(10)||
      'DELETE FROM '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||CHR(10)||
      '  WHERE sign_name = '''||UPPER(inSign)||''''||CHR(10)||
      '    AND effective_start BETWEEN to_date('''||to_char(inBegDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') AND to_date('''||to_char(last_day(inEndDate),'DD.MM.YYYY')||''',''DD.MM.YYYY'');'||CHR(10)||
      '  :1 := SQL%ROWCOUNT;'||CHR(10)||
      '  COMMIT;'||CHR(10)||
      'END;';
      EXECUTE IMMEDIATE vBuff USING OUT vCou;
      vMes := 'SUCCESSFULLY :: ------- "'||UPPER(inSign)||'" - '||vCou||' rows deleted from table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'"';
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
      -- Удаление записей, с effective_start больше или равно текущей даты (такие получаются когда считаем всё, по вчерашний день)
      vBuff :=
      'BEGIN'||CHR(10)||
      'DELETE FROM '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||CHR(10)||
      '  WHERE sign_name = '''||UPPER(inSign)||''''||CHR(10)||
      '    AND effective_start >= to_date('''||to_char(trunc(SYSDATE,'DD'),'DD.MM.YYYY')||''',''DD.MM.YYYY'');'||CHR(10)||
      '  :1 := SQL%ROWCOUNT;'||CHR(10)||
      '  COMMIT;'||CHR(10)||
      'END;';
      EXECUTE IMMEDIATE vBuff USING OUT vCou;
      vMes := 'SUCCESSFULLY :: ------- "'||UPPER(inSign)||'" - Technical Fictitious Future - '||vCou||' rows deleted from table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'"';
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    END IF;
  END IF;

  vEndTime := SYSDATE;
  vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - окончание подготовки существующих данных в целевой таблице. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

  IF vLoadTarget THEN
    vTIBegin := SYSDATE;
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - загрузка данных в целевую таблицу --------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    vTIBegin := SYSDATE;
    sign_gluing(UPPER(inSign),UPPER(inAnltCode),'010');

    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - окончание загрузки данных в целевую таблицу. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    -- Проставляем effective_end = 31.12.5999 на последних записях
    -- !!!ТОЛЬКО ЕСЛИ ДАТА ОКОНЧАНИЯ НЕ РАНЕЕ ВЧЕРАШНЕЙ ИНАЧЕ БУДЕТ ОШИБКА Unique constraint!!!
    IF last_day(inEndDate) >= TRUNC(SYSDATE - 1,'DD') THEN
       vMes := 'CONTINUE :: -------- "'||UPPER(inSign)||'" - EFFECTIVE_END -> "31.12.5999" апдейт последних записей ---------';
       pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

       vTIBegin := SYSDATE;
       tb_upd_eff_end(UPPER(inSign),UPPER(inAnltCode),last_day(inEndDate));

     ELSE
       vMes := 'SUCCESSFULLY :: ------- "'||UPPER(inSign)||'" Update of column EFFECTIVE_END on date "31.12.5999" not required';
       pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
     END IF;

     vEndTime := SYSDATE;
     vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - EFFECTIVE_END -> "31.12.5999" окончание апдейта последних записей. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
     pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
  END IF;

  IF vCompress THEN
    -- Сжатие данных в целевой таблице
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - сжатие данных в целевой таблице --------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    vTIBegin := SYSDATE;
    vBuff := 'ALTER TABLE '||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||' MOVE PARTITION '||UPPER(inSign)||' COMPRESS';
    BEGIN
      EXECUTE IMMEDIATE vBuff;
      vEndTime := SYSDATE;
      vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" - Partition '||UPPER(inSign)||' compressed in '||get_ti_as_hms(vEndTime - vTIBegin);
    EXCEPTION WHEN OTHERS THEN
      vEndTime := SYSDATE;
      vMes := 'ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" - Partition '||UPPER(inSign)||' not compressed :: '||SQLERRM||Chr(10)||'------'||Chr(10)||'Execution time: '||get_ti_as_hms(vEndTime - vTIBegin);
    END;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - окончание сжатия данных в целевой таблице. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    -- Перестроение индексов в целевой таблице
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - перестроение индексов в целевой таблице --------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    -- Получение наименования индекса
    SELECT index_name INTO vIdx FROM all_indexes
      WHERE owner = UPPER(vOwner) AND table_name = UPPER(SUBSTR(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END,INSTR(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END,'.',1,1) + 1,LENGTH(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END) - INSTR(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END,'.',1,1)))
        AND uniqueness = 'UNIQUE'
        AND index_name LIKE 'UIX%';

    vTIBegin := SYSDATE;
    vBuff := 'ALTER INDEX '||lower(vOwner||'.'||vIdx)||' REBUILD PARTITION '||UPPER(inSign);
    BEGIN
      EXECUTE IMMEDIATE vBuff;
      vEndTime := SYSDATE;
      vMes := 'SUCCESSFULLY :: Index "'||lower(vOwner||'.'||vIdx)||'" - Partition '||UPPER(inSign)||' rebuilded in '||get_ti_as_hms(vEndTime - vTIBegin);
    EXCEPTION WHEN OTHERS THEN
      vEndTime := SYSDATE;
      vMes := 'ERROR :: Index "'||lower(vOwner||'.'||vIdx)||'" - Partition '||UPPER(inSign)||' not rebuilded :: '||SQLERRM||Chr(10)||'------'||Chr(10)||'Execution time: '||get_ti_as_hms(vEndTime - vTIBegin);
    END;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - окончание перестроения индексов в целевой таблице. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
  END IF;

  IF vStats THEN
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - сбор статистики по целевой таблице --------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    vTIBegin := SYSDATE;
    vBuff := 'BEGIN dbms_stats.gather_table_stats('''||UPPER(vOwner)||''','''||UPPER(SUBSTR(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END,INSTR(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END,'.',1,1) + 1,LENGTH(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END) - INSTR(CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END,'.',1,1)))||''','''||UPPER(inSign)||''',20); END;';
    BEGIN
      EXECUTE IMMEDIATE vBuff;
      vEndTime := SYSDATE;
      vMes := 'SUCCESSFULLY :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" - Partition '||UPPER(inSign)||' Statistic gathered in '||get_ti_as_hms(vEndTime - vTIBegin);
    EXCEPTION WHEN OTHERS THEN
      vEndTime := SYSDATE;
      vMes := 'ERROR :: Table "'||CASE WHEN inAnltCode IS NULL THEN vHistTable ELSE vHistATable END||'" - Partition '||UPPER(inSign)||' Statistic not gathered :: '||SQLERRM||Chr(10)||'------'||Chr(10)||'Execution time: '||get_ti_as_hms(vEndTime - vTIBegin);
    END;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);

    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------- "'||UPPER(inSign)||'" - окончание сбора статистики по целевой таблице. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||inSign||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.tb_load_mass" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
EXCEPTION
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
    vMes := 'FINISH :: "'||inSign||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.tb_load_mass" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.tb_load_mass',vMes);
END tb_load_mass;

PROCEDURE SignExtProcessing(inSign IN VARCHAR2,inDate IN DATE)
  IS
    vStmt CLOB;
    vRes VARCHAR2(32700);
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vMes := 'START :: "'||UPPER(inSign)||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.SignExtProcessing" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.SignExtProcessing',vMes);

  -- Получение ext_plsql
  BEGIN
    SELECT ext_plsql
      INTO vStmt
      FROM tb_signs_pool p
      WHERE p.sign_name = UPPER(inSign);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Показатель "'||inSign||'" не найден в таблице '||lower(vOwner)||'.tb_signs_pool');
  END;

  EXECUTE IMMEDIATE 'ALTER SESSION SET nls_date_format = ''DD.MM.RRRR HH24:MI:SS''';
  -- Обработка
  EXECUTE IMMEDIATE vStmt USING IN UPPER(inSign),IN inDate,OUT vRes;

  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: "'||UPPER(inSign)||'" extended processing :: '||vRes;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.SignExtProcessing',vMes);
  vMes := 'FINISH :: "'||UPPER(inSign)||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.SignExtProcessing" finished successfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.SignExtProcessing',vMes);
EXCEPTION WHEN OTHERS THEN
  vEndTime := SYSDATE;
  vMes := 'ERROR :: "'||UPPER(inSign)||'" extended processing :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.SignExtProcessing',vMes);
  vMes := 'FINISH :: "'||UPPER(inSign)||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.SignExtProcessing" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.SignExtProcessing',vMes);
END;


FUNCTION get_empty_sign_id RETURN NUMBER
  IS
    vRes NUMBER;
BEGIN
  SELECT MAX(ID) + 1 INTO vRes FROM tb_signs_pool;

  WITH
    digit AS (
     SELECT LEVEL AS ID FROM dual CONNECT BY ROWNUM <= vRes
    )
  SELECT MIN(d_id) AS ID INTO vRes
    FROM (SELECT digit.id AS d_id,p.id AS p_id FROM digit LEFT JOIN tb_signs_pool p ON p.id = digit.id)
    WHERE p_id IS NULL;
  RETURN vRes;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN vRes;
  WHEN OTHERS THEN
    RETURN -1;
END get_empty_sign_id;

FUNCTION DropSignPartitions(inSign IN VARCHAR2) RETURN VARCHAR2
  IS
    vOut VARCHAR2(4000);
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vOut := '------------';
  FOR idx IN (
    WITH
      fct AS (
        SELECT UPPER(fct_table_name) AS table_name
          FROM tb_signs_pool p
               INNER JOIN tb_entity e ON e.id = p.entity_id
          WHERE p.sign_name = UPPER(inSign)
            AND p.hist_flg = 0
        UNION
        SELECT UPPER(e.fct_table_name)
          FROM tb_signs_pool p
               INNER JOIN tb_sign_2_anlt s2a ON s2a.sign_name = p.sign_name
               INNER JOIN tb_signs_anlt a
                 ON a.anlt_code = s2a.anlt_code
                    AND SYSDATE BETWEEN a.effective_start AND a.effective_end
               INNER JOIN tb_entity e
                 ON e.id = a.entity_id
          WHERE p.sign_name = UPPER(inSign)
            AND p.hist_flg = 0
      )
     ,hist AS (
        SELECT UPPER(e.hist_table_name) AS table_name
          FROM tb_signs_pool p
               INNER JOIN tb_entity e ON e.id = p.entity_id
          WHERE p.sign_name = UPPER(inSign)
            AND p.hist_flg = 1
        UNION
        SELECT UPPER(e.tmp_table_name)
          FROM tb_signs_pool p
               INNER JOIN tb_entity e ON e.id = p.entity_id
          WHERE p.sign_name = UPPER(inSign)
            AND p.hist_flg = 1
        UNION
        SELECT UPPER(e.hist_table_name)
          FROM tb_signs_pool p
               INNER JOIN tb_sign_2_anlt s2a ON s2a.sign_name = p.sign_name
               INNER JOIN tb_signs_anlt a
                 ON a.anlt_code = s2a.anlt_code
                    AND SYSDATE BETWEEN a.effective_start AND a.effective_end
               INNER JOIN tb_entity e
                 ON e.id = a.entity_id
          WHERE p.sign_name = UPPER(inSign)
            AND p.hist_flg = 1
        UNION
        SELECT UPPER(e.tmp_table_name)
          FROM tb_signs_pool p
               INNER JOIN tb_sign_2_anlt s2a ON s2a.sign_name = p.sign_name
               INNER JOIN tb_signs_anlt a
                 ON a.anlt_code = s2a.anlt_code
                    AND SYSDATE BETWEEN a.effective_start AND a.effective_end
               INNER JOIN tb_entity e
                 ON e.id = a.entity_id
          WHERE p.sign_name = UPPER(inSign)
            AND p.hist_flg = 1
      )

    SELECT fct.table_name,prt.partition_name
      FROM fct
           INNER JOIN all_tab_partitions prt
             ON prt.table_owner = UPPER(vOwner)
                AND prt.table_name = fct.table_name
                AND prt.partition_name = UPPER(inSign)
    UNION
    SELECT hist.table_name,prt.partition_name
      FROM hist
           INNER JOIN all_tab_partitions prt
             ON prt.table_owner = UPPER(vOwner)
                AND prt.table_name = hist.table_name
                AND prt.partition_name = UPPER(inSign)
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE '||lower(vOwner||'.'||idx.table_name)||' DROP PARTITION '||UPPER(inSign);
      vOut := vOut||CHR(10)||'SUCCESSFULLY :: Table "'||lower(vOwner||'.'||idx.table_name)||'" - Partition "'||UPPER(inSign)||'" dropped';
      --dbms_output.put_line(idx.table_name||'|'||idx.partition_name);
    EXCEPTION WHEN OTHERS THEN
      vOut := vOut||CHR(10)||'ERROR :: Table "'||lower(vOwner||'.'||idx.table_name)||'" - Partition "'||UPPER(inSign)||'" not dropped :: '||SQLERRM;
    END;
  END LOOP;
  RETURN vOut;
END DropSignPartitions;

PROCEDURE drop_sign(inSign IN VARCHAR2,outRes OUT VARCHAR2)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  -- Удаление партиции
  outRes := DropSignPartitions(UPPER(inSign));

  -- Удаление привязки к аналитикам
  BEGIN
    DELETE FROM tb_sign_2_anlt WHERE sign_name = UPPER(inSign);
    outRes := outRes||CHR(10)||'------------'||CHR(10)||'SUCCESSFULLY :: "'||UPPER(inSign)||'" - Удалено '||SQL%ROWCOUNT||' привязок к аналитикам';
  EXCEPTION WHEN OTHERS THEN
    outRes := outRes||CHR(10)||'------------'||CHR(10)||'ERROR :: "'||UPPER(inSign)||'" - не возможно удалить привязки к аналитикам :: '||SQLERRM;
    RAISE_APPLICATION_ERROR(-20000,outRes);
  END;

  -- Удаление привязки к группе
  BEGIN
    DELETE FROM tb_signs_2_group WHERE sign_name = UPPER(inSign);
    outRes := outRes||CHR(10)||'SUCCESSFULLY :: "'||UPPER(inSign)||'" - Удалено '||SQL%ROWCOUNT||' привязок к группам';
  EXCEPTION WHEN OTHERS THEN
    outRes := outRes||CHR(10)||'ERROR :: "'||UPPER(inSign)||'" - не возможно удалить привязки к группам :: '||SQLERRM;
    RAISE_APPLICATION_ERROR(-20000,outRes);
  END;
  -- Удаление зависимости от других показателей
  BEGIN
    DELETE FROM tb_sign_2_sign WHERE sign_name = UPPER(inSign) OR prev_sign_name = UPPER(inSign);
    outRes := outRes||CHR(10)||'SUCCESSFULLY :: "'||UPPER(inSign)||'" - Удалено '||SQL%ROWCOUNT||' зависимостей от других показателей';
  EXCEPTION WHEN OTHERS THEN
    outRes := outRes||CHR(10)||'ERROR :: "'||UPPER(inSign)||'" - не возможно удалить зависимости от других показателей :: '||SQLERRM;
    RAISE_APPLICATION_ERROR(-20000,outRes);
  END;

  -- Удаление из списка показателей
  BEGIN
    DELETE FROM tb_signs_pool WHERE sign_name = UPPER(inSign);
    outRes := outRes||CHR(10)||'------------'||CHR(10)||'SUCCESSFULLY :: "'||UPPER(inSign)||'" - Показатель удален из списка показателей';
  EXCEPTION WHEN OTHERS THEN
    outRes := outRes||CHR(10)||'------------'||CHR(10)||'ERROR :: "'||UPPER(inSign)||'" - не возможно удалить показатель в таблице "'||lower(vOwner)||'.tb_signs_pool" :: '||SQLERRM;
  END;
EXCEPTION WHEN OTHERS THEN
  outRes := outRes||CHR(10)||'------------'||CHR(10)||'ERROR :: "'||UPPER(inSign)||'" - '||SQLERRM;
END drop_sign;

FUNCTION GetTreeList(inSQL IN CLOB) RETURN TTabTree PIPELINED
  IS
    rec TRecTree;
      cur INTEGER;       -- хранит идентификатор (ID) курсора
      ret INTEGER;       -- хранит возвращаемое по вызову значение
BEGIN
  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur, inSQL, dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.Id,4000);
  dbms_sql.define_column(cur,2,rec.ParentId,4000);
  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.Id);
    dbms_sql.column_value(cur,2,rec.ParentId);
    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);
END;

FUNCTION GetChainList(inSQL IN CLOB) RETURN TTabCHBuilder PIPELINED
  IS
    rec TRecCHBuilder;
    cur INTEGER;       -- хранит идентификатор (ID) курсора
    ret INTEGER;       -- хранит возвращаемое по вызову значение
BEGIN
    cur := dbms_sql.open_cursor;
    dbms_sql.parse(cur, inSQL, dbms_sql.native);
    dbms_sql.define_column(cur,1,rec.id,4000);
    dbms_sql.define_column(cur,2,rec.parent_id,4000);
    dbms_sql.define_column(cur,3,rec.unit,4000);
    dbms_sql.define_column(cur,4,rec.params,4000);
    dbms_sql.define_column(cur,5,rec.skip);

    ret := dbms_sql.execute(cur);

    LOOP
      EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
      dbms_sql.column_value(cur,1,rec.id);
      dbms_sql.column_value(cur,2,rec.parent_id);
      dbms_sql.column_value(cur,3,rec.unit);
      dbms_sql.column_value(cur,4,rec.params);
      dbms_sql.column_value(cur,5,rec.skip);
      PIPE ROW(rec);
    END LOOP;
    dbms_sql.close_cursor(cur);
END GetChainList;

FUNCTION GetTreeSQL(inFullSQL IN CLOB
                   ,inStartSQL IN CLOB
                   ,inIncludeChilds IN INTEGER DEFAULT 0)
  RETURN CLOB
  IS
    vRes CLOB;
    vBuff VARCHAR2(32700);
    vCou INTEGER :=0;
    vStartSQL CLOB := inStartSQL;
BEGIN
  IF inStartSQL IS NULL THEN vStartSQL := inFullSQL; END IF;

  dbms_lob.createtemporary(vRes,FALSE);

  IF inIncludeChilds = 0 AND inStartSQL IS NOT NULL THEN
    FOR idx IN (
      WITH
        f AS (SELECT * FROM TABLE(GetTreeList(inFullSQL)))
       ,s AS (SELECT * FROM TABLE(GetTreeList(vStartSQL)))
       SELECT DISTINCT s.id,f.parentid
         FROM s INNER JOIN f ON f.id = s.id
         WHERE NVL(f.parentid,s.id) IN (SELECT ID FROM s)
    ) LOOP
      vBuff := CASE WHEN vCou > 0 THEN CHR(10)||'UNION ALL'||CHR(10) END||'SELECT '''||idx.id||''' AS id,'||CASE WHEN idx.parentid IS NOT NULL THEN ''''||idx.parentid||'''' ELSE 'NULL' END||' AS PARENT_ID FROM dual';
      dbms_lob.writeappend(vRes,LENGTH(vBuff),vBuff);
      vCou := vCou + 1;
    END LOOP;
  ELSIF inIncludeChilds = 1  AND inStartSQL IS NOT NULL THEN
    FOR idx IN (
    WITH
      f AS (SELECT * FROM TABLE(GetTreeList(inFullSQL)))
     ,s AS (SELECT * FROM TABLE(GetTreeList(vStartSQL)))
     ,c AS (
        SELECT ID,parentid FROM s
        UNION ALL
        SELECT ID,parentid FROM (
          SELECT ID,parentid FROM f
          MINUS
          SELECT ID,parentid FROM s)
      )
      SELECT DISTINCT ID,parentid FROM (
        SELECT ID,parentid
          FROM c CONNECT BY PRIOR ID = parentid START WITH id IN (SELECT ID FROM s)
      ) WHERE parentid IS NULL OR parentid IN (SELECT ID FROM s)
    ) LOOP
      vBuff := CASE WHEN vCou > 0 THEN CHR(10)||'UNION ALL'||CHR(10) END||'SELECT '''||idx.id||''' AS id,'||CASE WHEN idx.parentid IS NOT NULL THEN ''''||idx.parentid||'''' ELSE 'NULL' END||' AS PARENT_ID FROM dual';
      dbms_lob.writeappend(vRes,LENGTH(vBuff),vBuff);
      vCou := vCou + 1;
    END LOOP;
  ELSE
    FOR idx IN (
      WITH
        f AS (SELECT * FROM TABLE(GetTreeList(inFullSQL)))
       SELECT DISTINCT f.id,f.parentid
         FROM f
    ) LOOP
      vBuff := CASE WHEN vCou > 0 THEN CHR(10)||'UNION ALL'||CHR(10) END||'SELECT '''||idx.id||''' AS id,'||CASE WHEN idx.parentid IS NOT NULL THEN ''''||idx.parentid||'''' ELSE 'NULL' END||' AS PARENT_ID FROM dual';
      dbms_lob.writeappend(vRes,LENGTH(vBuff),vBuff);
      vCou := vCou + 1;
    END LOOP;
  END IF;
  RETURN vRes;
END GetTreeSQL;

FUNCTION ChainBuilder(inSQL CLOB) RETURN VARCHAR2
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vID VARCHAR2(30) := to_char(tb_signs_job_id_seq.nextval);
    vChainName VARCHAR2(256) := vOwner||'.CHAIN_'||vID;
    vBuff VARCHAR2(32700);
    vPrg CLOB;
    vArg CLOB;
    vStp CLOB;
    vRul CLOB;
    vAct CLOB;
    ErrAct VARCHAR2(256);
    ErrComm VARCHAR2(256);
    PrgCou INTEGER := 0;
BEGIN
  
  -- Программы
  dbms_lob.createtemporary(vPrg,FALSE);
  dbms_lob.writeappend(vPrg,LENGTH('BEGIN'||CHR(10)),'BEGIN'||CHR(10));

  vBuff :=
  '  sys.dbms_scheduler.create_program(program_name        => '''||vOwner||'.PRG_START_'||vID||''','||CHR(10)||
  '                                    program_type        => ''PLSQL_BLOCK'','||CHR(10)||
  '                                    program_action      => ''BEGIN NULL; END;'','||CHR(10)||
  '                                    enabled             => true,'||CHR(10)||
  '                                    comments            => ''Старт'');'||CHR(10);
  dbms_lob.writeappend(vPrg,LENGTH(vBuff),vBuff);

  FOR idx IN (
    WITH
      p AS (
        SELECT /*+ materialize */ id,parent_id,unit,params,skip FROM TABLE(GetChainList(inSQL))
      )
    SELECT DISTINCT
           vOwner||'.PRG_'||ora_hash(p.id)||'_'||vID AS prg_name
          ,lower(p.unit) AS action
          ,ID AS comm
          ,SUM(NVL2(a.OBJECT_ID,1,0)) OVER (PARTITION BY p.id,p.parent_id) AS arg_cou
      FROM p
           LEFT JOIN all_procedures prc
             ON lower(prc.owner||NVL2(prc.object_name,'.'||prc.object_name,NULL)||NVL2(prc.procedure_name,'.'||prc.procedure_name,NULL)) = lower(p.unit)
           LEFT JOIN all_arguments a
             ON a.OBJECT_ID = prc.object_id AND a.argument_name IS NOT NULL
                AND a.object_name = NVL(prc.procedure_name,prc.object_name)
  ) LOOP
     IF PrgCou <=1 THEN ErrAct := idx.action; ErrComm := idx.comm; END IF;
     PrgCou := PrgCou + 1;
      vBuff :=
      '  sys.dbms_scheduler.create_program(program_name        => '''||idx.prg_name||''','||CHR(10)||
      '                                    program_type        => ''STORED_PROCEDURE'','||CHR(10)||
      '                                    program_action      => '''||idx.action||''','||CHR(10)||
      '                                    number_of_arguments => '||idx.arg_cou||','||CHR(10)||
      '                                    enabled             => false,'||CHR(10)||
      '                                    comments            => '''||idx.comm||''');'||CHR(10);
      dbms_lob.writeappend(vPrg,length(vBuff),vBuff);
  END LOOP;
  dbms_lob.writeappend(vPrg,LENGTH('END;'),'END;');

  -- Параметры программ
  dbms_lob.createtemporary(vArg,FALSE);
  dbms_lob.writeappend(vArg,LENGTH('BEGIN'||CHR(10)),'BEGIN'||CHR(10));
 
  FOR idx IN (
    WITH
      p AS (
        SELECT /*+ materialize */ id,parent_id,unit,params,skip FROM TABLE(GetChainList(inSQL))
      )
    SELECT vOwner||'.PRG_'||ora_hash(p.id)||'_'||vID AS prg_name
          ,v.ord AS arg_position
          ,a.argument_name AS arg_name
          ,a.data_type arg_type
          ,v.str AS arg_value
      FROM p
           CROSS JOIN TABLE(parse_str(p.params,'#!#')) v
           LEFT JOIN all_procedures prc
             ON lower(prc.owner||NVL2(prc.object_name,'.'||prc.object_name,NULL)||NVL2(prc.procedure_name,'.'||prc.procedure_name,NULL)) = lower(p.unit)
           LEFT JOIN all_arguments a
             ON a.OBJECT_ID = prc.object_id
                AND a.object_name = NVL(prc.procedure_name,prc.object_name)
                AND a.position = v.ord
      WHERE a.argument_name IS NOT NULL
    GROUP BY v.ord,p.id,a.argument_name,a.data_type,v.ord,v.str
    ORDER BY p.id,v.ord
  ) LOOP
      vBuff :=
      '  BEGIN'||CHR(10)||
      '  sys.dbms_scheduler.define_program_argument(program_name => '''||idx.prg_name||''','||CHR(10)||
      '                                             argument_position => '||idx.arg_position||','||CHR(10)||
      '                                             argument_name     => '''||idx.arg_name||''','||CHR(10)||
      '                                             argument_type     => '''||idx.arg_type||''','||CHR(10)||
      '                                             default_value     => q''{'||idx.arg_value||'}'');'||CHR(10)||
      '  EXCEPTION WHEN OTHERS THEN'||CHR(10)||
      '    '||LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ChainBuilder'',q''{'||idx.arg_value||'}'');'||CHR(10)||
      '  END;';
      dbms_lob.writeappend(vArg,length(vBuff),vBuff);
  END LOOP;
  dbms_lob.writeappend(vArg,LENGTH('END;'),'END;');

  -- Цепь и шаги
  dbms_lob.createtemporary(vStp,FALSE);
  dbms_lob.writeappend(vStp,LENGTH('BEGIN'||CHR(10)),'BEGIN'||CHR(10));

  vBuff :=
  '  sys.dbms_scheduler.create_chain(chain_name          => '''||vChainName||''','||CHR(10)||
  '                                  evaluation_interval => INTERVAL ''3'' MINUTE,'||CHR(10)||
  '                                  comments            => ''Головной CHAIN'');'||CHR(10);
  dbms_lob.writeappend(vStp,length(vBuff),vBuff);

  vBuff :=
  '  sys.dbms_scheduler.define_chain_step(chain_name   => '''||vChainName||''','||CHR(10)||
  '                                       step_name    => ''STP_START'','||CHR(10)||
  '                                       program_name => '''||vOwner||'.PRG_START_'||vID||''');'||CHR(10);
  dbms_lob.writeappend(vStp,length(vBuff),vBuff);

  FOR idx IN (
    SELECT DISTINCT
           'STP_'||ora_hash(p.id) AS stp_name
          ,vOwner||'.PRG_'||ora_hash(ID)||'_'||vID AS prg_name
          ,skip
      FROM TABLE(GetChainList(inSQL)) p
  ) LOOP
    vBuff :=
    '  sys.dbms_scheduler.define_chain_step(chain_name   => '''||vChainName||''','||CHR(10)||
    '                                       step_name    => '''||idx.stp_name||''','||CHR(10)||
    '                                       program_name => '''||idx.prg_name||''');'||CHR(10);
    dbms_lob.writeappend(vStp,length(vBuff),vBuff);

    IF idx.skip = 1 THEN
      vBuff :=
      'dbms_scheduler.alter_chain(chain_name  =>  '''||vChainName||''','||CHR(10)||
      'step_name   =>  '''||idx.stp_name||''','||CHR(10)||
      'attribute   =>  ''SKIP'','||CHR(10)||
      'value       =>  TRUE);'||CHR(10);
      dbms_lob.writeappend(vStp,length(vBuff),vBuff);
    END IF;
  END LOOP;
  dbms_lob.writeappend(vStp,LENGTH('END;'),'END;');

  -- Правила
  dbms_lob.createtemporary(vRul,FALSE);
  dbms_lob.writeappend(vRul,LENGTH('BEGIN'||CHR(10)),'BEGIN'||CHR(10));

  vBuff :=
  '  sys.dbms_scheduler.define_chain_rule(chain_name => '''||vChainName||''','||CHR(10)||
  '                                       rule_name  => '''||vOwner||'.RUL_START_'||vID||''','||CHR(10)||
  '                                       condition  => ''TRUE'','||CHR(10)||
  '                                       action     => ''START "STP_START"'','||CHR(10)||
  '                                       comments   => ''Старт'');'||CHR(10);
  dbms_lob.writeappend(vRul,length(vBuff),vBuff);

  FOR idx IN (
    SELECT 'STP_'||ora_hash(p.id) AS stp_name
          ,vOwner||'.RUL_'||ora_hash(ID)||'_'||vID AS rul_name
          ,LISTAGG(CASE WHEN parent_id IS NOT NULL THEN 'STP_'||ora_hash(parent_id)||' COMPLETED' END,' AND ') WITHIN GROUP (ORDER BY ora_hash(parent_id)) AS cond
          ,p.id
      FROM TABLE(GetChainList(inSQL)) p
    GROUP BY p.id
  ) LOOP
    vBuff :=
    '  sys.dbms_scheduler.define_chain_rule(chain_name => '''||vChainName||''','||CHR(10)||
    '                                       rule_name  => '''||idx.rul_name||''','||CHR(10)||
    '                                       condition  => '''||NVL(idx.cond,'STP_START COMPLETED')||''','||CHR(10)||
    '                                       action     => ''START "'||idx.stp_name||'"'','||CHR(10)||
    '                                       comments   => '''||idx.id||''');'||CHR(10);
    dbms_lob.writeappend(vRul,length(vBuff),vBuff);
  END LOOP;
  dbms_lob.writeappend(vRul,LENGTH('END;'),'END;');

  -- Активация программ и цепи
  dbms_lob.createtemporary(vAct,FALSE);
  dbms_lob.writeappend(vAct,LENGTH('BEGIN'||CHR(10)),'BEGIN'||CHR(10));

  FOR idx IN (
    SELECT DISTINCT
           vOwner||'.PRG_'||ora_hash(p.id)||'_'||vID AS prg_name
          ,unit
          ,params
      FROM TABLE(GetChainList(inSQL)) p
  ) LOOP
      vBuff :=
      'BEGIN  '||CHR(10)||
      '  sys.dbms_scheduler.enable('''||idx.prg_name||''');'||CHR(10)||
      'EXCEPTION WHEN OTHERS THEN'||CHR(10)||
      '  '||LOWER(vOWner)||'.pkg_etl_signs.pr_log_write('''||lower(vOwner)||'.pkg_etl_signs.ChainBuilder'',q''{'||idx.unit||' :: '||idx.params||' :: Program "'||idx.prg_name||'" has been dropped}'');'||CHR(10)||
      '  sys.dbms_scheduler.drop_program('''||idx.prg_name||''',TRUE);'||CHR(10)||
      'END;';
      dbms_lob.writeappend(vAct,length(vBuff),vBuff);
  END LOOP;

  vBuff :=
  '  sys.dbms_scheduler.enable('''||vChainName||''');'||CHR(10);
  dbms_lob.writeappend(vAct,length(vBuff),vBuff);

  dbms_lob.writeappend(vAct,LENGTH('END;'),'END;');

  EXECUTE IMMEDIATE vPrg;
  --dbms_output.put_line(vPrg);
  
  EXECUTE IMMEDIATE vArg;
  --dbms_output.put_line(vArg);
  
  EXECUTE IMMEDIATE vStp;
  --dbms_output.put_line(vStp);
  
  EXECUTE IMMEDIATE vRul;
  --dbms_output.put_line(vRul);
  
  EXECUTE IMMEDIATE vAct;
  --dbms_output.put_line(vAct);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ChainBuilder','SUCCESSFULLY :: Chain '||vChainName||' - Action '||ErrAct||' - Comments '||ErrComm||' builded');
  
  RETURN vChainName;
EXCEPTION WHEN OTHERS THEN
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ChainBuilder','ERROR :: '||SQLERRM);
  RETURN vChainName;
END ChainBuilder;

FUNCTION ChainStarter(inChainName IN VARCHAR2,inHeadJobName IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vRes VARCHAR2(4000);
    vJobName VARCHAR2(256) := NVL(inHeadJobName,vOwner||'.CHAINJOB_'||to_char(SYSDATE,'RRRRMMDDHH24MISS'));
    vBuff VARCHAR2(32700);
BEGIN
  vBuff :=
  'BEGIN'||CHR(10)||
  '  EXECUTE IMMEDIATE ''ALTER SESSION SET nls_numeric_characters = '''', '''''';'||CHR(10)||
  '  EXECUTE IMMEDIATE ''ALTER SESSION SET nls_date_format = ''''DD.MM.RRRR HH24:MI:SS'''''';'||CHR(10)||
  'END;';
  EXECUTE IMMEDIATE vBuff;
  
  vRes := inChainName;
  sys.dbms_scheduler.run_chain(inChainName,'STP_START',vJobName);
  RETURN vRes;
EXCEPTION WHEN OTHERS THEN
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ChainStarter','ERROR :: '||SQLERRM);
  RETURN vRes;
END ChainStarter;

PROCEDURE ChainKiller(inChainName VARCHAR2)
  IS
  vRunChCou INTEGER := 1;
  curPrg SYS_REFCURSOR;
  vPrgName VARCHAR2(256);
  vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  -- Ожидание пока отработает цепь
  LOOP
    SELECT COUNT(1) INTO vRunChCou
      FROM all_scheduler_running_chains
      WHERE lower(owner)||'.'||lower(chain_name) = LOWER(inChainName)
        AND completed = 'FALSE';
    EXIT WHEN vRunChCou = 0;
    -- ждем 10 секунд, затем проверяем снова
    --stage.mysleep(10);
    dbms_lock.sleep(10);
  END LOOP;

  -- Открытие курсора с наименованиями программ
  OPEN curPrg FOR
    SELECT LOWER(owner||'.'||program_name) AS prg_name
      FROM all_scheduler_chain_steps
      WHERE lower(owner)||'.'||lower(chain_name) = LOWER(inChainName);

  -- Удаление цепи
  BEGIN
    sys.dbms_scheduler.drop_chain(LOWER(inChainName),TRUE);
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  -- Удаление программ
  LOOP
    FETCH curPrg INTO vPrgName;
    EXIT WHEN curPrg%NOTFOUND;
    BEGIN
      sys.dbms_scheduler.drop_program(vPrgName,TRUE);
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;

  CLOSE curPrg;
EXCEPTION WHEN OTHERS THEN
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ChainKiller','ERROR :: '||SQLERRM);
END ChainKiller;

PROCEDURE calc(inBegDate IN DATE,inEndDate IN DATE,inSendMessage BOOLEAN DEFAULT FALSE)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'AUTOCALC_'||tb_signs_job_id_seq.nextval;
    vBuff VARCHAR2(32700);
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.RRRR');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.RRRR');
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vErrCou NUMBER := 0;
BEGIN
  vMes := 'START :: "'||vBegDate||'" :: Procedure '||UPPER(vOwner)||'.PKG_ETL_SIGNS.CALC started';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.calc',vMes);
   
  vBuff :=
    'SELECT p.id,c2c.parent_id,p.e_unit AS unit'||CHR(10)||
    '      ,REPLACE(REPLACE(p.params,'':INBEGDATE'','''||vBegDate||'''),'':INENDDATE'','''||vEndDate||''') AS params'||CHR(10)||
    '      ,CASE WHEN p.archive_flag = 0 AND '||vOwner||'.pkg_etl_signs.GetConditionResult(p.condition,''INBEGDATE DATE '||vBegDate||'#!#INENDDATE DATE '||vEndDate||''',p.calc_descr) = 1 THEN 0 ELSE 1 END AS skip'||CHR(10)||
    ' FROM tb_calc_pool p'||CHR(10)||
    '       LEFT JOIN tb_calc_2_calc c2c'||CHR(10)||
    '         ON c2c.id = p.id /*AND c2c.parent_id IN (SELECT id FROM tb_calc_pool WHERE archive_flag = 0)*/'||CHR(10)||
    '  /*WHERE p.archive_flag = 0*/'||CHR(10);

    load_new(vBuff,vJobName);
    --dbms_output.put_line(vBuff);
   
    vEndTime := SYSDATE;
    -- Отправка ошибок на почту если они обнаружены в расчете
    IF inSendMessage THEN
      WITH
      dt AS (
        SELECT start_time,last_update AS end_time
          FROM tb_signs_job
          WHERE job_name = vJobName
        ORDER BY start_time DESC  
      )
      SELECT COUNT(1) INTO vErrCou
        FROM tb_signs_log l
             INNER JOIN dt ON l.dat BETWEEN dt.start_time AND dt.end_time
        WHERE l.message LIKE '%ERROR%'
      ORDER BY l.id DESC;
      
      IF vErrCou > 0 THEN 
        vBuff :=
        q'[WITH
          dt AS (
            SELECT start_time,last_update AS end_time
              FROM ]'||vOwner||q'[.tb_signs_job
              WHERE job_name = ']'||vJobName||q'['
            ORDER BY start_time DESC  
          )
        SELECT to_char(l.id) as id,to_char(l.dat,'DD.MM.RRRR HH24:MI:SS') as dat,l.unit,l.message
          FROM ]'||vOwner||q'[.tb_signs_log l
               INNER JOIN dt ON l.dat BETWEEN dt.start_time AND dt.end_time
          WHERE l.message LIKE '%ERROR%' OR LOWER(l.unit) LIKE '%myexecute%' AND l.message LIKE 'ORA-%'
        ORDER BY l.id DESC]';

      send_message_about_project(to_number(GetVarValue('vProjectID')),'DWHLegator (ошибки расчета от '||to_char(vEndTime,'DD.MM.RRRR HH24:MI:SS')||')'
        ,SQLasHTML(vBuff,'ID#!#DAT#!#UNIT#!#MESSAGE','ИД записи:#!#Время:#!#Процедура:#!#Сообщение:'));
      
      END IF;
    END IF;
    -- Окончание отправки ошибок
    
    vMes := 'FINISH :: "'||vBegDate||'" :: Procedure '||UPPER(vOwner)||'.PKG_ETL_SIGNS.CALC finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.calc',vMes);
EXCEPTION WHEN OTHERS THEN
  vEndTime := SYSDATE;
  vMes := 'ERROR :: Procedure '||lower(vOwner)||'.pkg_etl_signs.calc :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.calc',vMes);
  
  vMes := 'FINISH :: Procedure '||UPPER(vOwner)||'.PKG_ETL_SIGNS.CALC finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.calc',vMes);
END calc;

PROCEDURE CalcSignsByGroup(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER,inJobName VARCHAR2)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vBuff VARCHAR2(32700);
    vUnit VARCHAR2(256) := lower(vOwner)||'.pkg_etl_signs.'||CASE WHEN ABS(MONTHS_BETWEEN(inEndDate,inBegDate)) <= 1 THEN 'load_sign' ELSE 'mass_load' END;
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.RRRR');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.RRRR');
    vJobName VARCHAR2(256) := NVL(inJobName,UPPER(vOwner)||'.'||'SIGNSBYGROUPJOB_'||tb_signs_job_id_seq.nextval);
BEGIN
  vBuff :=
  q'[
  SELECT s2g.sign_name AS id
        ,s2s.prev_sign_name AS parent_id
        ,']'||vUnit||q'[' AS unit
        ,']'||vBegDate||'#!#'||vEndDate||q'[#!#'||s2g.sign_name||'#!##!#1' AS params
        ,CASE WHEN p.condition IS NULL OR pkg_etl_signs.GetConditionResult(p.condition,'INBEGDATE DATE ]'||vBegDate||'#!#INENDDATE DATE '||vEndDate||q'[',s2g.sign_name) = 1 THEN 0 ELSE 1 END AS skip
    FROM tb_signs_2_group s2g
         LEFT JOIN tb_signs_pool p ON p.sign_name = s2g.sign_name
         LEFT JOIN tb_sign_2_sign s2s
           ON s2s.sign_name = s2g.sign_name
              AND EXISTS (SELECT NULL FROM tb_signs_pool WHERE sign_name = s2s.prev_sign_name AND archive_flg = 0)
              AND s2s.prev_sign_name IN (SELECT g1.sign_name
                                           FROM tb_signs_2_group g1
                                           WHERE g1.group_id = ]'||inGroupID||q'[)
    WHERE s2g.group_id = ]'||inGroupID||q'[
      AND EXISTS (SELECT NULL FROM tb_signs_pool WHERE sign_name = s2g.sign_name AND archive_flg = 0)
  ORDER BY s2g.sign_name
  ]';

  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
END CalcSignsByGroup;


PROCEDURE CalcSignsByStar(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER,inJobName VARCHAR2)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vBuff VARCHAR2(32700);
    vUnit VARCHAR2(256) := lower(vOwner)||'.pkg_etl_signs.'||CASE WHEN ABS(MONTHS_BETWEEN(inEndDate,inBegDate)) <= 1 THEN 'load_sign' ELSE 'mass_load' END;
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.YYYY');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.YYYY');
    vJobName VARCHAR2(256) := NVL(inJobName,UPPER(vOwner)||'.'||'SIGNSBYSTARJOB_'||tb_signs_job_id_seq.nextval);
BEGIN
  vBuff :=
  q'[
  SELECT s2g.sign_name AS ID
        ,s2s.prev_sign_name AS parent_id
        ,']'||vUnit||q'[' AS unit
        ,']'||vBegDate||'#!#'||vEndDate||q'[#!#'||s2g.sign_name||'#!##!#1' AS params
        ,CASE WHEN p.condition IS NULL OR pkg_etl_signs.GetConditionResult(p.condition,'INBEGDATE DATE ]'||vBegDate||'#!#INENDDATE DATE '||vEndDate||q'[',s2g.sign_name) = 1 THEN 0 ELSE 1 END AS skip
    FROM tb_signs_2_group s2g
         LEFT JOIN tb_signs_pool p ON p.sign_name = s2g.sign_name
         LEFT JOIN tb_sign_2_sign s2s
           ON s2s.sign_name = s2g.sign_name
              AND EXISTS (SELECT NULL FROM tb_signs_pool WHERE sign_name = s2s.prev_sign_name AND archive_flg = 0)
              AND s2s.prev_sign_name IN (SELECT sg.sign_name
                                           FROM tb_signs_group g1
                                                LEFT JOIN tb_signs_2_group sg
                                                  ON sg.group_id = g1.group_id
                                           WHERE LEVEL <= 2
                                           CONNECT BY PRIOR g1.group_id = g1.parent_group_id
                                           START WITH g1.group_id = ]'||inGroupID||q'[)
    WHERE s2g.group_id IN (SELECT group_id FROM tb_signs_group WHERE LEVEL <= 2 CONNECT BY PRIOR group_id = parent_group_id START WITH group_id = ]'||inGroupID||q'[)
      AND EXISTS (SELECT NULL FROM tb_signs_pool WHERE sign_name = s2g.sign_name AND archive_flg = 0)
  ORDER BY s2g.sign_name
  ]';

  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
END CalcSignsByStar;

PROCEDURE CalcAnltByGroup(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER,inJobName VARCHAR2,inParallelJobs NUMBER DEFAULT 30)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vBuff VARCHAR2(32700);
    vUnit VARCHAR2(256) := lower(vOwner)||'.pkg_etl_signs.'||CASE WHEN ABS(MONTHS_BETWEEN(inEndDate,inBegDate)) <= 1 THEN 'load_sign' ELSE 'mass_load' END;
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.YYYY');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.YYYY');
    vJobName VARCHAR2(256) := NVL(inJobName,UPPER(vOwner)||'.'||'ANLTBYGROUPJOB_'||tb_signs_job_id_seq.nextval);
BEGIN
  vBuff :=
  q'[
    SELECT to_char(ROWNUM) AS id
          ,CASE WHEN ROWNUM BETWEEN 1 AND ]'||inParallelJobs||q'[ THEN NULL ELSE ROWNUM - ]'||inParallelJobs||q'[ END AS parent_id
          ,']'||vUnit||q'[' AS unit
          ,']'||vBegDate||'#!#'||vEndDate||q'[#!#'||s2g.sign_name||'#!#'||s2a.anlt_code||'#!#1' AS params
          ,0 AS skip
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_pool p
             ON p.sign_name = s2g.sign_name
                AND p.archive_flg = 0
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2g.sign_name = s2a.sign_name
                AND EXISTS (SELECT NULL FROM tb_anlt_2_group a2g WHERE a2g.anlt_code = s2a.anlt_code AND a2g.group_id = ]'||inGroupID||')
      WHERE s2g.group_id = '||inGroupID||'
        AND s2a.anlt_code IS NOT NULL';
  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
END CalcAnltByGroup;

PROCEDURE CalcAnltByStar(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER,inJobName VARCHAR2,inParallelJobs NUMBER DEFAULT 30)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vBuff VARCHAR2(32700);
    vUnit VARCHAR2(256) := lower(vOwner)||'.pkg_etl_signs.'||CASE WHEN ABS(MONTHS_BETWEEN(inEndDate,inBegDate)) <= 1 THEN 'load_sign' ELSE 'mass_load' END;
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.YYYY');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.YYYY');
    vJobName VARCHAR2(256) := NVL(inJobName,UPPER(vOwner)||'.'||'ANLTBYSTARJOB_'||tb_signs_job_id_seq.nextval);
BEGIN
  vBuff :=
  q'[
  SELECT to_char(ROWNUM) AS id
        ,CASE WHEN ROWNUM BETWEEN 1 AND ]'||inParallelJobs||q'[ THEN NULL ELSE ROWNUM - ]'||inParallelJobs||q'[ END AS parent_id
        ,']'||vUnit||q'[' AS unit
        ,']'||vBegDate||'#!#'||vEndDate||q'[#!#'||s2g.sign_name||'#!#'||s2a.anlt_code||'#!#1' AS params
        ,0 AS skip
    FROM tb_signs_2_group s2g
         LEFT JOIN tb_sign_2_anlt s2a
           ON s2g.sign_name = s2a.sign_name
              AND EXISTS (SELECT NULL FROM tb_anlt_2_group a2g
                            WHERE a2g.anlt_code = s2a.anlt_code
                              AND a2g.group_id = (SELECT group_id FROM tb_signs_group WHERE LEVEL = 3 CONNECT BY PRIOR group_id = parent_group_id START WITH group_id = ]'||inGroupID||q'[))
    WHERE s2g.group_id = (SELECT group_id FROM tb_signs_group WHERE LEVEL = 3 CONNECT BY PRIOR group_id = parent_group_id START WITH group_id = ]'||inGroupID||q'[)
      AND EXISTS (SELECT NULL FROM tb_signs_pool WHERE sign_name = s2g.sign_name AND archive_flg = 0)
  ORDER BY s2g.sign_name
  ]';
  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
END CalcAnltByStar;

/******************************** ИМПОРТ - ЭКСПОРТ **************************************/
/*FUNCTION AnltSpecImpGetCondition(inSignName VARCHAR2,inIds VARCHAR2 DEFAULT NULL,inProduct IN NUMBER DEFAULT 0) RETURN CLOB
  IS
    vCond CLOB;
    vBuff VARCHAR2(32700);
    vCou INTEGER := 0;
BEGIN
  dbms_lob.createtemporary(vCond,TRUE);
  FOR idx IN (
    SELECT rul.rule_id
      FROM skb_ecc_new.ecc_rule rul
      WHERE skb_ecc_new.getdim(rul.dim_key,CASE WHEN inProduct = 0 THEN 'D38328296CBF147E5A0794D9AF4FB1F59DFFACBD' ELSE '597074F6BDD5CDBFCBBA37523F1D0C4D72BB0B23' END) = inSignName
        AND (inIds IS NULL OR inIds IS NOT NULL AND rul.rule_id IN (SELECT str FROM TABLE(parse_str(inIds,','))))
  ) LOOP
    IF vCou = 0 THEN
      vBuff := REPLACE(REPLACE(dbms_lob.substr(skb_ecc_new.rule_pkg.getSqlCondition(idx.rule_id,'N')),'t.','anlt.'),'T.','anlt.');
      ELSE vBuff := CHR(10)||' OR '||CHR(10)||REPLACE(REPLACE(dbms_lob.substr(skb_ecc_new.rule_pkg.getSqlCondition(idx.rule_id,'N')),'t.','anlt.'),'T.','anlt.');
    END IF;
    dbms_lob.writeappend(vCond,LENGTH(vBuff),vBuff);
    vCou := vCou + 1;
  END LOOP;
  RETURN vCond;
EXCEPTION WHEN OTHERS THEN
  RETURN 'ERROR :: '||inSignName||' :: '||SQLERRM;
END AnltSpecImpGetCondition;*/
PROCEDURE ImportAnltSpecs(inDate IN DATE,inAnltCodes IN VARCHAR2)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vBuff VARCHAR2(4000) :=
'SELECT str AS id
       ,NULL AS parent_id
       ,'''||lower(vOwner)||'.pkg_etl_signs.AnltSpecImport'' AS unit
       ,'''||to_char(inDate,'DD.MM.RRRR')||'#!#''||str AS params
       ,0 AS skip
   FROM TABLE('||lower(vOwner)||'.pkg_etl_signs.parse_str('''||inAnltCodes||''','',''))';
       
    vMes VARCHAR2(4000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'IMPORT_ANALYTICS_'||tb_signs_job_id_seq.nextval;
BEGIN
  vMes := 'START :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ImportAnltSpecs" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ImportAnltSpecs',vMes);

  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
  
  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ImportAnltSpecs" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ImportAnltSpecs',vMes);
EXCEPTION WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: "'||UPPER(inAnltCodes)||'"  - '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ImportAnltSpecs',vMes);
    vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ImportAnltSpecs" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ImportAnltSpecs',vMes);
END ImportAnltSpecs;

PROCEDURE AnltSpecImport(inDate IN DATE,inAnltCode IN VARCHAR2)
  IS
    vAnltID NUMBER;
    vMes VARCHAR2(4000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vMes := 'START :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.AnltSpecImport" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.AnltSpecImport',vMes);

  -- ИД аналитики
  SELECT id
    INTO vAnltID
    FROM tb_signs_anlt
    WHERE anlt_code = UPPER(inAnltCode) AND inDate BETWEEN effective_start AND effective_end;

  MERGE INTO tb_signs_anlt_spec dest
    USING (SELECT vAnltID AS anlt_id,val,parent_val,name,condition
             FROM TABLE(get_anlt_spec_imp(inDate,UPPER(inAnltCode)))
          ) src ON (dest.anlt_id = src.anlt_id AND dest.anlt_spec_val = src.val)
    WHEN NOT MATCHED THEN
      INSERT (dest.id,dest.anlt_id,dest.anlt_spec_val,dest.parent_val,dest.anlt_spec_name,dest.condition)
        VALUES (tb_signs_anlt_spec_id_seq.nextval,src.anlt_id,src.val,src.parent_val,src.name,src.condition)
    WHEN MATCHED THEN
      UPDATE SET dest.parent_val = src.parent_val
                ,dest.anlt_spec_name = src.name
                ,dest.condition = src.condition
        WHERE (isEqual(dest.parent_val,src.parent_val) = 0 OR
               isEqual(dest.anlt_spec_name,src.name) = 0 OR
               isEqual(dest.condition,src.condition) = 0)
               AND dbms_lob.substr(src.condition,1,30) != '1 = 0'
               AND dest.block_import = 0;

  vMes := 'SUCCESSFULLY :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Аналитика "'||UPPER(inAnltCode)||'"  - '||SQL%ROWCOUNT||' rows merged into table "'||vOwner||'.tb_signs_anlt_spec"';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.AnltSpecImport',vMes);
  COMMIT;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.AnltSpecImport" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.AnltSpecImport',vMes);
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    vMes := 'ERROR :: "'||UPPER(inAnltCode)||'"  - Аналитика не найдена в таблице "'||lower(vOwner)||'.tb_signs_anlt"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.AnltSpecImport',vMes);
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: "'||UPPER(inAnltCode)||'"  - '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.AnltSpecImport',vMes);
    vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.AnltSpecImport" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.AnltSpecImport',vMes);
END AnltSpecImport;

/********************* ЗВЕЗДЫ И ВСЁ ЧТО С НИМИ СВЯЗАНО **********************************/
FUNCTION  GetAnltLineSQL(inSQL IN CLOB,inIDName IN VARCHAR2
  ,inPIDName IN VARCHAR2,inName IN VARCHAR2,inValue IN VARCHAR2) RETURN CLOB
  IS
    vMaxLev INTEGER;
    vBuff VARCHAR2(32700);
    vWith CLOB;
    vSQL CLOB;
    vSel VARCHAR2(32700);
    vSelNames VARCHAR2(32700);
    vSelIDs VARCHAR2(32700);
    vSelValues VARCHAR2(32700);
BEGIN
  vBuff :=
  'DECLARE'||CHR(10)||
  '  vMaxLev INTEGER;'||CHR(10)||
  'BEGIN'||CHR(10)||
  'SELECT MAX(LEVEL) INTO vMaxLev FROM ('||inSQL||') CONNECT BY PRIOR '||inIDName||' = '||inPIDName||CHR(10)||
  'START WITH '||inPIDName||' IS NULL;'||CHR(10)||
  ':1 := vMaxLev;'||CHR(10)||
  'END;';

  EXECUTE IMMEDIATE vBuff USING OUT vMaxLev;
  --dbms_output.put_line(vBuff);

  FOR idx IN 1..vMaxLev LOOP
    vBuff :=
    'DECLARE'||CHR(10)||
    '  vId VARCHAR2(32700);'||CHR(10)||
    '  vName VARCHAR2(32700);'||CHR(10)||
    '  vValue VARCHAR2(32700);'||CHR(10)||
    'BEGIN'||CHR(10)||
    '  SELECT id'||idx||',lev_name'||idx||',lev_value'||idx||CHR(10)||
    '    INTO vId,vName,vValue'||CHR(10)||
    '    FROM ('||CHR(10)||
    '      SELECT '||inIDName||' AS id,'||inPIDName||' AS parent_id,'||inName||' AS lev_name,'||inValue||' AS lev_value,LEVEL AS lev'||CHR(10)||
    '        ,SUBSTR(sys_connect_by_path(''lev''||to_char(abs(LEVEL - '||idx||') + 1)||''.id'','',''),2,LENGTH(sys_connect_by_path(''lev''||to_char(abs(LEVEL - '||idx||') + 1)||''.id'','','')) - 1) AS id'||idx||CHR(10)||
    '        ,SUBSTR(sys_connect_by_path(''lev''||to_char(abs(LEVEL - '||idx||') + 1)||''.lev_name'','',''),2,LENGTH(sys_connect_by_path(''lev''||to_char(abs(LEVEL - '||idx||') + 1)||''.lev_name'','','')) - 1) AS lev_name'||idx||CHR(10)||
    '        ,SUBSTR(sys_connect_by_path(''lev''||to_char(abs(LEVEL - '||idx||') + 1)||''.lev_value'','',''),2,LENGTH(sys_connect_by_path(''lev''||to_char(abs(LEVEL - '||idx||') + 1)||''.lev_value'','','')) - 1) AS lev_value'||idx||CHR(10)||
    '        FROM ('||inSQL||')'||CHR(10)||
    '      CONNECT BY PRIOR '||inIDName||' = '||inPIDName||CHR(10)||
    '      START WITH '||inPIDName||' IS NULL'||CHR(10)||
    '  ) WHERE lev = '||idx||'  GROUP BY id'||idx||',lev_name'||idx||',lev_value'||idx||';'||CHR(10)||
    '  :1 := vId;'||CHR(10)||
    '  :2 := vName;'||CHR(10)||
    '  :3 := vValue;'||CHR(10)||
    'END;'||CHR(10);

    EXECUTE IMMEDIATE vBuff USING OUT vSelIDs,OUT vSelNames,OUT vSelValues;
    --dbms_output.put_line(vBuff);

    IF idx = 1 THEN
      vSel := 'lev'||idx||'.id AS id'||idx||',lev'||idx||'.lev_name AS name'||idx||',lev'||idx||'.lev_value AS value'||idx||CHR(10);
    ELSE
      vSel := vSel||',COALESCE('||vSelIDs||') AS id'||idx||',COALESCE('||vSelNames||') AS name'||idx||',COALESCE('||vSelValues||') AS value'||idx||CHR(10);
    END IF;
  END LOOP;

  dbms_lob.createtemporary(vWith,FALSE);
  dbms_lob.createtemporary(vSQL,FALSE);

  vBuff := 'WITH'||CHR(10)||
  '  src AS ('||CHR(10)||'    '||inSQL||CHR(10)||'  )'||CHR(10);
  dbms_lob.writeappend(vWith,LENGTH(vBuff),vBuff);

  FOR idx IN 1..vMaxLev LOOP
    vBuff :=
    ' ,lev'||idx||' AS ('||CHR(10)||
    '  SELECT id,parent_id,lev_name,lev_value,lev'||CHR(10)||
    '    FROM ('||CHR(10)||
    '      SELECT '||inIDName||' AS id,'||inPIDName||' AS parent_id,'||inName||' AS lev_name,'||inValue||' AS lev_value, LEVEL AS lev'||CHR(10)||
    --'        FROM ('||inSQL||')'||CHR(10)||
    '        FROM src'||CHR(10)||
    '      CONNECT BY PRIOR '||inIDName||' = '||inPIDName||CHR(10)||
    '      START WITH '||inPIDName||' IS NULL'||CHR(10)||
    '  ) WHERE lev BETWEEN '||idx||' - 1 AND '||idx||CHR(10)||
    ')'||CHR(10);
    dbms_lob.writeappend(vWith,LENGTH(vBuff),vBuff);

    IF idx > 1 THEN
      vBuff := '       LEFT JOIN lev'||idx||' ON lev'||idx||'.lev = '||idx||' AND lev'||idx||'.parent_id = lev'||to_char(idx - 1)||'.id OR'||CHR(10)||
               '                                 lev'||idx||'.lev = '||idx||' - 1 AND lev'||idx||'.id = lev'||to_char(idx - 1)||'.id'||CHR(10);
    ELSE
      vBuff := CHR(10)||'  FROM lev1'||CHR(10);
    END IF;
    dbms_lob.writeappend(vSQL,LENGTH(vBuff),vBuff);
  END LOOP;

  RETURN vWith||'SELECT '||vSel||vSQL;
END GetAnltLineSQL;

FUNCTION StarGetFldList(inDate DATE,inGroupID NUMBER) RETURN TTabStarFldList PIPELINED
  IS
    rec TRecStarFldList;
BEGIN
  FOR idx IN (
    WITH
      grp AS (
        SELECT /*+ materialize */
               group_id
              ,parent_group_id
              ,LEVEL AS lev
              ,group_name
              ,CONNECT_BY_ROOT(group_id) AS head_group_id
          FROM tb_signs_group g
        CONNECT BY PRIOR group_id = parent_group_id
        START WITH group_id = inGroupID
      )
     ,aa AS (
        SELECT /*+ materialize */
               CASE WHEN LEVEL = 2 THEN 'FCT_'||group_id ELSE 'ANLTLINE_'||(SELECT group_id FROM tb_signs_group WHERE parent_group_id IS NULL CONNECT BY group_id = PRIOR parent_group_id START WITH group_id = g.parent_group_id) END AS tbl_prefix
          FROM tb_signs_group g
               WHERE LEVEL >= 2
        CONNECT BY PRIOR group_id = parent_group_id
        START WITH group_id = inGroupID
      )
     ,a AS (
       SELECT /*+ materialize */
              a.id AS anlt_id
             ,a.anlt_code
             ,a.anlt_alias
             ,CASE WHEN EXISTS(SELECT NULL FROM tb_signs_anlt_spec WHERE anlt_id = a.id) THEN
                CASE WHEN a.anlt_sql IS NULL THEN -1 ELSE 1 END
                ELSE NVL2(a.anlt_code,0,NULL)
              END AS anlt_flg
             ,(SELECT ID FROM tb_entity WHERE parent_id IS NULL CONNECT BY ID = PRIOR parent_id START WITH ID = a.entity_id) AS a_e_id
             ,(SELECT group_id FROM tb_signs_group WHERE parent_group_id IS NULL CONNECT BY group_id = PRIOR parent_group_id START WITH group_id = grp.parent_group_id) AS a_h_id
             ,a.anlt_alias_descr
             ,a.anlt_name
         FROM grp
              LEFT JOIN tb_anlt_2_group a2g
                ON a2g.group_id = grp.group_id
              LEFT JOIN tb_signs_anlt a
                ON a.anlt_code = a2g.anlt_code
                   AND inDate BETWEEN a.effective_start AND a.effective_end
         WHERE grp.lev = 3
      )
     ,a1 AS (
       SELECT /*+ materialize */
              a.*
         FROM a
         WHERE anlt_flg = 0
     )
   ,a2 AS (
       SELECT /*+ materialize */
              a.*
             ,fld.col_name
         FROM a
              CROSS JOIN TABLE(pkg_etl_signs.DescribeColumns(pkg_etl_signs.GetAnltLineSQL('SELECT anlt_spec_val AS id,parent_val AS parent_id,anlt_spec_name AS name, anlt_spec_val AS value FROM '||LOWER(pkg_etl_signs.GetVarValue('vOwner'))||'.tb_signs_anlt_spec WHERE anlt_id = '||a.anlt_id,'id','parent_id','name','value'))) fld
         WHERE anlt_flg != 0
     )
    ,anlt AS (
       SELECT /*+ materialize */
              b.*,aa.tbl_prefix FROM (
           SELECT a2.*
             FROM a2
           UNION ALL
           SELECT a1.*,NULL
             FROM a1
         ) b CROSS JOIN aa
     )
    ,sgn AS (
       SELECT /*+ materialize */
              grp.group_id
             ,parent_group_id
             ,grp.lev
             ,s2g.sign_name
             ,NVL(s2g.sgn_alias,s2g.sign_name) AS sgn_alias
             ,s2a.anlt_code
             ,s2g.preaggr_flg
             ,(SELECT ID FROM tb_entity WHERE parent_id IS NULL CONNECT BY ID = PRIOR parent_id START WITH ID = p.entity_id) AS p_e_id
             ,(SELECT entity_name FROM tb_entity WHERE parent_id IS NULL CONNECT BY ID = PRIOR parent_id START WITH ID = p.entity_id) AS p_e_name
             ,p.sign_descr
             ,grp.group_name
             ,CASE WHEN p.data_type = 'Строка' THEN 'VARCHAR2(4000)'
                   WHEN p.data_type = 'Число' THEN 'NUMBER'
                   WHEN p.data_type = 'Дата'  THEN 'DATE'
              END AS sgn_data_type
         FROM grp
              INNER JOIN tb_signs_2_group s2g
                ON s2g.group_id = grp.group_id AND grp.lev <= 2
              INNER JOIN tb_signs_pool p
                ON p.sign_name = s2g.sign_name
              LEFT JOIN tb_sign_2_anlt s2a
                ON s2a.sign_name = s2g.sign_name
      )
     ,prev AS (SELECT * FROM sgn FULL JOIN anlt ON sgn.lev > 1 AND anlt.anlt_code = sgn.anlt_code OR sgn.lev = 1 AND anlt.a_e_id = sgn.p_e_id)
      SELECT DISTINCT
             CASE WHEN tbl LIKE 'FCT%' THEN 'F' ELSE anlt_alias END AS table_alias
            ,tbl AS table_name
            ,table_comment
            ,col_name
            ,col_type
            ,preaggr_flg
            ,CASE WHEN comm IS NULL THEN
               CASE WHEN REGEXP_LIKE(col_name,'ID[[:digit:]]{1,}') THEN 'ИД '
                    WHEN REGEXP_LIKE(col_name,'NAME[[:digit:]]{1,}') THEN 'Наименование '
                 ELSE 'Значение '
               END||CASE WHEN SUBSTR(col_name,INSTR(col_name,REGEXP_SUBSTR(col_name,'[[:digit:]]'))) = 1 THEN 'верхнего'
                           ELSE to_char(to_number(SUBSTR(col_name,INSTR(col_name,REGEXP_SUBSTR(col_name,'[[:digit:]]')))) - 1)
                            END||' уровня'
               ELSE comm
             END AS col_comment
            ,to_number(CASE WHEN ff.tbl LIKE 'DIM\_%' ESCAPE '\' THEN SUBSTR(ff.tbl,INSTR(ff.tbl,'#') + 1) END) AS entity_id
            ,e.entity_name
        FROM (
          SELECT DISTINCT 
                 CASE WHEN lev = 2 OR anlt_flg = -1 THEN tbl_prefix ELSE 'DIM_'||group_id END||
                   CASE WHEN tbl_prefix LIKE 'FCT%' AND (lev > 1 OR lev IS NULL) THEN NULL ELSE CASE WHEN lev = 1 THEN '#'||to_char(p_e_id) ELSE '#'||anlt_alias END
                 END AS tbl
                ,CASE WHEN NOT(lev = 2 OR anlt_flg = -1) THEN 'Измерение: Группа - "'||group_name||'"; Сущность - "'||p_e_name||'"'
                      WHEN tbl_prefix LIKE 'FCT%' AND (lev > 1 OR lev IS NULL) THEN group_name
                   ELSE REPLACE(anlt_name,'СУКА ::','Иерархическое измерение:')
                 END AS table_comment
                ,sgn_alias
                ,preaggr_flg
                ,anlt_alias
                ,anlt_flg
                ,CASE WHEN lev = 2 AND tbl_prefix LIKE 'FCT%' THEN anlt_alias ELSE NVL(col_name,sgn_alias) END AS col_name
                ,CASE WHEN lev = 1 THEN sign_descr WHEN lev = 2 AND tbl_prefix LIKE 'FCT%' THEN anlt_alias_descr ELSE NULL END AS comm
                ,group_name
                ,CASE WHEN lev = 1 THEN sgn_data_type ELSE 'VARCHAR2(4000)' END AS col_type
            FROM prev
        ) ff LEFT JOIN tb_entity e
               ON to_char(e.id) = CASE WHEN ff.tbl LIKE 'DIM\_%' ESCAPE '\' THEN SUBSTR(ff.tbl,INSTR(ff.tbl,'#') + 1) END
           WHERE (NOT(ff.tbl LIKE 'ANLTLINE%') OR ff.anlt_flg != 0) AND ff.anlt_flg >= 0
      UNION ALL
      SELECT 'AGGR_'||a.group_id||'#'||a.id AS table_alias
             ,'AGGR_'||a.group_id||'#'||a.id AS table_name
             ,a.aggr_name AS table_comment
             ,fld.col_name
             ,fld.col_type
             ,1 AS preaggr_flg
             ,dbms_lob.substr(ap.p_value,dbms_lob.getlength(ap.p_value),INSTR(ap.p_value,'#!#') + 3) AS col_comment
             ,a.id
             ,a.aggr_name
        FROM tb_signs_aggrs a
             CROSS JOIN TABLE(pkg_etl_signs.DescribeColumns(a.aggr_sql)) fld
             LEFT JOIN tb_signs_aggrs_p ap
               ON ap.aggr_id = a.id
                  AND ap.p_name LIKE 'COLUMN%'
                  AND dbms_lob.substr(ap.p_value,INSTR(ap.p_value,'#!#') - 1,1) = fld.col_name
        WHERE a.group_id = inGroupID
        UNION ALL
        SELECT DISTINCT
               anlt.anlt_alias AS table_alias
              ,'ANLTLINE_'||grp.head_group_id||'#'||anlt.anlt_alias AS table_name
              ,REPLACE(anlt.anlt_name,'СУКА ::','Иерархическое измерение:') AS table_comment
              ,anlt.col_name
              ,'VARCHAR2(4000)' AS col_type
              ,0 AS preaggr_flg
              ,CASE WHEN REGEXP_LIKE(col_name,'ID[[:digit:]]{1,}') THEN 'ИД '
                    WHEN REGEXP_LIKE(col_name,'NAME[[:digit:]]{1,}') THEN 'Наименование '
                 ELSE 'Значение '
               END||CASE WHEN SUBSTR(col_name,INSTR(col_name,REGEXP_SUBSTR(col_name,'[[:digit:]]'))) = 1 THEN 'верхнего'
                           ELSE to_char(to_number(SUBSTR(col_name,INSTR(col_name,REGEXP_SUBSTR(col_name,'[[:digit:]]')))) - 1)
                    END||' уровня' AS col_comment
              ,NULL
              ,NULL
          FROM grp
               INNER JOIN tb_anlt_2_group a2g
                 ON a2g.group_id = grp.group_id
                    AND grp.lev = 3
               INNER JOIN anlt
                 ON anlt.anlt_code = a2g.anlt_code
                    AND anlt.a_e_id IS NULL
      ORDER BY 1,4
  ) LOOP
    rec.table_alias := idx.table_alias;
    rec.table_name := idx.table_name;
    rec.table_comment := idx.table_comment;
    rec.col_name := idx.col_name;
    rec.col_type := idx.col_type;
    rec.preaggr_flg := idx.preaggr_flg;
    rec.col_comment := idx.col_comment;
    rec.entity_id := idx.entity_id;
    rec.entity_name := idx.entity_name;
    PIPE ROW(rec);
  END LOOP;
END StarGetFldList;

PROCEDURE StarPrepareAggrTable(inDate IN DATE,inAggrID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vGroupID NUMBER;
    vAggrSQL CLOB;
    vAggrNAme VARCHAR2(4000);
    -- список наименований полей через запятую (для использования при построении динамического SQL)
    vMes VARCHAR2(4000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrTable" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrTable',vMes);
  
  SELECT group_id,aggr_sql,aggr_name INTO vGroupID,vAggrSQL,vAggrName FROM tb_signs_aggrs WHERE id = inAggrID;
  
  PrepareTableBySQL(inDate,'AGGR_'||vGroupID||'#'||inAggrID,vAggrSQL,vAggrName);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrTable" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrTable',vMes);
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'"  - Описание агрегата "ID = '||inAggrID||'" не найдено в таблице "'||lower(vOwner)||'.tb_signs_aggrs"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrTable',vMes);
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: Агрегат "ID = '||inAggrID||'" :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrTable',vMes);
    
    vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrTable" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrTable',vMes);
END StarPrepareAggrTable;

PROCEDURE StarPrepareAggrs(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'PREPAREAGGRSJOB_'||tb_signs_job_id_seq.nextval;
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.RRRR');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.RRRR');
    vCou INTEGER := 0;
    vGroupName VARCHAR2(4000);
    vBuff VARCHAR2(32700) :=
q'{WITH
  dt AS (
    SELECT to_date('}'||vBegDate||q'{','DD.MM.RRRR') + LEVEL - 1 AS dt FROM dual CONNECT BY LEVEL <= to_date('}'||vEndDate||q'{','DD.MM.RRRR') - to_date('}'||vBegDate||q'{','DD.MM.RRRR') + 1
  )
SELECT to_char(dt.dt,'DD.MM.RRRR')||'|'||a.id AS id
      ,NVL2(LAG(dt.dt) OVER (PARTITION BY a.id ORDER BY dt.dt),to_char(LAG(dt.dt) OVER (PARTITION BY a.id ORDER BY dt.dt),'DD.MM.RRRR')||'|'||a.id,NULL) AS parent_id
      ,'}'||LOWER(vOwner)||q'{.pkg_etl_signs.StarPrepareAggrTable' AS unit
      ,to_char(dt.dt,'DD.MM.RRRR')||'#!#'||a.id AS params
      ,0 AS SKIP
  FROM }'||LOWER(vOwner)||q'{.tb_signs_aggrs a
       CROSS JOIN dt
  WHERE group_id = }'||inGroupID;
    ---
    vMes VARCHAR2(4000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: "'||vBegDate||'" :: Procedure '||LOWER(vOwner)||'.pkg_etl_signs.StarPrepareAggrs started';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrs',vMes);
  
  SELECT COUNT(1) INTO vCou FROM tb_signs_aggrs WHERE group_id = inGroupID;
  SELECT group_name INTO vGroupName FROM tb_signs_group WHERE group_id = inGroupID;

  IF vCou > 0 THEN
    load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
  ELSE
    vMes := 'INFORMATION :: У группы "'||vGroupName||'" отсутствуют агрегаты. Подготовка не требуется';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrs',vMes);
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrs" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrs',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrs" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrs',vMes);
  vEndTime := SYSDATE;

  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrs" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAggrs',vMes);
END StarPrepareAggrs;

PROCEDURE StarPrepareDim(inDate IN DATE,inGroupID IN NUMBER,inEntityID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    -- список наименований полей через запятую (для использования при построении динамического SQL)
    vFieldsForCreate VARCHAR2(32700);
    --
    vDDL CLOB;

    vBuff VARCHAR2(32700);
    vStarDimTable VARCHAR2(256) := vOwner||'.dim_'||inGroupID||'#'||inEntityID; -- наименование таблицы фактов в звезде
    vGroupName VARCHAR2(4000);   -- наименование группы показателей измерений
    vEntityName VARCHAR2(4000);  -- нименование сущности
    vTabCou INTEGER;

    vMes VARCHAR2(4000);
    vTIBegin DATE;
    vENdTime DATE;
BEGIN
  -- Получение наименования группы
  SELECT group_name INTO vGroupName FROM tb_signs_group WHERE group_id = inGroupID;
  -- Получение наименования сущности
  SELECT entity_name INTO vEntityName FROM tb_entity WHERE id = inEntityID;

  /*******************************************************************/
  vTIBegin := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Сущность: "'||vEntityName||'" - начало подготовки таблицы -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareDim',vMes);

  /*******************************************************************/

  -- Формирование строковых переменных со списком полей через запятую
  vFieldsForCreate := NULL;
  FOR idx IN (
    SELECT DISTINCT
           NVL(s2g.sgn_alias,p.sign_name) AS sign_name
          ,p.data_type
          ,LISTAGG(p.sign_descr,'; ') WITHIN GROUP (ORDER BY p.id) OVER (PARTITION BY NVL(s2g.sgn_alias,p.sign_name)) AS sign_descr
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_pool p
             ON p.sign_name = s2g.sign_name
                AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                    START WITH id = inEntityID)
      WHERE s2g.group_id = inGroupID
  ) LOOP
        vFieldsForCreate := vFieldsForCreate||','||idx.sign_name||' '||
        CASE WHEN idx.data_type = 'Число' THEN 'NUMBER'
             WHEN idx.data_type = 'Дата' THEN 'DATE'
          ELSE 'VARCHAR2(4000)'
        END;
  END LOOP;

  dbms_lob.createtemporary(vDDL,FALSE);

  vBuff :=
  'DECLARE'||CHR(10)||
  '  vBuff VARCHAR2(32700);'||CHR(10)||
  'BEGIN'||CHR(10);
  dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

  -- Проверка на существование таблиц измерений для звезды
  SELECT COUNT(1) INTO vTabCou FROM dba_all_tables
    WHERE owner = UPPER(vOwner) AND table_name = 'DIM_'||inGroupID||'#'||inEntityID;
    
  -- ЕСЛИ ТАБЛИЦА ОТСУТСТВУЕТ, ТО СОЗДАЕМ
  IF vTabCou = 0 THEN
    vBuff :=
    '  EXECUTE IMMEDIATE ''CREATE TABLE '||vStarDimTable||CHR(10)||
    '   (as_of_date DATE,obj_sid VARCHAR2(300)'||vFieldsForCreate||')'||CHR(10)||
    '   PARTITION BY LIST (as_of_date) '||CHR(10)||
    '   (PARTITION P'||to_char(inDate,'RRRRMMDD')||' VALUES(to_date('''''||to_char(inDate,'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')) STORAGE (INITIAL 64K NEXT 4M)) NOLOGGING'';'||CHR(10)||CHR(10);
    dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

    -- Комментарии для колонок таблицы
    vBuff := '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||vStarDimTable||'.as_of_date IS ''''Отчетная дата'''' '';'||CHR(10);
    dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
    vBuff := '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||vStarDimTable||'.obj_sid IS ''''Ид объекта (уникально в переделах одной даты). Используется для связки с фактами (по ключевым полям фактов).'''' '';'||CHR(10);
    dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
    FOR idx IN (
      SELECT DISTINCT
             NVL(s2g.sgn_alias,p.sign_name) AS sign_name
            ,p.data_type
            ,LISTAGG(p.sign_descr,'; ') WITHIN GROUP (ORDER BY p.id) OVER (PARTITION BY NVL(s2g.sgn_alias,p.sign_name)) AS sign_descr
        FROM tb_signs_2_group s2g
             INNER JOIN tb_signs_pool p
               ON p.sign_name = s2g.sign_name
                  AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                      START WITH id = inEntityID)
        WHERE s2g.group_id = inGroupID
    ) LOOP
      vBuff := '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||vStarDimTable||'.'||LOWER(idx.sign_name)||' IS '''''||REPLACE(idx.sign_descr,'''','''''')||''''' '';'||CHR(10);
      dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
    END LOOP;
    -- Вешаем комментарий на таблицу
    vBuff :=
    '  EXECUTE IMMEDIATE ''COMMENT ON TABLE '||vStarDimTable||' IS ''''Измерение: Группа - "'||vGroupName||'"; Сущность - "'||vEntityName||'"'''' '';'||CHR(10)||
    '  vBuff := ''SUCCESSFULLY :: Table "'||vStarDimTable||'" created''||CHR(10);'||CHR(10);
    dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

 -- ЕСЛИ ТАБЛИЦА УЖЕ СУЩЕСТВУЕТ
  ELSE
    -- Если партиция отсутствует - добавляем
    vBuff :=
    '  BEGIN'||CHR(10)||
    '    EXECUTE IMMEDIATE ''ALTER TABLE '||vStarDimTable||' ADD PARTITION P'||to_char(inDate,'RRRRMMDD')||' VALUES (to_date('''''||to_char(inDate,'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')) STORAGE (INITIAL 64K NEXT 4M) NOLOGGING'';'||CHR(10)||
    '    vBuff := ''SUCCESSFULLY :: Table "'||vStarDimTable||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' added''||CHR(10);'||CHR(10)||
    '  EXCEPTION WHEN OTHERS THEN'||CHR(10)||
    '    NULL;'||CHR(10)||
    '  END;'||CHR(10);
    dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

    -- Т.к., к моменту текущего разворачивания, ключевые колонки (как количество так и наименование), могут измениться
    -- то необходимо добавить недостающие (если таковые найдутся)
    -- !!!Пока что предполагается, что количество может только увеличиться!!!
    FOR idx IN (
      SELECT DISTINCT
             NVL(s2g.sgn_alias,p.sign_name) AS sign_name
            ,CASE WHEN p.data_type = 'Число' THEN 'NUMBER'
                  WHEN p.data_type = 'Дата' THEN 'DATE'
               ELSE 'VARCHAR2(4000)'
             END AS data_type
            ,LISTAGG(p.sign_descr,'; ') WITHIN GROUP (ORDER BY p.id) OVER (PARTITION BY NVL(s2g.sgn_alias,p.sign_name)) AS sign_descr
        FROM tb_signs_2_group s2g
             INNER JOIN tb_signs_pool p
               ON p.sign_name = s2g.sign_name
                  AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                      START WITH id = inEntityID)
        WHERE s2g.group_id = inGroupID
      MINUS
      SELECT c.column_name
            ,c.data_type
            ,cmnt.comments
        FROM all_tab_columns c
             INNER JOIN all_col_comments cmnt
               ON cmnt.owner = c.owner
                  AND cmnt.table_name = c.table_name
                  AND cmnt.column_name = c.column_name
        WHERE c.owner = UPPER(vOwner)
          AND c.table_name = UPPER(SUBSTR(vStarDimTable,INSTR(vStarDimTable,'.',1) + 1,LENGTH(vStarDimTable) - INSTR(vStarDimTable,'.',1)))
          AND NOT(c.column_name IN ('AS_OF_DATE'))
    ) LOOP
      vBuff :=
      '  BEGIN'||CHR(10)||
      -- Добавление колонки
      '    EXECUTE IMMEDIATE ''ALTER TABLE '||vStarDimTable||' ADD '||idx.sign_name||' '||idx.data_type||' '';'||CHR(10)||
      -- Добавление комментария
      '    EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||vStarDimTable||'.'||LOWER(idx.sign_name)||' IS '''''||REPLACE(idx.sign_descr,'''','''''')||''''' '';'||CHR(10)||
      '    vBuff := vBuff||''SUCCESSFULLY :: Column "'||vStarDimTable||'.'||lower(idx.sign_name)||'" added''||CHR(10);'||CHR(10)||
      '  EXCEPTION WHEN OTHERS THEN'||CHR(10)||
      '    NULL;'||CHR(10)||
      '  END;'||CHR(10);
      dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
    END LOOP;
  END IF;

  -- Финальный END
  vBuff :=
  '  :1 := vBuff;'||CHR(10)||
  'END;'||CHR(10);
  dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

  EXECUTE IMMEDIATE vDDL USING OUT vMes;
  --dbms_output.put_line(vDDL);
  IF vMes IS NULL THEN
    vMes := 'SUCCESSFULLY :: Table "'||vStarDimTable||'" - подготовка не требуется';
  END IF;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareDim',vMes);

  vEndTime := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Сущность: "'||vEntityName||'" - окончание подготовки таблицы. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareDim',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepareDim" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareDim',vMes);
END StarPrepareDim;

PROCEDURE PrepareTableBySQL(inDate IN DATE,inTableName IN VARCHAR2,inSQL IN CLOB,inComment IN VARCHAR2 DEFAULT NULL)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');

    vBuff VARCHAR2(32700);
    vDDL CLOB;
    vTableName VARCHAR2(256);
    vTabCou INTEGER;
    --
    vMes VARCHAR2(4000);
    vTIBegin DATE;
    vEndTime DATE;
BEGIN
  vTableName := LOWER(vOwner)||'.'||inTableName;
  
  vTIBegin := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Таблица: "'||vTableName||'" - начало подготовки -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.PrepareTableBySQL',vMes);

  dbms_lob.createtemporary(vDDL,FALSE);

  vBuff :=
  'DECLARE'||CHR(10)||
  '  vBuff VARCHAR2(32700);'||CHR(10)||
  'BEGIN'||CHR(10);
  dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

  -- Проверка на существование таблиц измерений для звезды
  SELECT COUNT(1) INTO vTabCou FROM dba_all_tables
    WHERE owner = UPPER(vOwner) AND table_name = UPPER(inTableName);
    
  -- ЕСЛИ ТАБЛИЦА ОТСУТСТВУЕТ, ТО СОЗДАЕМ
  IF vTabCou = 0 THEN
   vBuff := 'EXECUTE IMMEDIATE q''{CREATE TABLE '||vTableName||' ('||CHR(10)||'  AS_OF_DATE DATE'||CHR(10);
   dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
   
   FOR idx IN (
     SELECT col_name,col_type,col_len
       FROM TABLE(DescribeColumns(inSQL))
   ) LOOP
     vBuff := ' ,'||idx.col_name||' '||idx.col_type||CASE WHEN idx.col_type = 'VARCHAR2' THEN '(4000)'/*'('||idx.col_len||')'*/ END||CHR(10);
     dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
   END LOOP;
    
   vBuff := ' ) PARTITION BY LIST(as_of_date)('||CHR(10)||
   '  PARTITION P'||to_char(inDate,'RRRRMMDD')||' VALUES(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR'')) STORAGE(INITIAL 64K NEXT 1M) NOLOGGING'||CHR(10)||
   ' ) NOLOGGING}'';'||CHR(10);
   dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

   vBuff := '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||vTableName||'.as_of_date IS ''''Отчетная дата'''' '';'||CHR(10);
   dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

   -- Вешаем комментарий на таблицу
   vBuff :=
   '  EXECUTE IMMEDIATE ''COMMENT ON TABLE '||vTableName||' IS ''''''||q''{'||inComment||'}''||'''''' '';'||CHR(10)||
   '  vBuff := ''SUCCESSFULLY :: Table "'||vTableName||'" created''||CHR(10);'||CHR(10);
   dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

   -- ЕСЛИ ТАБЛИЦА УЖЕ СУЩЕСТВУЕТ
   ELSE
    -- Т.к., к моменту текущего разворачивания, ключевые колонки (как количество так и наименование), могут измениться
    -- то необходимо добавить недостающие (если таковые найдутся)
    -- !!!Пока что предполагается, что количество может только увеличиться!!!
    FOR idx IN (
      SELECT col_name,col_type||CASE WHEN col_type = 'VARCHAR2' THEN '(4000)'/*'('||col_len||')'*/ END AS col_type
        FROM TABLE(DescribeColumns(inSQL))
      MINUS
      SELECT column_name,data_type||CASE WHEN data_type = 'VARCHAR2' THEN '('||data_length||')' END
        FROM all_tab_columns
        WHERE owner = UPPER(vOwner)
          AND table_name = UPPER(inTableName)
    ) LOOP
      vBuff :=
      'BEGIN '||CHR(10)||
      '  EXECUTE IMMEDIATE ''ALTER TABLE '||vTableName||' ADD '||idx.col_name||' '||idx.col_type||' '';'||CHR(10)||
      '  vBuff := vBuff||''SUCCESSFULLY :: Column "'||UPPER(vTableName)||'.'||UPPER(idx.col_name)||'" added''||CHR(10);'||CHR(10)||
      'EXCEPTION WHEN OTHERS THEN'||CHR(10)||
      '  BEGIN '||CHR(10)||
      '    EXECUTE IMMEDIATE ''ALTER TABLE '||vTableName||' MODIFY '||idx.col_name||' '||idx.col_type||' '';'||CHR(10)||
      '    vBuff := vBuff||''SUCCESSFULLY :: Column "'||UPPER(vTableName)||'.'||UPPER(idx.col_name)||'" modified to "'||idx.col_name||' '||idx.col_type||'"''||CHR(10);'||CHR(10)||
      '  EXCEPTION WHEN OTHERS THEN'||CHR(10)||
      '    vBuff := vBuff||''ERROR :: Column "'||UPPER(vTableName)||'.'||UPPER(idx.col_name)||'" not modified :: ''||SQLERRM||CHR(10);'||CHR(10)||
      '  END;'||CHR(10)||
      'END;'||CHR(10);
      dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
    END LOOP;

   -- Если партиция отсутствует - добавляем
   vBuff :=
    '  BEGIN'||CHR(10)||
    '    EXECUTE IMMEDIATE ''ALTER TABLE '||vTableName||' ADD PARTITION P'||to_char(inDate,'RRRRMMDD')||' VALUES (to_date('''''||to_char(inDate,'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')) STORAGE (INITIAL 64K NEXT 1M) NOLOGGING'';'||CHR(10)||
    '    vBuff := vBuff||''SUCCESSFULLY :: Table "'||vTableName||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' added'';'||CHR(10)||
    '  EXCEPTION WHEN OTHERS THEN'||CHR(10)||
    '    NULL;'||CHR(10)||
    '  END;'||CHR(10);
    dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
  END IF;

  -- Финальный END
  vBuff :=
  '  :1 := vBuff;'||CHR(10)||
  'END;'||CHR(10);
  dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

  EXECUTE IMMEDIATE vDDL USING OUT vMes;
  --dbms_output.put_line(vDDL);

  IF vMes IS NULL THEN
    vMes := 'SUCCESSFULLY :: Table "'||vTableName||'" - подготовка не требуется';
  END IF;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.PrepareTableBySQL',vMes);

  vEndTime := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Таблица: "'||vTableName||'" - окончание подготовки. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.PrepareTableBySQL',vMes);
EXCEPTION
WHEN OTHERS THEN
    vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.PrepareTableBySQL" :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.PrepareTableBySQL',vMes);

    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Таблица: "'||vTableName||'" - окончание подготовки. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.PrepareTableBySQL',vMes);
END PrepareTableBySQL;

PROCEDURE StarPrepareAnlt(inDate IN DATE,inGroupID IN NUMBER,inAnltCode IN VARCHAR2)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vStarAnltTable VARCHAR2(256);
    vAnltAlias VARCHAR2(30);
    vAnltID NUMBER;
    vAnltName VARCHAR2(4000);
    vGroupName VARCHAR2(4000);   -- наименование группы показателей измерений
    vTabCou INTEGER;

    vBuff VARCHAR2(32700);
    vDDL CLOB;
    --
    vMes VARCHAR2(4000);
    vTIBegin DATE;
    vEndTime DATE;
    ---
    errNoSpec EXCEPTION;
    errNoGroup EXCEPTION;
    errNoAnlt EXCEPTION;
BEGIN
  -- Сохранение в переменную наименования группы
  BEGIN
    SELECT group_name INTO vGroupName FROM tb_signs_group WHERE group_id = inGroupID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errNoGroup;  
  END;
  -- Сохранение в переменную альяса и ИД аналитики
  BEGIN
    SELECT anlt_alias,id,anlt_name
      INTO vAnltAlias,vAnltID,vAnltName
      FROM tb_signs_anlt
      WHERE anlt_code = UPPER(inAnltCode)
        AND inDate BETWEEN effective_start AND effective_end;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errNoAnlt;
  END;
  vStarAnltTable := LOWER(vOwner)||'.anltline_'||inGroupID||'#'||vAnltAlias;
  
  vTIBegin := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Аналитика: "'||inAnltCode||'" - начало подготовки таблицы -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);

  dbms_lob.createtemporary(vDDL,FALSE);

  vBuff :=
  'DECLARE'||CHR(10)||
  '  vBuff VARCHAR2(32700);'||CHR(10)||
  'BEGIN'||CHR(10);
  dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

  -- Проверка на существование таблиц измерений для звезды
  SELECT COUNT(1) INTO vTabCou FROM dba_all_tables
    WHERE owner = UPPER(vOwner) AND table_name = UPPER('anltline_'||inGroupID||'#'||vAnltAlias);
    
  -- ЕСЛИ ТАБЛИЦА ОТСУТСТВУЕТ, ТО СОЗДАЕМ
  IF vTabCou = 0 THEN
   vBuff := 'EXECUTE IMMEDIATE q''{CREATE TABLE '||vStarAnltTable||' ('||CHR(10)||'  AS_OF_DATE DATE'||CHR(10);
   dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
   
   FOR idx IN (
     SELECT col_name,col_type,col_len
       FROM TABLE(DescribeColumns(GetAnltLineSQL('SELECT anlt_spec_val AS id,parent_val AS parent_id,anlt_spec_name AS name,anlt_spec_val AS value FROM '||LOWER(vOwner)||'.tb_signs_anlt_spec WHERE anlt_id = '||vAnltID,'id','parent_id','name','value')))
   ) LOOP
     vBuff := ' ,'||idx.col_name||' '||idx.col_type||CASE WHEN idx.col_type = 'VARCHAR2' THEN '('||idx.col_len||')' END||CHR(10);
     dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
   END LOOP;
    
   vBuff := ' ) PARTITION BY LIST(as_of_date)('||CHR(10)||
   '  PARTITION P'||to_char(inDate,'RRRRMMDD')||' VALUES(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR'')) STORAGE(INITIAL 64K NEXT 1M) NOLOGGING'||CHR(10)||
   ' ) NOLOGGING}'';'||CHR(10);
   dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

   -- Вешаем комментарий на таблицу
   vBuff :=
   '  EXECUTE IMMEDIATE ''COMMENT ON TABLE '||vStarAnltTable||' IS ''''Иерархическое измерение: Группа - "'||vGroupName||'"; Аналитика: - "'||vAnltName||'"'''' '';'||CHR(10)||
   '  vBuff := ''SUCCESSFULLY :: Table "'||vStarAnltTable||'" created''||CHR(10);'||CHR(10);
   dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

   -- ЕСЛИ ТАБЛИЦА УЖЕ СУЩЕСТВУЕТ
   ELSE
    -- Т.к., к моменту текущего разворачивания, ключевые колонки (как количество так и наименование), могут измениться
    -- то необходимо добавить недостающие (если таковые найдутся)
    -- !!!Пока что предполагается, что количество может только увеличиться!!!
    FOR idx IN (
      SELECT col_name,col_type||CASE WHEN col_type = 'VARCHAR2' THEN '('||col_len||')' END AS col_type
        FROM TABLE(DescribeColumns(GetAnltLineSQL('SELECT anlt_spec_val AS id,parent_val AS parent_id,anlt_spec_name AS name,anlt_spec_val AS value FROM '||LOWER(vOwner)||'.tb_signs_anlt_spec WHERE anlt_id = '||vAnltID,'id','parent_id','name','value')))
      MINUS
      SELECT column_name,data_type||CASE WHEN data_type = 'VARCHAR2' THEN '('||data_length||')' END
        FROM all_tab_columns WHERE owner = UPPER(vOwner)
         AND table_name = 'ANLTLINE_'||inGroupID||'#'||vAnltAlias
    ) LOOP
      vBuff := '  EXECUTE IMMEDIATE ''ALTER TABLE '||vStarAnltTable||' ADD '||idx.col_name||' '||idx.col_type||' '';'||CHR(10)||
      '  vBuff := vBuff||''SUCCESSFULLY :: Column "'||UPPER(vStarAnltTable)||'.'||UPPER(idx.col_name)||'" added''||CHR(10);'||CHR(10);
      dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
    END LOOP;

   -- Если партиция отсутствует - добавляем
   vBuff :=
    '  BEGIN'||CHR(10)||
    '    EXECUTE IMMEDIATE ''ALTER TABLE '||vStarAnltTable||' ADD PARTITION P'||to_char(inDate,'RRRRMMDD')||' VALUES (to_date('''''||to_char(inDate,'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')) STORAGE (INITIAL 64K NEXT 1M) NOLOGGING'';'||CHR(10)||
    '    vBuff := vBuff||''SUCCESSFULLY :: Table "'||vStarAnltTable||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' added'';'||CHR(10)||
    '  EXCEPTION WHEN OTHERS THEN'||CHR(10)||
    '    NULL;'||CHR(10)||
    '  END;'||CHR(10);
    dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
  END IF;

  -- Финальный END
  vBuff :=
  '  :1 := vBuff;'||CHR(10)||
  'END;'||CHR(10);
  dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);

  EXECUTE IMMEDIATE vDDL USING OUT vMes;
  --dbms_output.put_line(vDDL);

  IF vMes IS NULL THEN
    vMes := 'SUCCESSFULLY :: Table "'||vStarAnltTable||'" - подготовка не требуется';
  END IF;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);

  vEndTime := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Аналитика: "'||inAnltCode||'" - окончание подготовки таблицы. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);
EXCEPTION
  WHEN errNoGroup THEN
    vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" не найдена в таблице "'||UPPER(vOwner)||'.TB_SIGNS_GROUP"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);
    
    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Аналитика: "'||inAnltCode||'" - окончание подготовки таблицы. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);
  WHEN errNoAnlt THEN
    vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Аналитика: "'||inAnltCode||'" не найдена в таблице "'||UPPER(vOwner)||'.TB_SIGNS_ANLT"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);

    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Аналитика: "'||inAnltCode||'" - окончание подготовки таблицы. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);
  WHEN errNoSpec THEN
    vMes := 'SUCCESSFULLY :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Аналитика: "'||inAnltCode||'" не имеет спецификации, подготовка не требуется';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);

    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Аналитика: "'||inAnltCode||'" - окончание подготовки таблицы. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);
  WHEN OTHERS THEN
    vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt" :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);

    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Аналитика: "'||inAnltCode||'" - окончание подготовки таблицы. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareAnlt',vMes);
END StarPrepareAnlt;

PROCEDURE StarPrepareFct(inDate IN DATE,inGroupID IN NUMBER)
  IS
    vResBuff VARCHAR2(500);
    --
    vBuff VARCHAR2(32700);
    vCreateDDL CLOB;
    vAddPartDDL CLOB;
    vAddSubPartDDL CLOB;
    vFields VARCHAR2(32700);
    vCreateFields VARCHAR2(32700);
    vAnltCodes VARCHAR2(32700);
    vPartCou INTEGER := 0;
    vGroupName VARCHAR2(4000);

    vMes VARCHAR2(4000);
    vTIBegin DATE;
    vENdTime DATE;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  -- Получение наименования группы
  SELECT group_name INTO vGroupName FROM tb_signs_group WHERE group_id = inGroupID;

  vTIBegin := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - начало подготовки таблицы -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareFct',vMes);

  -- Получение и сохранение в строки ключевых колонок
  SELECT LISTAGG(anlt_alias,',') WITHIN GROUP (ORDER BY anlt_alias) AS Fields
        ,LISTAGG(anlt_alias||CASE WHEN data_type = 'Число' THEN ' NUMBER'
                      WHEN data_type = 'Дата' THEN ' DATE'
                 ELSE ' VARCHAR2(4000)' END
                 ,',') WITHIN GROUP (ORDER BY anlt_alias) AS CreateFields
        ,LISTAGG(anlt_alias||CASE WHEN data_type = 'Число' THEN ' NUMBER'
                      WHEN data_type = 'Дата' THEN ' DATE'
                 ELSE ' VARCHAR2(4000)' END||';'||anlt_alias_descr
                 ,',') WITHIN GROUP (ORDER BY anlt_alias) AS anlt_alias_descr
    INTO vFields,vCreateFields,vAnltCodes
    FROM (
      SELECT a.anlt_alias
            ,a.data_type
            ,MAX(a.anlt_alias_descr) AS anlt_alias_descr
        FROM tb_signs_2_group s2g
             INNER JOIN tb_signs_group g
               ON g.parent_group_id = s2g.group_id
             INNER JOIN tb_sign_2_anlt s2a
               ON s2a.sign_name = s2g.sign_name
                  AND EXISTS (SELECT NULL FROM tb_anlt_2_group WHERE anlt_code = s2a.anlt_code AND group_id = g.group_id)
             INNER JOIN tb_signs_anlt a
               ON a.anlt_code = s2a.anlt_code
                  AND inDate BETWEEN a.effective_start AND a.effective_end
        WHERE s2g.group_id = inGroupID
      GROUP BY a.anlt_alias,a.data_type
    );

  /*********************** Формирование и выполнение CreateDDL *****************************/

  dbms_lob.createtemporary(vCreateDDL,FALSE);

  vBuff :=
  'BEGIN'||CHR(10);
  dbms_lob.writeappend(vCreateDDL,LENGTH(vBuff),vBuff);

  -- Формирование добавления вновь появившихся ключевых колонок. Если ошибка, то таблица не существует.
  -- Оборачиваем блок добавления EXCEPTION'ом на такой случай
  FOR alt IN (
   SELECT '  BEGIN'||CHR(10)||
          '    EXECUTE IMMEDIATE ''ALTER TABLE '||LOWER(vOwner)||'.fct_'||inGroupID||' ADD '||SUBSTR(b.str,1,INSTR(b.str,';',1,1)-1)||''';'||CHR(10)||
          '    EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||LOWER(vOwner)||'.fct_'||inGroupID||'.'||LOWER(SUBSTR(b.str,1,INSTR(b.str,' ',1,1)-1))||' IS '''''||a.anlt_alias_descr||''''' '';'||CHR(10)||
          '  EXCEPTION WHEN OTHERS THEN'||CHR(10)||
          '    NULL;'||CHR(10)||
          '  END;'||CHR(10) AS alt_ddl
     FROM TABLE(parse_str(vAnltCodes,',')) b
          LEFT JOIN tb_signs_anlt a
            ON a.anlt_code = SUBSTR(b.str,INSTR(b.str,';',1,1) + 1,LENGTH(b.str))
               AND inDate BETWEEN a.effective_start AND a.effective_end
  ) LOOP
    vBuff := alt.alt_ddl;
    dbms_lob.writeappend(vCreateDDL,LENGTH(vBuff),vBuff);
  END LOOP;

  -- Создание таблицы
  vBuff :=
  '  EXECUTE IMMEDIATE'||CHR(10)||
  '  ''CREATE TABLE '||LOWER(vOwner)||'.fct_'||inGroupID||CHR(10)||
  '    (as_of_date DATE'||CHR(10)||
  '    ,obj_gid VARCHAR2(256)'||CHR(10)||
  '    ,source_system_id VARCHAR2(30)'||CHR(10)||
  '    ,sign_name VARCHAR2(256)'||CHR(10)||
  '    ,sgn_alias VARCHAR2(256)'||CHR(10)||
  '    ,sign_val VARCHAR2(4000),'||CHR(10)||vCreateFields||')'||CHR(10)||
  '  PARTITION BY LIST (as_of_date)'||CHR(10)||
  '  SUBPARTITION BY LIST (sgn_alias) ('||CHR(10)||
  '  PARTITION P'||to_char(inDate,'RRRRMMDD')||' VALUES(to_date('''''||to_char(inDate,'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')) STORAGE(INITIAL 64K NEXT 4M) NOLOGGING ('||CHR(10);
  dbms_lob.writeappend(vCreateDDL,LENGTH(vBuff),vBuff);

  FOR idx IN (
    SELECT DISTINCT
           NVL(s2g.sgn_alias,s2g.sign_name) AS sign_name
          ,'SP'||ora_hash(NVL(s2g.sgn_alias,s2g.sign_name)) AS sp_code
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_pool p
             ON p.sign_name = s2g.sign_name
                AND GetConditionResult(p.condition,'INBEGDATE DATE '||to_char(inDate,'DD.MM.RRRR'),s2g.sign_name) = 1
      WHERE s2g.group_id = inGroupID
  ) LOOP
     vBuff :=
     '   '||CASE WHEN vPartCou > 0 THEN ',' END||'SUBPARTITION '||idx.sp_code||'_'||to_char(inDate,'RRRRMMDD')||' VALUES('''''||idx.sign_name||''''')'||CHR(10);
     dbms_lob.writeappend(vCreateDDL,LENGTH(vBuff),vBuff);
     vPartCou := vPartCou + 1;
  END LOOP;

  vBuff := '  )) NOLOGGING''; '||CHR(10)||CHR(10);
  dbms_lob.writeappend(vCreateDDL,LENGTH(vBuff),vBuff);

  -- Добавление комментариев на колонки
  vBuff :=
  '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||LOWER(vOwner)||'.fct_'||inGroupID||'.as_of_date IS ''''Отчетная дата'''' '';'||CHR(10)||
  '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||LOWER(vOwner)||'.fct_'||inGroupID||'.obj_gid IS ''''ИД объекта (зависит от сущности, например на договорах CONTRACT_GID и т.д.)'''' '';'||CHR(10)||
  '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||LOWER(vOwner)||'.fct_'||inGroupID||'.source_system_id IS ''''ИД системы - источника'''' '';'||CHR(10)||
  '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||LOWER(vOwner)||'.fct_'||inGroupID||'.sign_name IS ''''Наименование показателя'''' '';'||CHR(10)||
  '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||LOWER(vOwner)||'.fct_'||inGroupID||'.sgn_alias IS ''''Альяс показателя'''' '';'||CHR(10)||
  '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||LOWER(vOwner)||'.fct_'||inGroupID||'.sign_val IS ''''Значение показателя'''' '';'||CHR(10);
  dbms_lob.writeappend(vCreateDDL,LENGTH(vBuff),vBuff);

  FOR idx IN (
    SELECT DISTINCT
           a.anlt_alias
          ,MAX(a.anlt_alias_descr) KEEP (dense_rank LAST ORDER BY a.effective_start) AS col_descr
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_group g
             ON g.parent_group_id = s2g.group_id
           INNER JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = s2g.sign_name
                AND EXISTS (SELECT NULL FROM tb_anlt_2_group WHERE anlt_code = s2a.anlt_code AND group_id = g.group_id)
           INNER JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inDate BETWEEN a.effective_start AND a.effective_end
      WHERE s2g.group_id = inGroupID
    GROUP BY a.anlt_alias
  ) LOOP
    vBuff := '  EXECUTE IMMEDIATE ''COMMENT ON COLUMN '||LOWER(vOwner)||'.fct_'||inGroupID||'.'||LOWER(idx.anlt_alias)||' IS '''''||idx.col_descr||''''' '';'||CHR(10);
    dbms_lob.writeappend(vCreateDDL,LENGTH(vBuff),vBuff);
  END LOOP;

  -- Добавление комментария на таблицу
  vBuff := '  EXECUTE IMMEDIATE ''COMMENT ON TABLE '||LOWER(vOwner)||'.fct_'||inGroupID||' IS '''''||vGroupNAme||''''' '';'||CHR(10);
  dbms_lob.writeappend(vCreateDDL,LENGTH(vBuff),vBuff);

  -- Логирование и обработка ошибок
  vBuff :=
  '  :1 := ''SUCCESSFULLY :: Table "'||LOWER(vOwner)||'.fct_'||inGroupID||'" created'';'||CHR(10)||
  'EXCEPTION WHEN OTHERS THEN'||CHR(10)||
  '  :1 := NULL;'||CHR(10)||
  'END;';
  dbms_lob.writeappend(vCreateDDL,LENGTH(vBuff),vBuff);

  EXECUTE IMMEDIATE vCreateDDL USING OUT vResBuff;
  --dbms_output.put_line(vCreateDDL);

  IF vResBuff IS NOT NULL THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareFct',vResBuff);
  END IF;

  /*********************** Формирование и выполнение AddPartDDL *****************************/
  dbms_lob.createtemporary(vAddPartDDL,FALSE);

  vBuff :=
  'BEGIN'||CHR(10)||
  '  EXECUTE IMMEDIATE ''ALTER TABLE '||LOWER(vOwner)||'.fct_'||inGroupID||' ADD PARTITION P'||to_char(inDate,'RRRRMMDD')||' VALUES(to_date('''''||to_char(inDate,'DD.MM.YYYY')||''''',''''DD.MM.YYYY'''')) STORAGE(INITIAL 64K NEXT 4M) NOLOGGING ('||CHR(10);
  dbms_lob.writeappend(vAddPartDDL,LENGTH(vBuff),vBuff);

  vPartCou := 0;
  FOR idx IN (
    SELECT DISTINCT
           NVL(s2g.sgn_alias,s2g.sign_name) AS sign_name
          ,'SP'||ora_hash(NVL(s2g.sgn_alias,s2g.sign_name)) AS sp_code
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_pool p
             ON p.sign_name = s2g.sign_name
                AND GetConditionResult(p.condition,'INBEGDATE DATE '||to_char(inDate,'DD.MM.RRRR'),s2g.sign_name) = 1
      WHERE s2g.group_id = inGroupID
  ) LOOP
     vBuff :=
     '   '||CASE WHEN vPartCou > 0 THEN ',' END||'SUBPARTITION '||idx.sp_code||'_'||to_char(inDate,'RRRRMMDD')||' VALUES('''''||idx.sign_name||''''')'||CHR(10);
     dbms_lob.writeappend(vAddPartDDL,LENGTH(vBuff),vBuff);
     vPartCou := vPartCou + 1;
  END LOOP;

  vBuff :=
  ')'';'||CHR(10)||
  '  :1 := ''SUCCESSFULLY :: Table "'||LOWER(vOwner)||'.fct_'||inGroupID||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' added'';'||CHR(10)||
  'EXCEPTION WHEN OTHERS THEN'||CHR(10)||
  '  :1 := NULL;'||CHR(10)||
  'END;';
  dbms_lob.writeappend(vAddPartDDL,LENGTH(vBuff),vBuff);

  EXECUTE IMMEDIATE vAddPartDDL USING OUT vResBuff;
  --dbms_output.put_line(vAddPartDDL);

  IF vResBuff IS NOT NULL THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareFct',vResBuff);
  END IF;

  /*********************** Формирование и выполнение AddSubPartDDL *****************************/

  dbms_lob.createtemporary(vAddSubPartDDL,FALSE);

  vBuff :=
  'DECLARE'||CHR(10)||
  '  vCou INTEGER := 0;'||CHR(10)||
  'BEGIN'||CHR(10);
  dbms_lob.writeappend(vAddSubPartDDL,LENGTH(vBuff),vBuff);

  FOR idx IN (
    SELECT DISTINCT
           NVL(s2g.sgn_alias,s2g.sign_name) AS sign_name
          ,'SP'||ora_hash(NVL(s2g.sgn_alias,s2g.sign_name)) AS sp_code
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_pool p
             ON p.sign_name = s2g.sign_name
                AND GetConditionResult(p.condition,'INBEGDATE DATE '||to_char(inDate,'DD.MM.RRRR'),s2g.sign_name) = 1
      WHERE s2g.group_id = inGroupID
  ) LOOP
     vBuff :=
     'BEGIN'||CHR(10)||
     '  EXECUTE IMMEDIATE ''ALTER TABLE '||LOWER(vOwner)||'.fct_'||inGroupID||' MODIFY PARTITION P'||to_char(inDate,'RRRRMMDD')||CHR(10)||
     '    ADD SUBPARTITION '||idx.sp_code||'_'||to_char(inDate,'RRRRMMDD')||' VALUES('''''||idx.sign_name||''''') '';'||CHR(10)||
     '  vCou := vCou + 1;'||CHR(10)||
     'EXCEPTION WHEN OTHERS THEN'||CHR(10)||
     '  NULL;'||CHR(10)||
     'END;'||CHR(10);
     dbms_lob.writeappend(vAddSubPartDDL,LENGTH(vBuff),vBuff);
  END LOOP;

  vBuff :=
  '  :1 := ''SUCCESSFULLY :: Table "'||LOWER(vOwner)||'.fct_'||inGroupID||'" - ''||vCou||'' SubPartitions added'';'||CHR(10)||
  'EXCEPTION WHEN OTHERS THEN'||CHR(10)||
  '  :1 := NULL;'||CHR(10)||
  'END;'||CHR(10);
  dbms_lob.writeappend(vAddSubPartDDL,LENGTH(vBuff),vBuff);

  EXECUTE IMMEDIATE vAddSubPartDDL USING OUT vResBuff;
  --dbms_output.put_line(vAddSubPartDDL);

  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareFct',vResBuff);

  vEndTime := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - окончание подготовки таблицы. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareFct',vMes);
EXCEPTION WHEN OTHERS THEN
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepareFct','ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||' :: '||SQLERRM);
END StarPrepareFct;

/*PROCEDURE StarFctOnDate(inDate IN DATE,inGroupID IN NUMBER,inEntityID IN NUMBER)
  IS
    vTIBegin DATE;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vMes VARCHAR2(4000);
    -- список наименований полей через запятую (для использования при построении динамического SQL)
    vOtherFields VARCHAR2(4000);
    vAnltFieldsPref VARCHAR2(4000);
    vAnltFields VARCHAR2(4000);
    vAnltJoins VARCHAR2(4000);
    --
    vDML CLOB;

    vBuff VARCHAR2(32700);
    vHistTable VARCHAR2(256);    -- наименование таблицы хранения периодами
    vFctTable VARCHAR2(256);     -- наименование таблицы хранения по датам
    vGroupName VARCHAR2(4000);   -- наименование группы показателей
    vEntityName VARCHAR2(4000);  -- нименование сущности
    vRowCou INTEGER := 0;
    vAnltCou INTEGER := 0;
    vAlsCou INTEGER := 0;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vMes := 'START :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarFctOnDate" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDate',vMes);
  -- Сохранение наименований сущности и её таблиц хранения в переменные
  BEGIN
    SELECT vOwner||'.'||fct_table_name AS FctTable
          ,vOwner||'.'||hist_table_name AS HistTable
          ,entity_name
      INTO vFctTable,vHistTable\*,vFctView*\,vEntityName
      FROM tb_entity
      WHERE ID = inEntityID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Описание сущности ID = '||inEntityID||' не найдено в таблице '||vOwner||'.tb_entity');
  END;

  -- Сохранение наименования группы в переменную
  BEGIN
    SELECT group_name
      INTO vGroupName
      FROM tb_signs_group
      WHERE group_id = inGroupID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Описание группы ID = '||inGroupID||' не найдено в таблице '||vOwner||'.tb_signs_group');
  END;

  vTIBegin := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.RRRR')||'" - Группа: "'||vGroupName||'" - Сущность: "'||vEntityName||'" - вставка данных -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDate',vMes);

  -- Формирование строковых переменных со списком полей через запятую
  FOR idx IN (
    SELECT p.sign_name
          ,p.data_type
          ,p.sign_descr
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_pool p
             ON p.sign_name = s2g.sign_name
                AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                    START WITH id = inEntityID)
                AND GetConditionResult(p.condition,'INBEGDATE DATE '||to_char(inDate,'DD.MM.RRRR'),s2g.sign_name) = 1
      WHERE s2g.group_id = inGroupID
  ) LOOP
      vOtherFields := vOtherFields||CHR(10)||','''||idx.sign_name||'''';
  END LOOP;
  vOtherFields := SUBSTR(vOtherFields,3,LENGTH(vOtherFields) - 2);

  -- Формирование строк с полями и джойнов для аналитик
  SELECT LISTAGG(anlt_alias||'.sign_val'||CASE WHEN suka_flg IS NULL THEN '||''#!#''||'||anlt_alias||'.source_system_id' END||' AS '||anlt_alias,',') WITHIN GROUP (ORDER BY anlt_alias) AS FieldsPref
        ,LISTAGG(anlt_alias,',') WITHIN GROUP (ORDER BY anlt_alias) AS Fields
        ,LISTAGG(' LEFT JOIN '||anlt_alias||CHR(10)||
                 '   ON '||anlt_alias||'.'||'sign_name = fct.sign_name'||CHR(10)||
                 '      AND '||anlt_alias||'.'||'obj_gid = fct.obj_gid'||CHR(10)||
                 '      AND '||anlt_alias||'.'||'source_system_id = fct.source_system_id',CHR(10)
                ) WITHIN GROUP (ORDER BY anlt_alias) AS joins
    INTO vAnltFieldsPref,vAnltFields,vAnltJoins
    FROM (
      SELECT DISTINCT a.anlt_alias,CASE WHEN EXISTS(SELECT NULL FROM tb_signs_anlt_spec WHERE anlt_id = a.id) THEN 'СУKA' END AS suka_flg
        FROM tb_signs_2_group s2g
             INNER JOIN tb_signs_pool p
               ON p.sign_name = s2g.sign_name
                  AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                      START WITH id = inEntityID)
             LEFT JOIN tb_sign_2_anlt s2a
               ON s2a.sign_name = s2g.sign_name
                  AND EXISTS (SELECT NULL
                                FROM tb_anlt_2_group
                                WHERE anlt_code = s2a.anlt_code
                                  AND group_id IN (SELECT group_id FROM tb_signs_group
                                                   CONNECT BY PRIOR group_id = parent_group_id
                                                   START WITH group_id = inGroupID)
                             )
             LEFT JOIN tb_signs_anlt a
               ON a.anlt_code = s2a.anlt_code
                  AND inDate BETWEEN a.effective_start AND a.effective_end
        WHERE s2g.group_id = inGroupID
      GROUP BY a.anlt_alias,a.id
  );

  dbms_lob.createtemporary(vDML,FALSE);
  vBuff :=
  'BEGIN'||CHR(10)||
  'EXECUTE IMMEDIATE ''ALTER SESSION SET nls_numeric_characters = '''', '''''';'||CHR(10)||
  'EXECUTE IMMEDIATE ''ALTER SESSION SET nls_date_format = ''''DD.MM.RRRR HH24:MI:SS'''''';'||CHR(10)||
  'INSERT INTO '||lower(vOwner)||'.fct_'||inGroupID||'(as_of_date,obj_gid,source_system_id,sign_name,sgn_alias,sign_val,'||vAnltFields||')'||CHR(10)||
  'WITH'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  -- Формирование подзапросов для аналитик
  FOR als IN (
    SELECT a.anlt_alias
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_pool p
             ON p.sign_name = s2g.sign_name
                AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                    START WITH id = inEntityID)
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = s2g.sign_name
                AND EXISTS (SELECT NULL
                              FROM tb_anlt_2_group
                              WHERE anlt_code = s2a.anlt_code
                                AND group_id IN (SELECT group_id FROM tb_signs_group
                                                 CONNECT BY PRIOR group_id = parent_group_id
                                                 START WITH group_id = inGroupID)
                           )
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inDate BETWEEN a.effective_start AND a.effective_end
      WHERE s2g.group_id = inGroupID
    GROUP BY a.anlt_alias
  ) LOOP
    vBuff := CASE WHEN vAlsCou > 0 THEN ',' END||als.anlt_alias||' AS ('||CHR(10);
    dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
    vAnltCou := 0;
    FOR idx IN (
      SELECT LISTAGG(''''||p.sign_name||'''',',') WITHIN GROUP (ORDER BY s2g.sign_name) AS sign_name
            ,s2a.anlt_code
            ,lower(vOwner)||'.'||e.fct_table_name AS a_fct_table
            ,lower(vOwner)||'.'||e.hist_table_name AS a_hist_table
            ,a.anlt_alias
        FROM tb_signs_2_group s2g
             INNER JOIN tb_signs_pool p
               ON p.sign_name = s2g.sign_name
                  AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                      START WITH id = inEntityID)
                  AND GetConditionResult(p.condition,'INBEGDATE DATE '||to_char(inDate,'DD.MM.RRRR'),s2g.sign_name) = 1
             LEFT JOIN tb_sign_2_anlt s2a
               ON s2a.sign_name = s2g.sign_name
                  AND EXISTS (SELECT NULL
                                FROM tb_anlt_2_group
                                WHERE anlt_code = s2a.anlt_code
                                  AND group_id IN (SELECT group_id FROM tb_signs_group
                                                   CONNECT BY PRIOR group_id = parent_group_id
                                                   START WITH group_id = inGroupID)
                             )
             LEFT JOIN tb_signs_anlt a
               ON a.anlt_code = s2a.anlt_code
                  AND inDate BETWEEN a.effective_start AND a.effective_end
             LEFT JOIN tb_entity e
               ON e.id = a.entity_id
        WHERE s2g.group_id = inGroupID
          AND a.anlt_alias = als.anlt_alias
      GROUP BY a.anlt_alias,s2a.anlt_code,e.fct_table_name,e.hist_table_name
      HAVING s2a.anlt_code IS NOT NULL
    ) LOOP
      vBuff :=
      CASE WHEN vAnltCou > 0 THEN '  UNION ALL'||CHR(10) END||
      '  SELECT sign_name,obj_gid,source_system_id,sign_val'||CHR(10)||
      '    FROM '||idx.a_fct_table||CHR(10)||
      '    WHERE sign_name IN ('||idx.sign_name||')'||CHR(10)||
      '      AND as_of_date = to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'')'||CHR(10)||
      '  UNION ALL'||CHR(10)||
      '  SELECT \*+ no_index(v) *\ v.sign_name,v.obj_gid,v.source_system_id,v.sign_val'||CHR(10)||
      '    FROM '||idx.a_hist_table||' v'||CHR(10)||
      '    WHERE v.sign_name IN ('||idx.sign_name||')'||CHR(10)||
      '      AND to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') BETWEEN v.effective_start AND v.effective_end'||CHR(10);
      dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
      vAnltCou := vAnltCou + 1;
    END LOOP;

    vBuff := ')'||CHR(10);
    dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
    vAlsCou := vAlsCou + 1;
  END LOOP;
  -- Окончание формирования подзапросов для аналитик

  vBuff :=
  ',fct AS ('||CHR(10)||
  'SELECT \*+ no_index(fct) *\
          to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') AS as_of_date,fct.obj_gid,fct.source_system_id,fct.sign_name,CASE WHEN fct.sign_val = ''0,'' THEN null ELSE fct.sign_val END AS sign_val'||CHR(10)||
  '  FROM '||vHistTable||' fct'||CHR(10)||
  '  WHERE fct.sign_name IN ('||vOtherFields||')'||CHR(10)||
  '    AND to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') BETWEEN fct.effective_start AND fct.effective_end'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  vBuff :=
  'UNION ALL'||CHR(10)||
  'SELECT to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') AS as_of_date,fct.obj_gid,fct.source_system_id,fct.sign_name,CASE WHEN fct.sign_val = ''0,'' THEN null ELSE fct.sign_val END AS sign_val'||CHR(10)||
  '  FROM '||vFctTable||' fct'||CHR(10)||
  '  WHERE fct.sign_name IN ('||vOtherFields||')'||CHR(10)||
  '    AND fct.as_of_date = to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY''))'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  -- Подзапрос альясов не ключевых показателей
  vBuff :=
  ',als AS ('||CHR(10)||
  '   SELECT \*+ no_index(s2g) *\ s2g.sign_name,NVL(s2g.sgn_alias,s2g.sign_name) AS sgn_alias FROM tb_signs_2_group s2g WHERE s2g.group_id = '||inGroupID||' AND s2g.sign_name IN ('||vOtherFields||')'||CHR(10)||
  ')'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
  -- Окончание подзапроса альясов не ключевых показателей

  vBuff :=
  'SELECT fct.as_of_date,fct.obj_gid,fct.source_system_id,fct.sign_name,als.sgn_alias AS sign_name,fct.sign_val,'||vAnltFieldsPref||CHR(10)||
  '  FROM fct '||CHR(10)||vAnltJoins||CHR(10)||' LEFT JOIN als ON als.sign_name = fct.sign_name'||CHR(10)||
  '  WHERE fct.sign_val IS NOT NULL;'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  vBuff :=
  ':1 := SQL%ROWCOUNT;'||CHR(10)||
  'COMMIT;'||CHR(10)||
  'END;'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  EXECUTE IMMEDIATE vDML USING OUT vRowCou;
  --dbms_output.put_line(vDML);

  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Сущность: "'||vEntityName||'" - '||vRowCou||' rows inserted into table '||lower(vOwner)||'.fct_'||inGroupID||' in '||get_ti_as_hms(vEndTime - vTIBegin);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDate',vMes);

  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Сущность: "'||vEntityName||'" - окончание вставки данных. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDate',vMes);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarFctOnDate" finished successfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDate',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarFctOnDate" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDate',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarFctOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDate',vMes);
END StarFctOnDate;*/

PROCEDURE StarFctOnDateSign(inDate IN DATE,inGroupID IN NUMBER,inSign IN VARCHAR2)
  IS
    vTIBegin DATE;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vMes VARCHAR2(4000);
    -- список наименований полей через запятую (для использования при построении динамического SQL)
    vAnltFieldsPref VARCHAR2(4000);
    vAnltFields VARCHAR2(4000);
    vAnltJoins VARCHAR2(4000);
    vEntityId NUMBER;
    --
    vDML CLOB;

    vBuff VARCHAR2(32700);
    vHistTable VARCHAR2(256);    -- наименование таблицы хранения периодами
    vFctTable VARCHAR2(256);     -- наименование таблицы хранения по датам
    vGroupName VARCHAR2(4000);   -- наименование группы показателей
    vRowCou INTEGER := 0;
    vAnltCou INTEGER := 0;
    vAlsCou INTEGER := 0;
    vOwner VARCHAR2(4000) := pkg_etl_signs.GetVarValue('vOwner');
BEGIN
  vMes := 'START :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign',vMes);
  -- Сохранение наименований сущности и её таблиц хранения в переменные
  BEGIN
    SELECT vOwner||'.'||e.fct_table_name AS FctTable
          ,vOwner||'.'||e.hist_table_name AS HistTable
          ,s.entity_id
      INTO vFctTable,vHistTable,vEntityId
      FROM tb_signs_pool s
           INNER JOIN tb_entity e
             ON e.id = s.entity_id
      WHERE s.sign_name = inSign;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Описание показателя или сущности не найдено в таблицах "'||vOwner||'.tb_signs_pool; '||vOwner||'.tb_entity"');
  END;

  -- Сохранение наименования группы в переменную
  BEGIN
    SELECT group_name
      INTO vGroupName
      FROM tb_signs_group
      WHERE group_id = inGroupID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Описание группы ID = '||inGroupID||' не найдено в таблице '||vOwner||'.tb_signs_group');
  END;

  vTIBegin := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.RRRR')||'" - Группа: "'||vGroupName||'" - Показатель: "'||UPPER(inSign)||'" - вставка данных -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign',vMes);

  -- Формирование строк с полями и джойнов для аналитик
  SELECT LISTAGG(anlt_alias||'.sign_val'||CASE WHEN suka_flg IS NULL THEN '||''#!#''||'||anlt_alias||'.source_system_id' END||' AS '||anlt_alias,',') WITHIN GROUP (ORDER BY anlt_alias) AS FieldsPref
        ,LISTAGG(anlt_alias,',') WITHIN GROUP (ORDER BY anlt_alias) AS Fields
        ,LISTAGG(' LEFT JOIN '||anlt_alias||CHR(10)||
                 '   ON '||anlt_alias||'.'||'sign_name = fct.sign_name'||CHR(10)||
                 '      AND '||anlt_alias||'.'||'obj_gid = fct.obj_gid'||CHR(10)||
                 '      AND '||anlt_alias||'.'||'source_system_id = fct.source_system_id',CHR(10)
                ) WITHIN GROUP (ORDER BY anlt_alias) AS joins
    INTO vAnltFieldsPref,vAnltFields,vAnltJoins
    FROM (
      SELECT DISTINCT a.anlt_alias,CASE WHEN EXISTS(SELECT NULL FROM tb_signs_anlt_spec WHERE anlt_id = a.id) THEN 'СУKA' END AS suka_flg
        FROM tb_signs_2_group s2g
             INNER JOIN tb_signs_pool p
               ON p.sign_name = s2g.sign_name
                  AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                      START WITH id = vEntityID)
             LEFT JOIN tb_sign_2_anlt s2a
               ON s2a.sign_name = s2g.sign_name
                  AND EXISTS (SELECT NULL
                                FROM tb_anlt_2_group
                                WHERE anlt_code = s2a.anlt_code
                                  AND group_id IN (SELECT group_id FROM tb_signs_group
                                                   CONNECT BY PRIOR group_id = parent_group_id
                                                   START WITH group_id = inGroupID)
                             )
             LEFT JOIN tb_signs_anlt a
               ON a.anlt_code = s2a.anlt_code
                  AND inDate BETWEEN a.effective_start AND a.effective_end
        WHERE s2g.group_id = inGroupID
          AND s2g.sign_name = UPPER(inSign)
      GROUP BY a.anlt_alias,a.id
  );

  dbms_lob.createtemporary(vDML,FALSE);
  vBuff :=
  'BEGIN'||CHR(10)||
  'EXECUTE IMMEDIATE ''ALTER SESSION SET nls_numeric_characters = '''', '''''';'||CHR(10)||
  'EXECUTE IMMEDIATE ''ALTER SESSION SET nls_date_format = ''''DD.MM.RRRR HH24:MI:SS'''''';'||CHR(10)||
  'INSERT INTO '||lower(vOwner)||'.fct_'||inGroupID||'(as_of_date,obj_gid,source_system_id,sign_name,sgn_alias,sign_val,'||vAnltFields||')'||CHR(10)||
  'WITH'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  -- Формирование подзапросов для аналитик
  FOR als IN (
    SELECT a.anlt_alias
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_pool p
             ON p.sign_name = s2g.sign_name
                AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                    START WITH id = vEntityID)
           LEFT JOIN tb_sign_2_anlt s2a
             ON s2a.sign_name = s2g.sign_name
                AND EXISTS (SELECT NULL
                              FROM tb_anlt_2_group
                              WHERE anlt_code = s2a.anlt_code
                                AND group_id IN (SELECT group_id FROM tb_signs_group
                                                 CONNECT BY PRIOR group_id = parent_group_id
                                                 START WITH group_id = inGroupID)
                           )
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = s2a.anlt_code
                AND inDate BETWEEN a.effective_start AND a.effective_end
      WHERE s2g.group_id = inGroupID
        AND s2g.sign_name = UPPER(inSign)
    GROUP BY a.anlt_alias
  ) LOOP
    vBuff := CASE WHEN vAlsCou > 0 THEN ',' END||als.anlt_alias||' AS ('||CHR(10);
    dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
    vAnltCou := 0;
    FOR idx IN (
      SELECT LISTAGG(''''||p.sign_name||'''',',') WITHIN GROUP (ORDER BY s2g.sign_name) AS sign_name
            ,s2a.anlt_code
            ,lower(vOwner)||'.'||e.fct_table_name AS a_fct_table
            ,lower(vOwner)||'.'||e.hist_table_name AS a_hist_table
            ,a.anlt_alias
        FROM tb_signs_2_group s2g
             INNER JOIN tb_signs_pool p
               ON p.sign_name = s2g.sign_name
                  AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                      START WITH id = vEntityID)
                  AND pkg_etl_signs.GetConditionResult(p.condition,'INBEGDATE DATE '||to_char(inDate,'DD.MM.RRRR'),s2g.sign_name) = 1
             LEFT JOIN tb_sign_2_anlt s2a
               ON s2a.sign_name = s2g.sign_name
                  AND EXISTS (SELECT NULL
                                FROM tb_anlt_2_group
                                WHERE anlt_code = s2a.anlt_code
                                  AND group_id IN (SELECT group_id FROM tb_signs_group
                                                   CONNECT BY PRIOR group_id = parent_group_id
                                                   START WITH group_id = inGroupID)
                             )
             LEFT JOIN tb_signs_anlt a
               ON a.anlt_code = s2a.anlt_code
                  AND inDate BETWEEN a.effective_start AND a.effective_end
             LEFT JOIN tb_entity e
               ON e.id = a.entity_id
        WHERE s2g.group_id = inGroupID
          AND a.anlt_alias = als.anlt_alias
      GROUP BY a.anlt_alias,s2a.anlt_code,e.fct_table_name,e.hist_table_name
      HAVING s2a.anlt_code IS NOT NULL
    ) LOOP
      vBuff :=
      CASE WHEN vAnltCou > 0 THEN '  UNION ALL'||CHR(10) END||
      '  SELECT sign_name,obj_gid,source_system_id,sign_val'||CHR(10)||
      '    FROM '||idx.a_fct_table||CHR(10)||
      '    WHERE sign_name IN ('||idx.sign_name||')'||CHR(10)||
      '      AND as_of_date = to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'')'||CHR(10)||
      '  UNION ALL'||CHR(10)||
      '  SELECT /*+ no_index(v) */ v.sign_name,v.obj_gid,v.source_system_id,v.sign_val'||CHR(10)||
      '    FROM '||idx.a_hist_table||' v'||CHR(10)||
      '    WHERE v.sign_name IN ('||idx.sign_name||')'||CHR(10)||
      '      AND to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') BETWEEN v.effective_start AND v.effective_end'||CHR(10);
      dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
      vAnltCou := vAnltCou + 1;
    END LOOP;

    vBuff := ')'||CHR(10);
    dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
    vAlsCou := vAlsCou + 1;
  END LOOP;
  -- Окончание формирования подзапросов для аналитик

  vBuff :=
  ',fct AS ('||CHR(10)||
  'SELECT to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') AS as_of_date,fct.obj_gid,fct.source_system_id,fct.sign_name,CASE WHEN fct.sign_val = ''0,'' THEN null ELSE fct.sign_val END AS sign_val'||CHR(10)||
  '  FROM '||vHistTable||' fct'||CHR(10)||
  '  WHERE fct.sign_name = '''||UPPER(inSign)||''''||CHR(10)||
  '    AND to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') BETWEEN fct.effective_start AND fct.effective_end'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  vBuff :=
  'UNION ALL'||CHR(10)||
  'SELECT to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') AS as_of_date,fct.obj_gid,fct.source_system_id,fct.sign_name,CASE WHEN fct.sign_val = ''0,'' THEN null ELSE fct.sign_val END AS sign_val'||CHR(10)||
  '  FROM '||vFctTable||' fct'||CHR(10)||
  '  WHERE fct.sign_name = '''||UPPER(inSign)||''''||CHR(10)||
  '    AND fct.as_of_date = to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY''))'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  -- Подзапрос альясов не ключевых показателей
  vBuff :=
  ',als AS ('||CHR(10)||
  '   SELECT /*+ no_index(s2g) */ s2g.sign_name,NVL(s2g.sgn_alias,s2g.sign_name) AS sgn_alias FROM tb_signs_2_group s2g WHERE s2g.group_id = '||inGroupID||' AND s2g.sign_name = '''||UPPER(inSign)||''''||CHR(10)||
  ')'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
  -- Окончание подзапроса альясов не ключевых показателей

  vBuff :=
  'SELECT fct.as_of_date,fct.obj_gid,fct.source_system_id,fct.sign_name,als.sgn_alias AS sign_name,fct.sign_val,'||vAnltFieldsPref||CHR(10)||
  '  FROM fct '||CHR(10)||vAnltJoins||CHR(10)||' LEFT JOIN als ON als.sign_name = fct.sign_name'||CHR(10)||
  '  WHERE fct.sign_val IS NOT NULL;'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  vBuff :=
  ':1 := SQL%ROWCOUNT;'||CHR(10)||
  'COMMIT;'||CHR(10)||
  'END;'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  EXECUTE IMMEDIATE vDML USING OUT vRowCou;
  --dbms_output.put_line(vDML);

  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Показатель: "'||inSign||'" - '||vRowCou||' rows inserted into table '||lower(vOwner)||'.fct_'||inGroupID||' in '||get_ti_as_hms(vEndTime - vTIBegin);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign',vMes);

  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Показатель: "'||inSign||'" - окончание вставки данных. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign',vMes);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign" finished successfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign" :: '||SQLERRM||' :: '||CHR(10)||'-----'||CHR(10)||vDML;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFctOnDateSign',vMes);
END StarFctOnDateSign;


PROCEDURE StarAggrOnDate(inDate IN DATE,inAggrID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vGroupID NUMBER;
    vAggrSQL CLOB;
    vBuff VARCHAR2(32700);
    vCols VARCHAR2(4000);
    cur INTEGER;       -- хранит идентификатор (ID) курсора
    ret INTEGER;       -- хранит возвращаемое по вызову значение
    --
    vMes VARCHAR2(4000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrOnDate" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrOnDate',vMes);
  
  SELECT group_id,aggr_sql INTO vGroupID,vAggrSQL FROM tb_signs_aggrs WHERE id = inAggrID;
  
  SELECT LISTAGG(col_name,',') WITHIN GROUP (ORDER BY col_num) INTO vCols FROM TABLE(DescribeColumns(vAggrSQL));
  vBuff :=
  'DECLARE'||CHR(10)||
  '  vSqlRowcount INTEGER := 0;'||CHR(10)||
  'BEGIN'||CHR(10)||
  '  INSERT INTO '||LOWER(vOwner)||'.aggr_'||vGroupID||'#'||inAggrID||' (as_of_date,'||vCols||')'||CHR(10)||
  '    SELECT to_date(:inDate,''DD.MM.RRRR'') AS as_of_date,'||vCols||' FROM ('||CHR(10)||vAggrSQL||CHR(10)||
  '    );'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||lower(vOwner)||'.pkg_etl_signs.StarAggrOnDate'',''SUCCESSFULLY :: "'||to_char(inDate,'DD.MM.YYYY')||'" - ''||SQL%ROWCOUNT||'' rows inserted into "'||lower(vOwner)||'.aggr_'||vGroupID||'#'||inAggrID||'"'');'||CHR(10)||
  '  COMMIT;'||CHR(10)||
  'END;';
  
  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur,vBuff,dbms_sql.native);
  dbms_sql.bind_variable_char(cur,'inDate',to_char(inDate,'DD.MM.YYYY'));
  
  ret := dbms_sql.execute(cur);
  --dbms_output.put_line(vBuff);
  
  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrOnDate" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrOnDate',vMes);
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'"  - Описание агрегата "ID = '||inAggrID||'" не найдено в таблице "'||lower(vOwner)||'.tb_signs_aggrs"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrOnDate',vMes);
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: Агрегат "ID = '||inAggrID||'" :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrOnDate',vMes);
    
    vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrOnDate',vMes);
END StarAggrOnDate;

PROCEDURE StarDimOnDate(inDate IN DATE,inGroupID IN NUMBER,inEntityID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vTIBegin DATE;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vMes VARCHAR2(4000);
    -- список наименований полей через запятую (для использования при построении динамического SQL)
    vKeyFieldsForIns VARCHAR2(32700);
    vKeyFieldsForSel VARCHAR2(32700);
    vKeyFields VARCHAR2(32700);
    vKeyFieldsWithAlias VARCHAR2(32700);

    vDML CLOB;
    vRestrictSQL CLOB;

    vBuff VARCHAR2(32700);
    vHistTable VARCHAR2(256);    -- наименование таблицы хранения периодами
    vFctTable VARCHAR2(256);     -- наименование таблицы хранения по датам
    vFctView VARCHAR2(256);
    vStarDimTable VARCHAR2(256) := vOwner||'.dim_'||inGroupID||'#'||inEntityID; -- наименование таблицы фактов в звезде
    vGroupName VARCHAR2(4000);   -- наименование группы показателей
    vEntityName VARCHAR2(4000);  -- нименование сущности
    vTabCou INTEGER := 0;
    vRowCou INTEGER := 0;
BEGIN
  vMes := 'START :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarDimOnDate" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDimOnDate',vMes);
  -- Сохранение наименований сущности и её таблиц хранения в переменные
  BEGIN
    SELECT vOwner||'.'||fct_table_name AS FctTable
          ,vOwner||'.'||hist_table_name AS HistTable
          ,vOwner||'.v_'||hist_table_name AS FctView
          ,entity_name
      INTO vFctTable,vHistTable,vFctView,vEntityName
      FROM tb_entity
      WHERE ID = inEntityID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Описание сущности ID = '||inEntityID||' не найдено в таблице '||vOwner||'.tb_entity');
  END;

  -- Сохранение наименования группы в переменную
  BEGIN
    SELECT group_name
      INTO vGroupName
      FROM tb_signs_group
      WHERE group_id = inGroupID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Описание группы ID = '||inGroupID||' не найдено в таблице '||vOwner||'.tb_signs_group');
  END;

  vTIBegin := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Сущность: "'||vEntityName||'" - вставка данных -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDimOnDate',vMes);

  vKeyFieldsForIns := NULL;
  vKeyFieldsForSel := NULL;
  vKeyFields := NULL;
  vKeyFieldsWithAlias := NULL;

  -- Формирование строковых переменных со списком полей через запятую
  FOR idx IN (
    SELECT p.sign_name
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_pool p
             ON p.sign_name = s2g.sign_name
                AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                    START WITH id = inEntityID)
      WHERE s2g.group_id = inGroupID
  ) LOOP
      vKeyFields := vKeyFields||CHR(10)||','''||idx.sign_name||'''';
      vKeyFieldsWithAlias := vKeyFieldsWithAlias||CHR(10)||','''||idx.sign_name||''' AS '||idx.sign_name;
  END LOOP;

  FOR idx IN (
    SELECT DISTINCT
           NVL(s2g.sgn_alias,p.sign_name) AS sign_name
          ,NVL2(s2g.sgn_alias,'COALESCE('||LISTAGG(p.sign_name,',') WITHIN GROUP (ORDER BY p.id) OVER (PARTITION BY NVL(s2g.sgn_alias,p.sign_name))||',NULL) AS '||s2g.sgn_alias,p.sign_name) AS coal_sign_name
      FROM tb_signs_2_group s2g
           INNER JOIN tb_signs_pool p
             ON p.sign_name = s2g.sign_name
                AND p.entity_id IN (SELECT id FROM tb_entity CONNECT BY PRIOR id = parent_id
                                    START WITH id = inEntityID)
      WHERE s2g.group_id = inGroupID
  ) LOOP
      vKeyFieldsForIns := vKeyFieldsForIns||CHR(10)||','||lower(idx.sign_name);
      vKeyFieldsForSel := vKeyFieldsForSel||CHR(10)||','||lower(idx.coal_sign_name);
  END LOOP;

  vKeyFieldsForIns := SUBSTR(vKeyFieldsForIns,3,LENGTH(vKeyFieldsForIns) - 2);
  vKeyFieldsForSel := SUBSTR(vKeyFieldsForSel,3,LENGTH(vKeyFieldsForSel) - 2);
  vKeyFields := SUBSTR(vKeyFields,3,LENGTH(vKeyFields) - 2);
  vKeyFieldsWithAlias := SUBSTR(vKeyFieldsWithAlias,3,LENGTH(vKeyFieldsWithAlias) - 2);

  dbms_lob.createtemporary(vDML,FALSE);
  vBuff :=
  'BEGIN'||CHR(10)||
  'EXECUTE IMMEDIATE ''ALTER SESSION SET nls_numeric_characters = '''', '''''';'||CHR(10)||
  'EXECUTE IMMEDIATE ''ALTER SESSION SET nls_date_format = ''''DD.MM.YYYY HH24:MI:SS'''''';'||CHR(10)||
  'INSERT INTO '||vStarDimTable||'(as_of_date,obj_sid'||CHR(10)||','||vKeyFieldsForIns||')'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  -- Формирование блока ограничения (WITH...) для запроса вставки данных
  vBuff :=
  'WITH '||CHR(10)||'  fct_keys AS ('||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  dbms_lob.createtemporary(vRestrictSQL,FALSE);
  vBuff := 'SELECT sign_val||''#!#''||source_system_id AS obj_sid FROM ('||CHR(10);
  dbms_lob.writeappend(vRestrictSQL,LENGTH(vBuff),vBuff);

  FOR idx IN (
    SELECT LISTAGG(''''||g1.sign_name||'''',',') WITHIN GROUP (ORDER BY g1.sign_name) AS parts
          ,gr.anlt_alias
          ,CASE WHEN p.hist_flg = 0 THEN gr.fct_table_name ELSE gr.hist_table_name END AS table_name
          ,p.hist_flg
      FROM (
    SELECT g.group_id,g.parent_group_id,a2g.anlt_code,a.anlt_alias,e.entity_name
          ,e.fct_table_name
          ,e.hist_table_name
          ,(SELECT ID FROM tb_entity WHERE parent_id IS NULL CONNECT BY PRIOR parent_id = ID START WITH ID = a.entity_id) AS e_id
      FROM tb_signs_group g
           LEFT JOIN tb_anlt_2_group a2g
             ON a2g.group_id = g.group_id
           LEFT JOIN tb_signs_anlt a
             ON a.anlt_code = a2g.anlt_code
                AND inDate BETWEEN a.effective_start AND a.effective_end
           LEFT JOIN tb_entity e
             ON e.id = a.entity_id
    CONNECT BY PRIOR g.group_id = g.parent_group_id
    START WITH g.group_id = inGroupID
    ) gr LEFT JOIN tb_signs_2_group g1
           ON g1.group_id = gr.parent_group_id
              AND EXISTS (SELECT NULL FROM tb_sign_2_anlt WHERE sign_name = g1.sign_name AND anlt_code = gr.anlt_code)
         LEFT JOIN tb_signs_pool p
           ON p.sign_name = g1.sign_name
    WHERE gr.e_id = inEntityID
    GROUP BY gr.anlt_alias,p.hist_flg,CASE WHEN p.hist_flg = 0 THEN gr.fct_table_name ELSE gr.hist_table_name END
  ) LOOP
    vBuff :=
    CASE WHEN vTabCou > 0 THEN 'UNION ALL'||CHR(10) END||
    'SELECT /*+ no_index(v) */ v.sign_val,v.source_system_id FROM '||LOWER(vOwner)||'.'||idx.table_name||' v'||CHR(10)||
    '  WHERE v.sign_name IN ('||idx.parts||') AND '||
    CASE WHEN idx.hist_flg = 0 THEN 'v.as_of_date = to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'')'
    ELSE 'to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') BETWEEN v.effective_start and v.effective_end' END||CHR(10);
    dbms_lob.writeappend(vRestrictSQL,LENGTH(vBuff),vBuff);
    vTabCou := vTabCou + 1;
  END LOOP;

  vBuff := ') GROUP BY sign_val,source_system_id)'||CHR(10);
  dbms_lob.writeappend(vRestrictSQL,LENGTH(vBuff),vBuff);

  vBuff :=
  'SELECT /* не использовать parallel(2) */ to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') AS as_of_date,obj_gid||''#!#''||source_system_id as obj_sid'||CHR(10)||','||vKeyFieldsForSel||' FROM ('||CHR(10)||
  '      SELECT /*+ no_index(s) */ s.obj_gid,s.source_system_id,s.sign_name,s.sign_val'||CHR(10)||
  '        FROM '||vHistTable||' s'||CHR(10)||
  '             INNER JOIN fct_keys ON fct_keys.obj_sid = s.obj_gid||''#!#''||s.source_system_id'||CHR(10)||
  '        WHERE s.sign_name IN ('||vKeyFields||')'||CHR(10)||
  '          AND to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') BETWEEN s.effective_start AND s.effective_end'||CHR(10)||
  '      UNION ALL'||CHR(10)||
  '      SELECT s.obj_gid,s.source_system_id,s.sign_name,s.sign_val'||CHR(10)||
  '        FROM '||vFctTable||' s'||CHR(10)||
  '             INNER JOIN fct_keys ON fct_keys.obj_sid = s.obj_gid||''#!#''||s.source_system_id'||CHR(10)||
  '        WHERE sign_name IN ('||vKeyFields||')'||CHR(10)||
  '          AND s.as_of_date = to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'')'||CHR(10)||
  '    ) PIVOT (MAX(sign_val) FOR sign_name IN ('||vKeyFieldsWithAlias||'));'||CHR(10);

  dbms_lob.writeappend(vDML,dbms_lob.getlength(vRestrictSQL),vRestrictSQL);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  vBuff :=
  ':1 := SQL%ROWCOUNT;'||CHR(10)||
  'COMMIT;'||CHR(10)||
  'END;'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);

  EXECUTE IMMEDIATE vDML USING OUT vRowCou;
  --dbms_output.put_line(vDML);

  vEndTime := SYSDATE;
  vMes := 'SUCESSFULLY :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Сущность: "'||vEntityName||'" - '||vRowCou||' rows inserted into table '||vStarDimTable||' in '||get_ti_as_hms(vEndTime - vTIBegin);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDimOnDate',vMes);

  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Сущность: "'||vEntityName||'" - окончание вставки данных. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDimOnDate',vMes);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarDimOnDate" finished successfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDimOnDate',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarDimOnDate" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDimOnDate',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarDimOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDimOnDate',vMes);
END StarDimOnDate;

PROCEDURE StarAnltOnDate(inDate IN DATE,inGroupID IN NUMBER,inAnltAlias IN VARCHAR2)
  IS
    vSQL CLOB;
    vGroupName VARCHAR2(4000);
    vAnltName VARCHAR2(4000);
    vAnltSpecID VARCHAR2(4000);
    vCols VARCHAR2(32700);
    vColsAls VARCHAR2(32700);
    --
    vMes VARCHAR2(32700);
    vTIBegin DATE;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vCou INTEGER;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vMes := 'START :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate',vMes);
  -- Сохранение наименования группы в переменную
  BEGIN
    SELECT group_name
      INTO vGroupName
      FROM tb_signs_group
      WHERE group_id = inGroupID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Описание группы ID = '||inGroupID||' не найдено в таблице '||vOwner||'.tb_signs_group');
  END;
  
  -- Сохранение наименования аналитики в переменную
  BEGIN
    SELECT LISTAGG(a.anlt_code,',') WITHIN GROUP (ORDER BY a.id) AS AnltCode
              ,LISTAGG(a.id,',') WITHIN GROUP (ORDER BY a.id) AS AnltSpecID
          INTO vAnltName,vAnltSpecID
          FROM tb_signs_anlt a
          WHERE a.anlt_alias = inAnltAlias
            AND inDate BETWEEN a.effective_start AND a.effective_end -- 28,46
            AND a.anlt_code IN (SELECT a2g.anlt_code
                                  FROM tb_signs_group g
                                       LEFT JOIN tb_anlt_2_group a2g ON a2g.group_id = g.group_id
                                  WHERE LEVEL = 3
                                CONNECT BY PRIOR g.group_id = g.parent_group_id
                                START WITH g.group_id = inGroupID);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20000,'Описание аналитиики ANLT_ALIAS = '||inAnltAlias||' за дату "'||to_char(inDate,'DD.MM.YYYY')||'" не найдено в таблице '||vOwner||'.tb_signs_anlt');
  END;

  vTIBegin := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Аналитика: "'||vAnltName||'" - начало вставки данных -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate',vMes);

  vSQL := 'SELECT anlt_spec_val AS id,parent_val AS parent_id,anlt_spec_name AS name,anlt_spec_val AS val
       FROM '||lower(vOwner)||'.tb_signs_anlt_spec
     WHERE anlt_id IN ('||vAnltSpecID||')';

  vSQL := GetAnltLineSQL(vSQL,'id','parent_id','name','val');
  
  SELECT LISTAGG(col_name,',') WITHIN GROUP (ORDER BY col_name) AS a
        ,LISTAGG('A.'||col_name,',') WITHIN GROUP (ORDER BY col_name) AS b
    INTO vCols,vColsAls
    FROM TABLE(DescribeColumns(vSQL));
 
  EXECUTE IMMEDIATE
  'BEGIN'||CHR(10)||
  'INSERT INTO '||LOWER(vOwner)||'.anltline_'||inGroupID||'#'||inAnltAlias||' (AS_OF_DATE,'||vCols||')'||CHR(10)||
  '  WITH'||CHR(10)||
  '    dt AS ('||CHR(10)||
  '      SELECT to_date('''||to_char(inDate,'DD.MM.YYYY')||''',''DD.MM.YYYY'') AS as_of_date FROM dual'||CHR(10)||
  '    )'||CHR(10)||
  '    SELECT dt.as_of_date,'||vColsAls||' FROM dt CROSS JOIN ('||vSQL||') a;'||CHR(10)||
  '  :1 := SQL%ROWCOUNT;'||CHR(10)||
  '  COMMIT;'||CHR(10)||
  'END;'
  USING OUT vCou;

  dbms_lob.freetemporary(vSQL);

  vMes := 'SUCCESSFULLY :: '||vCou||' rows inserted into table '||LOWER(vOwner)||'.anltline_'||inGroupID||'#'||inAnltAlias;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate',vMes);

  vEndTime := SYSDATE;
  vMes := 'CONTINUE :: ------------ "'||to_char(inDate,'DD.MM.YYYY')||'" - Группа: "'||vGroupName||'" - Аналитика: "'||vAnltName||'" - окончание вставки данных. Время выполнения: '||get_ti_as_hms(vEndTime - vTIBegin)||' -----------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate',vMes);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate" finished successfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAnltOnDate',vMes);
END StarAnltOnDate;

PROCEDURE StarPrepare(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'PREPAREJOB_'||tb_signs_job_id_seq.nextval;
    vBuff VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vMes VARCHAR2(4000);
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.YYYY');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.YYYY');
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepare" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepare',vMes);

  vBuff :=
  q'[WITH
      dt AS (
        SELECT to_date(']'||vEndDate||q'[','DD.MM.YYYY') - LEVEL + 1 AS as_of_date
          FROM dual CONNECT BY LEVEL <= to_date(']'||vEndDate||q'[','DD.MM.YYYY') - to_date(']'||vBegDate||q'[','DD.MM.YYYY') + 1
        ORDER BY 1)
     ,a AS (
        SELECT group_id||'|'||head_entity_id AS ID
              ,group_id
              ,head_entity_id
          FROM (
            SELECT DISTINCT
                   g.group_id
                  ,CASE WHEN LEVEL = 1 THEN
                           (SELECT ID FROM tb_entity WHERE parent_id IS NULL
                            CONNECT BY ID = PRIOR parent_id START WITH ID = p.entity_id)
                         ELSE NULL END AS head_entity_id
              FROM tb_signs_group g
                   LEFT JOIN tb_signs_2_group s2g
                     ON s2g.group_id = g.group_id
                   LEFT JOIN tb_signs_pool p
                     ON p.sign_name = s2g.sign_name
                    WHERE s2g.sign_name IS NOT NULL AND LEVEL <= 2
            CONNECT BY PRIOR g.group_id = g.parent_group_id
            START WITH g.group_id = ]'||inGroupID||q'[))
     ,anlt as (
        SELECT DISTINCT id,anlt_code FROM (
          SELECT g.group_id||'|'||a.anlt_alias AS id
                ,a.anlt_code
            FROM tb_signs_group g
                 LEFT JOIN tb_anlt_2_group a2g
                   ON a2g.group_id = g.group_id
                 LEFT JOIN tb_signs_anlt a
                   ON a.anlt_code = a2g.anlt_code
                      AND to_date(']'||vEndDate||q'[','DD.MM.YYYY') BETWEEN a.effective_start AND a.effective_end
                      AND EXISTS(SELECT NULL FROM tb_signs_anlt_spec WHERE anlt_id = a.id)
                  WHERE a2g.anlt_code IS NOT NULL AND LEVEL = 3
          CONNECT BY PRIOR g.group_id = g.parent_group_id
          START WITH g.group_id = ]'||inGroupID||q'[) WHERE anlt_code IS NOT NULL
      )
      SELECT DISTINCT
             a.id||'|'||to_char(dt.as_of_date,'RRRRMMDD') AS ID
            ,NULL AS parent_id
            ,CASE WHEN a.head_entity_id IS NOT NULL THEN ']'||LOWER(vOwner)||q'[.pkg_etl_signs.StarPrepareDim' ELSE ']'||LOWER(vOwner)||q'[.pkg_etl_signs.StarPrepareFct' END AS unit
            ,to_char(dt.as_of_date,'DD.MM.YYYY')||'#!#'||a.group_id||CASE WHEN a.head_entity_id IS NOT NULL THEN '#!#'||a.head_entity_id END as params
            ,0 AS skip
        FROM dt CROSS JOIN a
      UNION ALL
      SELECT DISTINCT
             anlt.id||'|'||to_char(dt.as_of_date,'RRRRMMDD') AS ID
            ,NULL AS parent_id
            ,']'||LOWER(vOwner)||q'[.pkg_etl_signs.StarPrepareAnlt' AS unit
            ,to_char(dt.as_of_date,'DD.MM.YYYY')||'#!#'||]'||inGroupID||q'[||'#!#'||anlt.anlt_code AS params
            ,0 AS skip
        FROM dt CROSS JOIN anlt
  ]';
  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepare" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepare',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepare" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepare',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarPrepare" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarPrepare',vMes);
END StarPrepare;

PROCEDURE StarClear(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'CLEARJOB_'||tb_signs_job_id_seq.nextval;
    vBuff VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vMes VARCHAR2(4000);
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.YYYY');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.YYYY');
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarClear" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarClear',vMes);

  vBuff :=
  q'{WITH
      dt AS (
        SELECT to_date('}'||vEndDate||q'{','DD.MM.YYYY') - LEVEL + 1 AS as_of_date
          FROM dual CONNECT BY LEVEL <= to_date('}'||vEndDate||q'{','DD.MM.YYYY') - to_date('}'||vBegDate||q'{','DD.MM.YYYY') + 1
        ORDER BY 1)
SELECT to_char(dt,'DD.MM.RRRR')||'|'||tbl AS id
      ,NVL2(LAG(dt) OVER (PARTITION BY tbl ORDER BY dt),to_char(LAG(dt) OVER (PARTITION BY tbl ORDER BY dt),'DD.MM.RRRR')||'|'||tbl,NULL) AS parent_id
      ,'}'||LOWER(vOwner)||q'{.pkg_etl_signs.MyExecute' AS unit
      ,'ALTER TABLE }'||LOWER(vOwner)||q'{.'||tbl||' TRUNCATE PARTITION P'||to_char(dt,'RRRRMMDD') AS params
      ,0 AS SKIP
  FROM (
SELECT dt.as_of_date AS dt
      ,LOWER(t.table_name) AS tbl
  FROM dt CROSS JOIN TABLE(}'||LOWER(vOwner)||q'{.pkg_etl_signs.StarGetFldList(dt.as_of_date,}'||inGroupID||q'{)) t
GROUP BY dt.as_of_date,t.table_name)}';

  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarClear" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarClear',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarClear" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarClear',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarClear" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarClear',vMes);
END StarClear;

PROCEDURE StarAggrsLoadData(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER)
  IS
    vBegDate VARCHAR2(10) := to_char(inBegDate,'DD.MM.RRRR');
    vEndDate VARCHAR2(10) := to_char(inEndDate,'DD.MM.RRRR');
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'AGGRLOADDATAJOB_'||tb_signs_job_id_seq.nextval;
    vCou INTEGER := 0;
    vGroupName VARCHAR2(4000);
    vBuff VARCHAR2(32700) :=
    q'{WITH
        dt AS (
          SELECT to_date('}'||vBegDate||q'{','DD.MM.YYYY') + LEVEL - 1 AS as_of_date
            FROM dual CONNECT BY LEVEL <= to_date('}'||vEndDate||q'{','DD.MM.YYYY') - to_date('}'||vBegDate||q'{','DD.MM.YYYY') + 1
          ORDER BY 1)
  SELECT to_char(dt.as_of_date,'DD.MM.RRRR')||'|'||aggr.id AS id
        ,NULL AS parent_id
        ,'}'||LOWER(vOwner)||q'{.pkg_etl_signs.StarAggrOnDate' AS unit
        ,to_char(dt.as_of_date,'DD.MM.RRRR')||'#!#'||aggr.id AS params
        ,0 AS SKIP
    FROM dt
         INNER JOIN }'||LOWER(vOwner)||q'{.tb_signs_aggrs aggr
           ON aggr.group_id = }'||inGroupID||q'{
  GROUP BY dt.as_of_date,aggr.id}';

    vMes VARCHAR2(4000);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsLoadData" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsLoadData',vMes);
  
  SELECT COUNT(1) INTO vCou FROM tb_signs_aggrs WHERE group_id = inGroupID;
  SELECT group_name INTO vGroupName FROM tb_signs_group WHERE group_id = inGroupID;

  IF vCou > 0 THEN
    load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
  ELSE
    vMes := 'INFORMATION :: У группы "'||vGroupName||'" отсутствуют агрегаты. Пересчет не требуется';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsLoadData',vMes);
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsLoadData" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsLoadData',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsLoadData" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsLoadData',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsLoadData" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsLoadData',vMes);
END StarAggrsLoadData;

PROCEDURE StarExpand(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER,inMask VARCHAR2 DEFAULT '00',inDaemonId NUMBER DEFAULT NULL,inParallelJobs NUMBER DEFAULT 30)
  /************************************
   Описание маски (0 - не выполнять, 1 - выполнять):
   1-й символ - предварительный пересчет всех показателей по кубу
   2-й символ - предварительный пересчет всех аналитик по кубу
  ************************************/
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'EXPANDJOB_'||tb_signs_job_id_seq.nextval;
    vBuff VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vMes VARCHAR2(4000);
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.RRRR');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.RRRR');
    vFctGroupID NUMBER;
    vGroupName VARCHAR2(4000);
    --
    vDoSign BOOLEAN := SUBSTR(inMask,1,1) = '1';
    vDoAnlt BOOLEAN := SUBSTR(inMask,2,1) = '1';
    -- для централизованного логирования
    logOPTP NUMBER;
    logOPNM VARCHAR2(256);
    logCOMMENT VARCHAR2(4000);
    logErr VARCHAR2(32700);
    l_opid NUMBER;
    l_stid NUMBER;
    autocalc BOOLEAN := inDaemonId IS NOT NULL;
    --err NUMBER := 1;
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarExpand" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarExpand',vMes);
  
  SELECT group_id INTO vFctGroupID FROM tb_signs_group WHERE parent_group_id = inGroupID;
  SELECT group_name INTO vGroupName FROM tb_signs_group WHERE group_id = inGroupID;
  -- Если осуществлен автоматический запуск в рамках общего пула расчетов, делаем централизованное логирование
  IF autocalc THEN
    -- Получаем ИД типовой операции и заодно наименование группы, а так же ИД группы измерений и ИД группы фактов
    BEGIN
      SELECT NAME
            ,'DashBoard '||NAME||' (group_id = '||inGroupID||')' AS g_id
        INTO logOPNM
            ,logCOMMENT
        FROM tb_signs_daemons WHERE id = inDaemonId;
        
      logOPTP := LG_PKG.CreateTypeOper(logOPNM,logCOMMENT);
    EXCEPTION WHEN OTHERS THEN
      logErr := SQLERRM;
      SELECT to_number(SUBSTR(logErr,INSTR(logErr,' OPTP = ') + 8,INSTR(logErr,' c именем ') - INSTR(logErr,'OPTP = ') - 7)) INTO logOPTP FROM dual;
    END;
    -- Создаем новое логирование
    vMes := 'BEGIN OF LOGGING :: OPTP = '||logOPTP;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarExpand',vMes);
    
    l_opid := LG_PKG.StartOper(logOPTP,sysdate); -- Генерируем запись об операции
    LG_PKG.setparam(l_opid,'inBegDate',inBegDate,'D'); 
    LG_PKG.setparam(l_opid,'inEndDate',inEndDate,'D'); 
    LG_PKG.setparam(l_opid,'inGroupID',inGroupID,'N');
    LG_PKG.setparam(l_opid,'inFctGroupID',vFctGroupID,'N'); 
    LG_PKG.setparam(l_opid,'inMask',inMask,'S'); 
    LG_PKG.setparam(l_opid,'inDaemonId',inDaemonId,'N');
    LG_PKG.setparam(l_opid,'inGroupName',vGroupName,'S');
  END IF;
  
  -- Если требуется предварительный пересчет показателей
  IF vDoSign THEN
    -- если авторасчет - центральное логирование
    IF autocalc THEN
      -- фаза "пересчет показателей"
      l_stid := LG_PKG.RegPhase(l_opid,'ПРЕДВАРИТЕЛЬНЫЙ ПЕРЕСЧЕТ ПОКАЗАТЕЛЕЙ');
      LG_PKG.setparam(l_opid,'inBegDate',inBegDate,'D',l_stid);
      LG_PKG.setparam(l_opid,'inEndDate',inEndDate,'D',l_stid);
      LG_PKG.setparam(l_opid,'inGroupID',inGroupID,'N',l_stid);
      LG_PKG.setparam(l_opid,'inJobName',REPLACE(vJobName,'EXPANDJOB','SIGNSBYSTARJOB'),'S'); 
      LG_PKG.setparam(l_opid,'inDaemonId',inDaemonId,'N'); 
    END IF;  
    
    CalcSignsByStar(inBegDate,inEndDate,inGroupID,REPLACE(vJobName,'EXPANDJOB','SIGNSBYSTARJOB'));
    
    IF autocalc THEN
      LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 01.ПРЕДВАРИТЕЛЬНЫЙ ПЕРЕСЧЕТ ПОКАЗАТЕЛЕЙ :: OPID = ' ||l_opid);
      LG_PKG.ENDPHASE(l_stid);
    END IF;
  END IF;

  -- Если требуется предварительный пересчет аналитик
  IF vDoAnlt THEN
    -- если авторасчет - центральное логирование
    IF autocalc THEN
      -- фаза "пересчет аналитик"
      l_stid := LG_PKG.RegPhase(l_opid,'ПРЕДВАРИТЕЛЬНЫЙ ПЕРЕСЧЕТ АНАЛИТИК');
      LG_PKG.setparam(l_opid,'inBegDate',inBegDate,'D',l_stid);
      LG_PKG.setparam(l_opid,'inEndDate',inEndDate,'D',l_stid);
      LG_PKG.setparam(l_opid,'inGroupID',inGroupID,'N',l_stid);
      LG_PKG.setparam(l_opid,'inJobName',REPLACE(vJobName,'EXPANDJOB','ANLTBYSTARJOB'),'S'); 
      LG_PKG.setparam(l_opid,'inDaemonId',inDaemonId,'N'); 
    END IF;  
    
    CalcAnltByStar(inBegDate,inEndDate,inGroupID,REPLACE(vJobName,'EXPANDJOB','ANLTBYSTARJOB'),inParallelJobs);
    
    IF autocalc THEN
      LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 02.ПРЕДВАРИТЕЛЬНЫЙ ПЕРЕСЧЕТ АНАЛИТИК :: OPID = ' ||l_opid);
      LG_PKG.ENDPHASE(l_stid);
    END IF;
  END IF;

  -- Подготовка
  -- если авторасчет - центральное логирование
  IF autocalc THEN
    l_stid := LG_PKG.RegPhase(l_opid,'ПОДГОТОВКА ТАБЛИЦ ЗВЕЗДЫ');
    LG_PKG.setparam(l_opid,'inBegDate',inBegDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inEndDate',inEndDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inGroupID',inGroupID,'N',l_stid);
  END IF;  
    
  StarPrepare(inBegDate,inEndDate,inGroupID);
  StarPrepareAggrs(inBegDate,inEndDate,inGroupID);
  
  IF autocalc THEN
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 03.ПОДГОТОВКА ТАБЛИЦ ЗВЕЗДЫ :: OPID = ' ||l_opid);
    LG_PKG.ENDPHASE(l_stid);
  END IF;

  -- Очистка
  -- если авторасчет - центральное логирование
  IF autocalc THEN
    l_stid := LG_PKG.RegPhase(l_opid,'ОЧИСТКА ЗВЕЗДЫ');
    LG_PKG.setparam(l_opid,'inBegDate',inBegDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inEndDate',inEndDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inGroupID',inGroupID,'N',l_stid);
  END IF;  
    
  StarClear(inBegDate,inEndDate,inGroupID);

  IF autocalc THEN
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 04.ОЧИСТКА ЗВЕЗДЫ :: OPID = ' ||l_opid);
    LG_PKG.ENDPHASE(l_stid);
  END IF;

  -- Загрузка детального слоя звезды
  vBuff :=
    q'[WITH
      dt AS (
        SELECT to_date(']'||vEndDate||q'[','DD.MM.YYYY') - LEVEL + 1 AS as_of_date
          FROM dual CONNECT BY LEVEL <= to_date(']'||vEndDate||q'[','DD.MM.YYYY') - to_date(']'||vBegDate||q'[','DD.MM.YYYY') + 1
        ORDER BY 1)
     ,a AS (
        SELECT DISTINCT
               to_char(g.group_id) AS ID
              ,NULL AS parent_id
              ,(SELECT ID FROM tb_entity WHERE parent_id IS NULL
                CONNECT BY ID = PRIOR parent_id START WITH ID = p.entity_id) AS head_entity_id
              ,CASE WHEN LEVEL = 1 THEN ']'||LOWER(vOwner)||q'[.pkg_etl_signs.StarDimOnDate' ELSE ']'||LOWER(vOwner)||q'[.pkg_etl_signs.StarFctOnDateSign' END AS unit
              ,CASE WHEN LEVEL = 1 THEN 'dim' ELSE 'fct' END AS StarPart
              ,CASE WHEN LEVEL = 1 THEN NULL ELSE s2g.sign_name END AS sign_name
          FROM tb_signs_group g
               LEFT JOIN tb_signs_2_group s2g
                 ON s2g.group_id = g.group_id
               LEFT JOIN tb_signs_pool p
                 ON p.sign_name = s2g.sign_name
          WHERE s2g.sign_name IS NOT NULL AND LEVEL <= 2
        CONNECT BY PRIOR g.group_id = g.parent_group_id
        START WITH g.group_id = ]'||inGroupID||q'[)
     ,b AS (
        SELECT DISTINCT
               dt.as_of_date
              ,a.anlt_alias
          FROM tb_signs_group g CROSS JOIN dt
               LEFT JOIN tb_anlt_2_group a2g
                 ON a2g.group_id = g.group_id
               LEFT JOIN tb_signs_anlt a
                 ON a.anlt_code = a2g.anlt_code
                    AND dt.as_of_date BETWEEN a.effective_start AND a.effective_end
          WHERE LEVEL = 3 AND EXISTS (SELECT NULL FROM tb_signs_anlt_spec WHERE anlt_id = a.id)
        CONNECT BY PRIOR g.group_id = g.parent_group_id
        START WITH g.group_id = ]'||inGroupID||q'[)
      SELECT to_char(ROWNUM) AS id
            ,CASE WHEN ROWNUM BETWEEN 1 AND ]'||inParallelJobs||q'[ THEN NULL ELSE to_char(ROWNUM - ]'||inParallelJobs||q'[) END AS parent_id
            ,unit
            ,params
            ,skip
        FROM (
       -- Факты и ПИДАРЫ (ПИДАР - Простое Измерение Для Агрегирования Результатов)
      SELECT DISTINCT
             /*to_char(dt.as_of_date,'DD.MM.YYYY')||'#]'||LOWER(vOwner)||q'[.'||a.StarPart||'_'||a.id||'#'||CASE WHEN a.StarPart = 'fct' THEN a.sign_name ELSE to_char(a.head_entity_id) END AS ID
            ,*/NULL AS parent_id
            ,a.unit
            ,to_char(dt.as_of_date,'DD.MM.YYYY')||'#!#'||a.id||'#!#'||CASE WHEN a.StarPart = 'fct' THEN a.sign_name ELSE to_char(a.head_entity_id) END AS params
            ,0 AS skip
        FROM dt CROSS JOIN a
      -- СУКИ (СУКА - Сквозная Унифицированная Комплексная Аналитика)
      UNION ALL
      SELECT DISTINCT
             /*to_char(b.as_of_date,'DD.MM.YYYY')||'#]'||LOWER(vOwner)||q'[.anltline_]'||inGroupID||q'[#'||b.anlt_alias AS ID
            ,*/NULL AS parent_id
            ,']'||LOWER(vOwner)||q'[.pkg_etl_signs.StarAnltOnDate'
            ,to_char(b.as_of_date,'DD.MM.YYYY')||'#!#]'||inGroupID||q'[#!#'||b.anlt_alias AS params
            ,0 AS skip
        FROM b)]';

  -- если авторасчет - центральное логирование
  IF autocalc THEN
    l_stid := LG_PKG.RegPhase(l_opid,'ЗАГРУЗКА ДАННЫХ ЗВЕЗДЫ');
    LG_PKG.setparam(l_opid,'vBuff','CLOB','S',l_stid);
    LG_PKG.setparam(l_opid,'vJobName',vJobName,'S',l_stid);
    LG_PKG.setparam(l_opid,'inDaemonId',inDaemonId,'N'); 
  END IF;  
    
  load_new(vBuff,vJobName);
  
  IF autocalc THEN
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 05.ЗАГРУЗКА ДАННЫХ ЗВЕЗДЫ :: OPID = ' ||l_opid);
    LG_PKG.ENDPHASE(l_stid);
  END IF;

  -- Сжатие данных звезды
  -- если авторасчет - центральное логирование
  IF autocalc THEN
    l_stid := LG_PKG.RegPhase(l_opid,'СЖАТИЕ ДАННЫХ ЗВЕЗДЫ');
    LG_PKG.setparam(l_opid,'inBegDate',inBegDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inEndDate',inEndDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inGroupID',inGroupID,'N',l_stid);
  END IF;  
    
  StarCompress(inBegDate,inEndDate,inGroupID);

  IF autocalc THEN
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 06.СЖАТИЕ ДАННЫХ ЗВЕЗДЫ:: OPID = ' ||l_opid);
    LG_PKG.ENDPHASE(l_stid);
  END IF;
  
  -- Сбор статистики по таблицам звезды
  -- если авторасчет - центральное логирование
  IF autocalc THEN
    l_stid := LG_PKG.RegPhase(l_opid,'СБОР СТАТИСТИКИ ПО ТАБЛИЦАМ ЗВЕЗДЫ');
    LG_PKG.setparam(l_opid,'inBegDate',inBegDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inEndDate',inEndDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inGroupID',inGroupID,'N',l_stid);
  END IF;  
    
  StarGatherStats(inBegDate,inEndDate,inGroupID);

  IF autocalc THEN
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 07.СБОР СТАТИСТИКИ ПО ТАБЛИЦАМ ЗВЕЗДЫ:: OPID = ' ||l_opid);
    LG_PKG.ENDPHASE(l_stid);
  END IF;

  -- Загрузка предагрегатов
  -- если авторасчет - центральное логирование
  IF autocalc THEN
    l_stid := LG_PKG.RegPhase(l_opid,'ЗАГРУЗКА ПРЕДАГРЕГАТОВ');
    LG_PKG.setparam(l_opid,'inBegDate',inBegDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inEndDate',inEndDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inGroupID',inGroupID,'N',l_stid);
  END IF;  
    
  StarAggrsLoadData(inBegDate,inEndDate,inGroupID);
  
  IF autocalc THEN
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 08. ЗАГРУЗКА ПРЕДАГРЕГАТОВ :: OPID = ' ||l_opid);
    LG_PKG.ENDPHASE(l_stid);
  END IF;
  
  -- Сжатие предагрегатов
  -- если авторасчет - центральное логирование
  IF autocalc THEN
    l_stid := LG_PKG.RegPhase(l_opid,'СЖАТИЕ ПРЕДАГРЕГАТОВ');
    LG_PKG.setparam(l_opid,'inBegDate',inBegDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inEndDate',inEndDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inGroupID',inGroupID,'N',l_stid);
  END IF;  
    
  StarAggrsCompress(inBegDate,inEndDate,inGroupID);
  
  IF autocalc THEN
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 09.СЖАТИЕ ПРЕДАГРЕГАТОВ :: OPID = ' ||l_opid);
    LG_PKG.ENDPHASE(l_stid);
  END IF;


  -- Сбор статистики по таблицам - предагрегатам
  -- если авторасчет - центральное логирование
  IF autocalc THEN
    l_stid := LG_PKG.RegPhase(l_opid,'СБОР СТАТИСТИКИ ПО ТАБЛИЦАМ - ПРЕДАГРЕГАТАМ');
    LG_PKG.setparam(l_opid,'inBegDate',inBegDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inEndDate',inEndDate,'D',l_stid);
    LG_PKG.setparam(l_opid,'inGroupID',inGroupID,'N',l_stid);
  END IF;  
    
  StarAggrsGatherStats(inBegDate,inEndDate,inGroupID);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarFixEmptyComments',StarFixEmptyComments(inGroupID));
  
  IF autocalc THEN
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 10.СБОР СТАТИСТИКИ ПО ТАБЛИЦАМ - ПРЕДАГРЕГАТАМ :: OPID = ' ||l_opid);
    LG_PKG.ENDPHASE(l_stid);

    LG_PKG.EndOper(l_opid);
    vMes := 'END OF LOGGING :: OPTP = '||logOPTP;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarExpand',vMes);
    IF autocalc THEN SendMainLogs(l_opid); END IF;
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarExpand" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarExpand',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarExpand" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarExpand',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarExpand" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarExpand',vMes);
  IF autocalc THEN SendMainLogs(l_opid); END IF;
END StarExpand;

PROCEDURE StarCompress(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'COMPRESSJOB_'||tb_signs_job_id_seq.nextval;
    vBuff VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vMes VARCHAR2(4000);
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.YYYY');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.YYYY');
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarCompress" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarCompress',vMes);

  vBuff :=
    q'[WITH
      dt AS (
        SELECT to_date(']'||vEndDate||q'[','DD.MM.YYYY') - LEVEL + 1 AS as_of_date
          FROM dual CONNECT BY LEVEL <= to_date(']'||vEndDate||q'[','DD.MM.YYYY') - to_date(']'||vBegDate||q'[','DD.MM.YYYY') + 1
        ORDER BY 1)
     ,g AS (
        SELECT group_id,LEVEL AS lev FROM tb_signs_group WHERE LEVEL <= 2
        CONNECT BY PRIOR group_id = parent_group_id START WITH group_id = ]'||inGroupID||q'[
      )
     ,a AS (
        SELECT DISTINCT dt.as_of_date,aa.id,aa.parent_id,aa.head_entity_id,aa.unit,aa.StarPart,aa.sp_code FROM (
        SELECT to_char(g.group_id) AS ID
              ,NULL AS parent_id
              ,CASE WHEN g.lev = 1 THEN
                 (SELECT ID FROM tb_entity WHERE parent_id IS NULL
                  CONNECT BY ID = PRIOR parent_id START WITH ID = p.entity_id)
               ELSE NULL END AS head_entity_id
              ,']'||LOWER(vOwner)||q'[.pkg_etl_signs.MyExecute' AS unit
              ,CASE WHEN g.lev = 1 THEN 'dim' ELSE 'fct' END AS StarPart
              ,'SP'||ora_hash(NVL(s2g.sgn_alias,s2g.sign_name)) AS sp_code
              ,p.condition
              ,s2g.sign_name
          FROM g
               INNER JOIN tb_signs_2_group s2g
                 ON s2g.group_id = g.group_id
               INNER JOIN tb_signs_pool p
                 ON p.sign_name = s2g.sign_name
          WHERE s2g.sign_name IS NOT NULL) aa INNER JOIN dt ON ]'||LOWER(vOwner)||q'[.pkg_etl_signs.GetConditionResult(aa.condition,'INBEGDATE DATE '||to_char(dt.as_of_date,'DD.MM.RRRR'),aa.sign_name) = 1)
     ,anlt as (
        SELECT DISTINCT id,anlt_alias FROM (
          SELECT g.group_id||'|'||a.anlt_alias AS id
                ,a.anlt_alias
            FROM tb_signs_group g
                 LEFT JOIN tb_anlt_2_group a2g
                   ON a2g.group_id = g.group_id
                 LEFT JOIN tb_signs_anlt a
                   ON a.anlt_code = a2g.anlt_code
                      AND to_date(']'||vEndDate||q'[','DD.MM.YYYY') BETWEEN a.effective_start AND a.effective_end
                      AND EXISTS(SELECT NULL FROM tb_signs_anlt_spec WHERE anlt_id = a.id)
                  WHERE a2g.anlt_code IS NOT NULL AND LEVEL = 3
          CONNECT BY PRIOR g.group_id = g.parent_group_id
          START WITH g.group_id = ]'||inGroupID||q'[) WHERE anlt_alias IS NOT NULL
      )
      SELECT DISTINCT a.id||'_'||a.head_entity_id||'_P'||to_char(a.as_of_date,'RRRRMMDD')||CASE WHEN a.head_entity_id IS NULL THEN '_'||a.sp_code END AS ID
            ,NULL AS parent_id
            ,a.unit
            ,'ALTER TABLE ]'||LOWER(vOwner)||q'[.'||CASE WHEN a.head_entity_id IS NULL THEN 'fct_' ELSE 'dim_' END||a.id||CASE WHEN a.head_entity_id IS NULL THEN NULL ELSE '#'||a.head_entity_id END||CHR(10)||
            '   MOVE '||CASE WHEN a.head_entity_id IS NULL THEN 'SUBPARTITION '||a.sp_code||'_'||to_char(a.as_of_date,'RRRRMMDD') ELSE 'PARTITION P'||to_char(a.as_of_date,'RRRRMMDD') END||' COMPRESS' AS params
            ,0 AS skip
        FROM a
      UNION ALL
      SELECT DISTINCT
             anlt.id||'|'||to_char(dt.as_of_date,'RRRRMMDD') AS ID
            ,NULL AS parent_id
            ,']'||LOWER(vOwner)||q'[.pkg_etl_signs.MyExecute' AS unit
            ,'ALTER TABLE ]'||LOWER(vOwner)||q'[.ANLTLINE_]'||inGroupID||q'[#'||anlt.anlt_alias||' MOVE PARTITION P'||to_char(dt.as_of_date,'RRRRMMDD')||' COMPRESS' AS params
            ,0 AS skip
        FROM dt CROSS JOIN anlt
  ]';

  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarCompress" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarCompress',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarCompress" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarCompress',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarCompress" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarCompress',vMes);
END StarCompress;

PROCEDURE StarAggrsCompress(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'AGGRCOMPRESSJOB_'||tb_signs_job_id_seq.nextval;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vCou INTEGER := 0;
    vGroupName VARCHAR2(4000);
    vMes VARCHAR2(4000);
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.YYYY');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.YYYY');
    vBuff VARCHAR2(32700) :=
    q'{WITH
        dt AS (
          SELECT to_date('}'||vEndDate||q'{','DD.MM.YYYY') - LEVEL + 1 AS as_of_date
            FROM dual CONNECT BY LEVEL <= to_date('}'||vEndDate||q'{','DD.MM.YYYY') - to_date('}'||vBegDate||q'{','DD.MM.YYYY') + 1
          ORDER BY 1)
  SELECT to_char(dt,'DD.MM.RRRR')||'|'||tbl AS id
        ,NVL2(LAG(dt) OVER (PARTITION BY tbl ORDER BY dt),to_char(LAG(dt) OVER (PARTITION BY tbl ORDER BY dt),'DD.MM.RRRR')||'|'||tbl,NULL) AS parent_id
        ,'}'||LOWER(vOwner)||q'{.pkg_etl_signs.MyExecute' AS unit
        ,'ALTER TABLE }'||LOWER(vOwner)||q'{.'||tbl||' MOVE PARTITION P'||to_char(dt,'RRRRMMDD')||' COMPRESS' AS params
        ,0 AS SKIP
    FROM (
  SELECT dt.as_of_date AS dt
        ,LOWER(t.table_name) AS tbl
    FROM dt
         INNER JOIN TABLE(}'||LOWER(vOwner)||q'{.pkg_etl_signs.StarGetFldList(dt.as_of_date,}'||inGroupID||q'{)) t
           ON t.table_name LIKE 'AGGR%'
  GROUP BY dt.as_of_date,t.table_name)}';
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsCompress" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsCompress',vMes);

  SELECT COUNT(1) INTO vCou FROM tb_signs_aggrs WHERE group_id = inGroupID;
  SELECT group_name INTO vGroupName FROM tb_signs_group WHERE group_id = inGroupID;

  IF vCou > 0 THEN
    load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
  ELSE
    vMes := 'INFORMATION :: У группы "'||vGroupName||'" отсутствуют агрегаты. Сжатие не требуется';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsCompress',vMes);
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsCompress" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsCompress',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsCompress" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsCompress',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsCompress" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsCompress',vMes);
END StarAggrsCompress;

PROCEDURE StarGatherStats(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'GATHERSTATSJOB_'||tb_signs_job_id_seq.nextval;
    vBuff VARCHAR2(32700);
    vFctGroupID NUMBER;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vMes VARCHAR2(4000);
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.YYYY');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.YYYY');
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarGatherStats" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarGatherStats',vMes);
  
  SELECT group_id INTO vFctGroupID FROM tb_signs_group WHERE parent_group_id = inGroupID;
  
  vBuff :=
  'WITH
     dt AS (
       SELECT to_date('''||vEndDate||''',''DD.MM.YYYY'') - LEVEL + 1 AS as_of_date
         FROM dual CONNECT BY LEVEL <= to_date('''||vEndDate||''',''DD.MM.YYYY'') - to_date('''||vBegDate||''',''DD.MM.YYYY'') + 1
       ORDER BY 1)
    ,tbl AS (
      SELECT /*+ materialize */
             table_name
        FROM (
          SELECT table_name
            FROM TABLE('||LOWER(vOwner)||'.pkg_etl_signs.StarGetFldList(to_date('''||vEndDate||''',''DD.MM.RRRR''),'||inGroupID||'))
            WHERE NOT(table_name LIKE ''AGGR%'')
          GROUP BY table_name
        )
      )
      SELECT to_char(dt.as_of_date,''DD.MM.RRRR'')||''|''||t.table_name||''|''||p.partition_name AS ID
            ,NULL AS parent_id
            ,'''||LOWER(vOwner)||'.pkg_etl_signs.MyExecute'' AS unit
            ,''BEGIN dbms_stats.gather_table_stats(ownname=>''''''||t.owner||'''''',tabname=>''''''||t.table_name||''''''''||CASE WHEN t.partitioned = ''YES'' THEN '',partname=>''''''||p.partition_name||'''''''' END||CASE WHEN t.partitioned = ''YES'' THEN '',granularity=>''''''||CASE WHEN p.subpartition_count > 0 THEN ''SUBPARTITION'' ELSE ''PARTITION'' END||'''''''' END||''); END;'' AS params
            ,0 AS SKIP
        FROM tbl 
             INNER JOIN all_tables t
               ON t.owner = '''||UPPER(vOwner)||'''
                  AND t.table_name = tbl.table_name
             CROSS JOIN dt
             LEFT JOIN all_tab_partitions p
               ON p.table_owner = t.owner
                  AND p.table_name = t.table_name
                  AND p.partition_name = ''P''||to_char(dt.as_of_date,''RRRRMMDD'')';
  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarGatherStats" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarsGatherStats',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarGatherStats" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarGatherStats',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarGatherStats" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarGatherStats',vMes);
END StarGatherStats;

PROCEDURE StarAggrsGatherStats(inBegDate IN DATE,inEndDate IN DATE,inGroupID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'AGGRGATHERSTATSJOB_'||tb_signs_job_id_seq.nextval;
    vFctGroupID NUMBER;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vCou INTEGER := 0;
    vGroupName VARCHAR2(4000);
    vMes VARCHAR2(4000);
    vBegDate VARCHAR2(30) := to_char(inBegDate,'DD.MM.YYYY');
    vEndDate VARCHAR2(30) := to_char(inEndDate,'DD.MM.YYYY');
    vBuff VARCHAR2(32700) :=
    'WITH
       dt AS (
         SELECT to_date('''||vEndDate||''',''DD.MM.YYYY'') - LEVEL + 1 AS as_of_date
           FROM dual CONNECT BY LEVEL <= to_date('''||vEndDate||''',''DD.MM.YYYY'') - to_date('''||vBegDate||''',''DD.MM.YYYY'') + 1
         ORDER BY 1)
      ,tbl AS (
        SELECT /*+ materialize */
               table_name
          FROM (
            SELECT table_name
              FROM TABLE('||LOWER(vOwner)||'.pkg_etl_signs.StarGetFldList(to_date('''||vEndDate||''',''DD.MM.RRRR''),'||inGroupID||'))
              WHERE table_name LIKE ''AGGR%''
            GROUP BY table_name
          )
        )
        SELECT to_char(dt.as_of_date,''DD.MM.RRRR'')||''|''||t.table_name||''|''||p.partition_name AS ID
              ,NULL AS parent_id
              ,'''||LOWER(vOwner)||'.pkg_etl_signs.MyExecute'' AS unit
              ,''BEGIN dbms_stats.gather_table_stats(ownname=>''''''||t.owner||'''''',tabname=>''''''||t.table_name||''''''''||CASE WHEN t.partitioned = ''YES'' THEN '',partname=>''''''||p.partition_name||'''''''' END||CASE WHEN t.partitioned = ''YES'' THEN '',granularity=>''''''||CASE WHEN p.subpartition_count > 0 THEN ''SUBPARTITION'' ELSE ''PARTITION'' END||'''''''' END||''); END;'' AS params
              ,0 AS SKIP
          FROM tbl 
               INNER JOIN all_tables t
                 ON t.owner = '''||UPPER(vOwner)||'''
                    AND t.table_name = tbl.table_name
               CROSS JOIN dt
               LEFT JOIN all_tab_partitions p
                 ON p.table_owner = t.owner
                    AND p.table_name = t.table_name
                    AND p.partition_name = ''P''||to_char(dt.as_of_date,''RRRRMMDD'')';
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsGatherStats" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsGatherStats',vMes);
  
  SELECT group_id INTO vFctGroupID FROM tb_signs_group WHERE parent_group_id = inGroupID;
  
  SELECT COUNT(1) INTO vCou FROM tb_signs_aggrs WHERE group_id = inGroupID;
  SELECT group_name INTO vGroupName FROM tb_signs_group WHERE group_id = inGroupID;

  IF vCou > 0 THEN
    load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
  ELSE
    vMes := 'INFORMATION :: У группы "'||vGroupName||'" отсутствуют агрегаты. Сбор статистики не требуется';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsGatherStats',vMes);
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsGatherStats" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsGatherStats',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsGatherStats" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsGatherStats',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrsGatherStats" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrsGatherStats',vMes);
END StarAggrsGatherStats;

PROCEDURE StarDropOldParts(inDate IN DATE,inGroupID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'DROPPARTSJOB_'||tb_signs_job_id_seq.nextval;
    vCou INTEGER := 0;
    vBuff VARCHAR2(32700);
    --
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vMes VARCHAR2(4000);
BEGIN
  vMes := 'START :: "'||to_char(inDate,'DD.MM.RRRR')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarDropOldParts" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDropOldParts',vMes);

  vBuff :=
  q'{WITH
      g AS (
        SELECT CASE WHEN strg_period_type = 'D' THEN to_date('}'||to_char(inDate,'DD.MM.RRRR')||q'{','DD.MM.RRRR') - strg_period
                 ELSE add_months(to_date('}'||to_char(inDate,'DD.MM.RRRR')||q'{','DD.MM.RRRR'),-strg_period) END AS dt
          FROM tb_signs_group
          WHERE group_id = }'||inGroupID||q'{
      )
     ,tbl AS (
        SELECT /*+ materialize */
               '}'||LOWER(vOwner)||q'{.'||t.table_name AS table_name
              ,tp.partition_name
              ,to_date(SUBSTR(tp.partition_name,-8),'RRRRMMDD') AS dt
          FROM TABLE(}'||LOWER(vOwner)||q'{.pkg_etl_signs.StarGetFldList(to_date('}'||to_char(inDate,'DD.MM.RRRR')||q'{','DD.MM.RRRR'),}'||inGroupID||q'{)) t
               INNER JOIN all_tab_partitions tp
                 ON tp.table_name = UPPER(t.table_name)
        GROUP BY t.table_name,tp.partition_name
      )
      SELECT tbl.table_name||'|'||tbl.partition_name AS ID
            ,NULL AS parent_id
            ,'}'||LOWER(vOwner)||q'{.pkg_etl_signs.MyExecute' AS unit
            ,'ALTER TABLE '||tbl.table_name||' DROP PARTITION '||tbl.partition_name AS params
            ,0 AS skip
      FROM tbl
           INNER JOIN g
             ON tbl.dt BETWEEN to_date('01.01.1900','DD.MM.RRRR') + 1 AND g.dt}';
  
  EXECUTE IMMEDIATE 'DECLARE vCou INTEGER; BEGIN SELECT COUNT(1) INTO vCou FROM ('||vBuff||'); :1 := vCou; END;' USING OUT vCou;
  
  IF vCou > 0
    THEN load_new(vBuff,vJobName);
         --dbms_output.put_line(vDDL);
    ELSE pr_log_write(lower(vOwner)||'.pkg_Etl_signs.StarDropOldParts','INFORMATION :: "'||to_char(inDate,'DD.MM.RRRR')||'" - Для звезды с номером группы '||inGroupID||' не обнаружено сегментов старше периода хранения');
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.RRRR')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarDropOldParts" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDropOldParts',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.RRRR')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarDropOldParts" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDropOldParts',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.RRRR')||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarDropOldParts" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarDropOldParts',vMes);
END StarDropOldParts;

FUNCTION StarFixEmptyComments(inGroupID IN NUMBER) RETURN VARCHAR2
  IS
    vBuff VARCHAR2(32700);
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vCur SYS_REFCURSOR;
    vCol VARCHAR2(256);
    vComm VARCHAR2(4000);
    vCou INTEGER := 0;
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vBuff := q'{SELECT table_name||'.'||tbl.col_name AS col_name
                    ,tbl.col_comment
                FROM TABLE(}'||vOwner||q'{.pkg_etl_signs.StarGetFldList(TRUNC(SYSDATE,'DD'),}'||inGroupID||q'{)) tbl
                     LEFT JOIN all_col_comments tc
                       ON tc.owner = '}'||UPPER(vOwner)||q'{' AND tc.table_name = tbl.table_name AND tc.column_name = tbl.col_name
                WHERE tc.comments IS  NULL}';
  
  --dbms_output.put_line(vBuff);
  
  OPEN vCur FOR vBuff;
  LOOP
    FETCH vCur INTO vCol,vComm;
    EXIT WHEN vCur%NOTFOUND;
    BEGIN
      EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||UPPER(vOwner)||'.'||vCol||' IS '''||vComm||'''';
      --dbms_output.put_line('COMMENT ON COLUMN '||UPPER(vOwner)||'.'||vCol||' IS '''||vComm||'''');
      vCou := vCou + 1;
    EXCEPTION WHEN OTHERS THEN
      pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.StarFixEmptyComments','ERROR :: column_name = "'||vCol||'" :: '||SQLERRM);
    END;
  END LOOP;
  CLOSE vCur;

  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: Группа: ИД = '||inGroupID||' :: '||vCou||' comments added in '||get_ti_as_hms(vEndTime - vBegTime);
  RETURN(vMes);
EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarFixEmptyComments" :: '||SQLERRM;
  RETURN(vMes);
END StarFixEmptyComments;

FUNCTION StarFieldCommentsAsHTML(inGroupID IN NUMBER) RETURN CLOB
  IS
    vSQL VARCHAR2(32700);
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    --
    vBuff VARCHAR2(32700);
    vCLOB CLOB;
BEGIN
  dbms_lob.createtemporary(vCLOB,FALSE);
  FOR idx IN (
    SELECT table_name,table_comment FROM TABLE(StarGetFldList(TRUNC(SYSDATE,'DD'),inGroupID)) GROUP BY table_name,table_comment
  ) LOOP
    vSQL := 
    q'{SELECT table_name||'.'||col_name AS column_name
              ,col_type AS col_types
              ,NVL(comments,col_comment) AS comments
          FROM TABLE(}'||LOWER(vOwner)||q'{.pkg_etl_signs.StarGetFldList(TRUNC(SYSDATE,'DD'),}'||inGroupID||q'{)) tbl
               LEFT JOIN all_col_comments cc
                 ON cc.owner = '}'||vOwner||q'{'
                    AND cc.table_name = tbl.table_name
                    AND cc.column_name = tbl.col_name
         WHERE table_name = '}'||idx.table_name||'''';

    --dbms_output.put_line(vSQL);
    vBuff := SQLasHTML(vSQL,'COLUMN_NAME#!#COL_TYPES#!#COMMENTS','Наименование колонки:#!#Тип данных:#!#Описание колонки:','<br/><br/>',NULL,UPPER(vOwner)||'.'||idx.table_name||'<br/>'||idx.table_comment);
    
    dbms_lob.writeappend(vCLOB,LENGTH(vBuff),vBuff);
  END LOOP;

  RETURN(vCLOB);
EXCEPTION WHEN OTHERS THEN
  vCLOB := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarFixEmptyComments" :: '||SQLERRM;
  RETURN(vCLOB);
END StarFieldCommentsAsHTML;

PROCEDURE StarAggrRecalcOnDate(inDate IN DATE, inAggrID IN NUMBER)
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vAggrName VARCHAR2(4000);
    vGroupID NUMBER;
    vAggrTableName VARCHAR2(256);
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    --
    errNotFound EXCEPTION;
BEGIN
  BEGIN
    SELECT aggr_name,group_id INTO vAggrName,vGroupID FROM tb_signs_aggrs WHERE id = inAggrID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errNotFound;
  END;
  vMes := 'START :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);

  StarPrepareAggrTable(inDate,inAggrID);
  vAggrTableName := UPPER(vOwner)||'.AGGR_'||vGroupID||'#'||inAggrID;
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE '||vAggrTableName||' TRUNCATE PARTITION P'||to_char(inDate,'RRRRMMDD');
    vMes := 'SUCCESSFULLY :: "'||vAggrName||'" - Table "'||LOWER(vAggrTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' truncated';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);
  EXCEPTION WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vAggrName||'" - Table "'||LOWER(vAggrTableName)||'"  - Partition P'||to_char(inDate,'RRRRMMDD')||' not truncated :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);
  END;
  StarAggrOnDate(inDate,inAggrID);
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE '||vAggrTableName||' MOVE PARTITION P'||to_char(inDate,'RRRRMMDD')||' COMPRESS';
    vMes := 'SUCCESSFULLY :: "'||vAggrName||'" - Table "'||LOWER(vAggrTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' compressed';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);
  EXCEPTION WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vAggrName||'" - Table "'||LOWER(vAggrTableName)||'"  - Partition P'||to_char(inDate,'RRRRMMDD')||' not compressed :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);
  END;
  dbms_stats.gather_table_stats(ownname => UPPER(vOwner),tabname =>  UPPER(SUBSTR(vAggrTableName,INSTR(vAggrTableName,'.') + 1)),partname =>  'P'||to_char(inDate,'RRRRMMDD'),granularity =>  'PARTITION');
  vMes := 'SUCCESSFULLY :: "'||vAggrName||'" - Table "'||LOWER(vAggrTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' stats gathered';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);

EXCEPTION
  WHEN errNotFound THEN
    vMes := 'ERROR :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate" :: Описание агрегата не найдено в таблице "'||lower(vOwner)||'.tb_signs_aggrs"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);
  WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate" :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.StarAggrRecalcOnDate',vMes);
END StarAggrRecalcOnDate;

/****************************************************************************************/

PROCEDURE HistTableService(inTableName IN VARCHAR2,inMask IN VARCHAR2,inSign IN VARCHAR2 DEFAULT NULL,inParallelJobs IN NUMBER DEFAULT 30)
  IS
    vDDL CLOB;
    vIDX CLOB;
    vStats CLOB;
    vBuff VARCHAR2(32700);
    vCou INTEGER := 0;
    vJobName VARCHAR2(256);
    vCompress BOOLEAN := SUBSTR(inMask,1,1) = '1';
    vRebuildIdx BOOLEAN := SUBSTR(inMask,2,1) = '1';
    vGatherStats BOOLEAN := SUBSTR(inMask,3,1) = '1';
    --
    vMes VARCHAR2(32700);
    vTIBegin DATE;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  vMes := 'START :: "'||inTableName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.HistTableService" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.HistTableService',vMes);

  -- Если необходим сбор статистики
  IF vGatherStats THEN
    vTIBegin := SYSDATE;
    vJobName := UPPER(vOwner)||'.SERVICEGATHERSTATSJOB_'||tb_signs_job_id_seq.nextval;
    vCou := 0;

    dbms_lob.createtemporary(vStats,FALSE);
    FOR idx IN (
      SELECT p.table_owner||'.'||p.table_name AS table_name
            ,p.partition_name AS partition_name
        FROM all_tab_partitions p
        WHERE p.table_owner = UPPER(vOwner)
          AND p.table_name = UPPER(SUBSTR(inTableName,INSTR(inTableName,'.',1,1) + 1))
          AND (UPPER(inSign) IS NULL OR
               UPPER(inSign) IS NOT NULL AND p.partition_name IN (SELECT str FROM TABLE(parse_str(UPPER(inSign),',')))
              )
    ) LOOP
      vBuff :=
      CASE WHEN vCou > 0 THEN CHR(10)||'UNION ALL'||CHR(10) END||'SELECT '''||idx.table_name||'|'||idx.partition_name||''' AS id'||CHR(10)||
      --'      ,NULL AS parent_id'||CHR(10)||
      '      ,CASE WHEN ROWNUM BETWEEN 1 AND '||inParallelJobs||' THEN NULL ELSE ROWNUM - '||inParallelJobs||' END AS parent_id'||CHR(10)||
      '      ,'''||LOWER(vOwner)||'.pkg_etl_signs.MyExecute'' AS unit'||CHR(10)||
      '      ,''BEGIN dbms_stats.gather_table_stats(ownname => '''''||UPPER(vOwner)||''''', tabname => '''''||SUBSTR(inTableName,INSTR(inTableName,'.',1,1) + 1)||''''', partname => '''''||idx.partition_name||''''', degree => 2, granularity => ''''PARTITION''''); END;'' AS params'||CHR(10)||
      '      ,0 AS skip'||CHR(10)||
      '  FROM dual';
      dbms_lob.writeappend(vStats,LENGTH(vBuff),vBuff);
      vCou := vCou + 1;
    END LOOP;

    load_new(vStats,vJobName);
    --dbms_output.put_line(vStats);

    vEndTime := SYSDATE;
    vMes := 'SUCCESSFULLY :: Table "'||inTableName||'" - stats gathered in '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.HistTableService',vMes);

  END IF;

  -- Если необходимо сжатие
  IF vCompress THEN
    vTIBegin := SYSDATE;
    vJobName := UPPER(vOwner)||'.SERVICECOMPRESSJOB_'||tb_signs_job_id_seq.nextval;
    -- После сжатия необходимо обязательное перестроение индексов,
    -- т.к. они становятся UNUSABLE
    -- Устанавливаем соответствующий флаг принудительно
    vRebuildIdx := TRUE;

    dbms_lob.createtemporary(vDDL,FALSE);
    vCou := 0;
    FOR idx IN (
      SELECT LOWER(p.table_owner||'.'||p.table_name) AS table_name
            ,LOWER(p.partition_name) AS partition_name
            ,LOWER(s.subpartition_name) AS subpartition_name
        FROM all_tab_partitions p
             LEFT JOIN all_tab_subpartitions s
               ON s.table_owner = p.table_owner
                  AND s.table_name = p.table_name
                  AND s.partition_name = p.partition_name
                  --AND s.num_rows > 0
        WHERE p.table_owner = UPPER(vOwner)
          AND p.table_name = UPPER(SUBSTR(inTableName,INSTR(inTableName,'.',1,1) + 1))
          --AND p.num_rows > 0
          AND (UPPER(inSign) IS NULL OR
               UPPER(inSign) IS NOT NULL AND p.partition_name IN (SELECT str FROM TABLE(parse_str(UPPER(inSign),',')))
              )
    ) LOOP
      vBuff :=
      CASE WHEN vCou > 0 THEN CHR(10)||'UNION ALL'||CHR(10) END||'SELECT '''||idx.table_name||'|'||CASE WHEN idx.subpartition_name IS NULL THEN idx.partition_name ELSE idx.subpartition_name END||''' AS id'||CHR(10)||
      --'      ,NULL AS parent_id'||CHR(10)||
      '      ,CASE WHEN ROWNUM BETWEEN 1 AND '||inParallelJobs||' THEN NULL ELSE ROWNUM - '||inParallelJobs||' END AS parent_id'||CHR(10)||
      '      ,'''||LOWER(vOwner)||'.pkg_etl_signs.MyExecute'' AS unit'||CHR(10)||
      '      ,''ALTER TABLE '||idx.table_name||' MOVE'||CASE WHEN idx.subpartition_name IS NULL THEN ' PARTITION '||idx.partition_name ELSE ' SUBPARTITION '||idx.subpartition_name END||' COMPRESS'' AS params'||CHR(10)||
      '      ,0 AS skip'||CHR(10)||
      '  FROM dual';
      dbms_lob.writeappend(vDDL,LENGTH(vBuff),vBuff);
      vCou := vCou + 1;
    END LOOP;

    --dbms_output.put_line(vDDL);
    load_new(vDDL,vJobName);

    vEndTime := SYSDATE;
    vMes := 'SUCCESSFULLY :: Table "'||inTableName||'" - '||vCou||' partitions compressed in '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.HistTableService',vMes);

  END IF;
  
  -- Если необходимо перестроение индексов
  IF vRebuildIdx THEN
    vTIBegin := SYSDATE;
    dbms_lob.createtemporary(vIDX,FALSE);
    vBuff := 'BEGIN'||CHR(10);
    dbms_lob.writeappend(vIDX,LENGTH(vBuff),vBuff);
    vCou := 0;
    FOR idx IN (
      SELECT LOWER(i.owner||'.'||i.index_name) AS index_name
            ,LOWER(ip.partition_name) AS partition_name
            ,LOWER(sp.subpartition_name) AS subpartition_name
        FROM all_indexes i
             LEFT JOIN all_ind_partitions ip
               ON ip.index_owner = i.owner
                  AND ip.index_name = i.index_name
                  AND (UPPER(inSign) IS NULL OR
                       UPPER(inSign) IS NOT NULL AND ip.partition_name IN (SELECT str FROM TABLE(pkg_etl_signs.parse_str(UPPER(inSign),',')))
                      )
             LEFT JOIN all_ind_subpartitions sp
               ON sp.index_owner = ip.index_owner
                  AND sp.index_name = ip.index_name
                  AND sp.partition_name = ip.partition_name
        WHERE i.owner = UPPER(vOwner)
          AND i.table_name = UPPER(SUBSTR(inTableName,INSTR(inTableName,'.',1,1) + 1))
    ) LOOP
      vBuff := 'EXECUTE IMMEDIATE ''ALTER INDEX '||idx.index_name||' REBUILD'||CASE WHEN idx.subpartition_name IS NULL THEN ' PARTITION '||idx.partition_name ELSE ' SUBPARTITION '||idx.subpartition_name END||' COMPRESS PARALLEL 4''; '||CHR(10);
      dbms_lob.writeappend(vIDX,LENGTH(vBuff),vBuff);
      vCou := vCou + 1;
    END LOOP;
    vBuff := 'END;';
    dbms_lob.writeappend(vIDX,LENGTH(vBuff),vBuff);

    BEGIN
      EXECUTE IMMEDIATE vIDX;
      --dbms_output.put_line(vIDX);

      vEndTime := SYSDATE;
      vMes := 'SUCCESSFULLY :: Table "'||inTableName||'" - '||vCou||' partitions rebuilded in '||get_ti_as_hms(vEndTime - vTIBegin);
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.HistTableService',vMes);
    EXCEPTION WHEN OTHERS THEN
      vEndTime := SYSDATE;
      vMes := 'ERROR :: Table "'||inTableName||'" :: Rebuild of indexses finished in '||get_ti_as_hms(vEndTime - vTIBegin)||' with error:'||CHR(10)||SQLERRM;
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.HistTableService',vMes);
    END;
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||inTableName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.HistTableService" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.HistTableService',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||inTableName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.HistTableService" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.HistTableService',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||inTableName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.HistTableService" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.HistTableService',vMes);
END HistTableService;

PROCEDURE ServiceTables(inAgeDays NUMBER,inDaemonId NUMBER DEFAULT NULL)
  IS
    vOwner VARCHAR2(30) := LOWER(GetVarValue('vOwner'));
    vJobName VARCHAR2(256);
    vBuff VARCHAR2(32700) :=
q'{WITH
  e AS (
    SELECT UPPER(hist_table_name) AS tbl_name,id FROM }'||vOwner||q'{.tb_entity GROUP BY hist_table_name,id
  )
SELECT ID
      ,LAG(ID) OVER (ORDER BY e_id) AS parent_id
      ,'}'||vOwner||q'{.pkg_etl_signs.histtableservice' AS unit
      ,params
      ,0 AS SKIP
  FROM (    
    SELECT LOWER(tbl) AS id
          ,LOWER(tbl)||'#!#111#!#'||LISTAGG(prt,',') WITHIN GROUP (ORDER BY NULL)||'#!#30' AS params
          ,e_id
      FROM (    
        SELECT a.table_owner||'.'||a.table_name AS tbl
              ,a.partition_name AS prt
              ,e.id AS e_id
          FROM all_tab_partitions a
               INNER JOIN e ON e.tbl_name = a.table_name
          WHERE a.table_owner = UPPER('}'||vOwner||q'{')
            AND a.partition_name != 'EMPTY_SIGN'
            AND TRUNC(SYSDATE,'DD') - TRUNC(a.last_analyzed,'DD') > }'||inAgeDays||'
        ORDER BY a.last_analyzed NULLS FIRST
      )
    GROUP BY tbl,e_id
  )';
    vCou INTEGER;
    ---
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    -- для централизованного логирования
    logOPTP NUMBER;
    logOPNM VARCHAR2(256);
    logCOMMENT VARCHAR2(4000);
    logErr VARCHAR2(32700);
    l_opid NUMBER;
    l_stid NUMBER;
    autocalc BOOLEAN := inDaemonId IS NOT NULL;
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ServiceTables" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ServiceTables',vMes);

  -- Проверка необходимости сервиса
  WITH
    e AS (
      SELECT UPPER(hist_table_name) AS tbl_name FROM tb_entity GROUP BY hist_table_name
    )
    SELECT COUNT(1) INTO vCou
        FROM all_tab_partitions a
             INNER JOIN e ON e.tbl_name = a.table_name
        WHERE a.table_owner = UPPER(vOwner)
          AND a.partition_name != 'EMPTY_SIGN'
          AND TRUNC(SYSDATE,'DD') - TRUNC(a.last_analyzed,'DD') > inAgeDays;
    
  -- Если необходим сервис
  IF vCou > 0 THEN
    IF autocalc THEN
      -- Получаем ИД типовой операции. Если типовая операция логирования отсутствует - создаем
      BEGIN
        SELECT NAME
              ,NAME
          INTO logOPNM,logCOMMENT
          FROM tb_signs_daemons WHERE id = inDaemonId;
          
        logOPTP := LG_PKG.CreateTypeOper(logOPNM,logCOMMENT);
      EXCEPTION WHEN OTHERS THEN
        logErr := SQLERRM;
        SELECT to_number(SUBSTR(logErr,INSTR(logErr,' OPTP = ') + 8,INSTR(logErr,' c именем ') - INSTR(logErr,'OPTP = ') - 7)) INTO logOPTP FROM dual;
      END;
      -- Создаем новое логирование
      vMes := 'BEGIN OF LOGGING :: OPTP = '||logOPTP;
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.ServiceTables',vMes);
      
      l_opid := LG_PKG.StartOper(logOPTP,sysdate); -- Генерируем запись об операции
      LG_PKG.setparam(l_opid,'inAgeDays',inAgeDays,'N');
      LG_PKG.setparam(l_opid,'inDaemonId',inDaemonId,'N');
      l_stid := LG_PKG.RegPhase(l_opid,'СЕРВИСНОЕ ОБСЛУЖИВАНИЕ ТАБЛИЦ');
    END IF;  
    
    vJobName := UPPER(vOwner)||'.ALLHISTTABSERVICEJOB_'||tb_signs_job_id_seq.nextval;
    load_new(vBuff,vJobName);
    --dbms_output.put_line(vBuff);
    --dbms_lock.sleep(3);

    IF autocalc THEN
      LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: СЕРВИСНОЕ ОБСЛУЖИВАНИЕ ТАБЛИЦ :: OPID = ' ||l_opid);
      LG_PKG.ENDPHASE(l_stid);
      
      LG_PKG.EndOper(l_opid);
      vMes := 'END OF LOGGING :: OPTP = '||logOPTP;
      pr_log_write(lower(vOwner)||'.pkg_etl_signs.ServiceTables',vMes);
      IF autocalc THEN SendMainLogs(l_opid); END IF;
    END IF;

  ELSE
    vMes := 'SUCCESSFULLY :: Обслуживание производилось недавно. Новое обслуживание пока не требуется ::';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ServiceTables',vMes);
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ServiceTables" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ServiceTables',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ServiceTables" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ServiceTables',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ServiceTables" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ServiceTables',vMes);
END ServiceTables;


FUNCTION GetVarCLOBValue(inVarName VARCHAR2) RETURN CLOB DETERMINISTIC
  IS
    vType VARCHAR2(30);
    vVal CLOB := NULL;
    vRes CLOB := NULL;
    --
    errNotExists EXCEPTION;
BEGIN
  BEGIN
    SELECT var_type INTO vType FROM tb_variable_registry WHERE NAME = inVarName;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errNotExists;
  END;

  SELECT val INTO vVal FROM tb_variable_registry WHERE NAME = inVarName;

  IF vType = 'Простая' THEN
    RETURN vVal;
  ELSE
    EXECUTE IMMEDIATE vVal USING OUT vRes;
    RETURN vRes;
  END IF;
EXCEPTION
  WHEN errNotExists THEN
    RETURN 'Переменная не найдена';
  WHEN OTHERS THEN
  RETURN SQLERRM;
END;

FUNCTION GetVarValue(inVarName VARCHAR2) RETURN VARCHAR2 DETERMINISTIC
IS
BEGIN
  RETURN dbms_lob.substr(pkg_etl_signs.GetVarCLOBValue(inVarName),32700,1);
END;

FUNCTION call_hist(inTable IN VARCHAR2, inID IN VARCHAR2,inAction VARCHAR2) RETURN VARCHAR2
  IS
  vStr VARCHAR2(32700);
  vRes VARCHAR2(4000);
BEGIN
  IF NOT(UPPER(inAction) IN ('ON','OFF')) THEN
    RAISE_APPLICATION_ERROR(-20000,'Неизвестное значение параметра inAction.'||CHR(10)||'Возможные значения параметра inAction: ON - включить; OFF - отключить');
  END IF;

  IF NOT CanHaveHistory(inTable) AND UPPER(inAction) = 'ON' THEN
    RAISE_APPLICATION_ERROR(-20001,'История не может быть включена для таблиц хранения, а так же для фактов и измерений куба');
  END IF;

  IF UPPER(inAction) = 'ON' THEN
    vStr := 'CREATE OR REPLACE TRIGGER '||SUBSTR(inTable,1,24)||'_h_trg';
    vStr := vStr||' AFTER INSERT OR UPDATE OR DELETE'||' ON '||inTable||Chr(10);
    vStr := vStr||'FOR EACH ROW'||Chr(10);
    vStr := vStr||'DECLARE'||Chr(10);
    vStr := vStr||'  vDML_Type VARCHAR2(1);'||Chr(10)||'  vTableID VARCHAR2(255);'||Chr(10);
    vStr := vStr||'BEGIN'||Chr(10);
    vStr := vStr||'  IF DELETING THEN'||Chr(10)||'    vDML_Type := ''D'';'||Chr(10)||'    vTableID := :Old.'||inID||';'||Chr(10)||
                  '  ELSIF INSERTING THEN'||Chr(10)||'    vDML_Type := ''I'';'||Chr(10)||'    vTableID := :New.'||inID||';'||Chr(10)||
                  '  ELSE'||Chr(10)||'    vDML_Type := ''U'';'||Chr(10)||'    vTableID := :Old.'||inID||';'||Chr(10)||
                  '  END IF;'||Chr(10);
    vStr := vStr||'  IF DELETING OR INSERTING THEN'||Chr(10);
    FOR col IN (SELECT column_name,data_type FROM dba_tab_columns
                 WHERE lower(owner||'.'||table_name) = lower(inTable) AND column_name != 'LASTUPDATE'
               )
    LOOP
      vStr := vStr||'    INSERT INTO tb_signs_history (table_name,col_name,dt,os_user,ip_addr,dml_type,old_val,new_val,table_id)'||Chr(10);
      vStr := vStr||'      VALUES('''||UPPER(inTable)||''','''||col.column_name||''',SYSDATE,sys_context(''userenv'',''OS_USER''),sys_context(''userenv'',''IP_ADDRESS''),vDML_Type,'||
        CASE WHEN col.data_type = 'NUMBER' THEN 'to_char(:Old.'||col.column_name||',''FM999999999999999D999999999'',''nls_numeric_characters='''', '''''')'
             WHEN col.data_type = 'DATE' THEN 'to_char(:Old.'||col.column_name||',''DD.MM.YYYY HH24:MI:SS'')'
        ELSE ':Old.'||col.column_name
        END||','||
        CASE WHEN col.data_type = 'NUMBER' THEN 'to_char(:New.'||col.column_name||',''FM999999999999999D999999999'',''nls_numeric_characters='''', '''''')'
             WHEN col.data_type = 'DATE' THEN 'to_char(:New.'||col.column_name||',''DD.MM.YYYY HH24:MI:SS'')'
        ELSE ':New.'||col.column_name
        END||',vTableID);'||Chr(10);
    END LOOP;
    vStr := vStr||'  ELSE'||Chr(10);
    FOR col IN (SELECT column_name,data_type FROM dba_tab_columns
                 WHERE lower(owner||'.'||table_name) = lower(inTable) AND column_name != 'LASTUPDATE'
               )
    LOOP
      vStr := vStr||'    IF :Old.'||col.column_name||' != :New.'||col.column_name||' OR'||Chr(10);
      vStr := vStr||'       :Old.'||col.column_name||' IS NULL AND :New.'||col.column_name||' IS NOT NULL OR'||Chr(10);
      vStr := vStr||'       :Old.'||col.column_name||' IS NOT NULL AND :New.'||col.column_name||' IS NULL'||Chr(10);
      vStr := vStr||'      THEN'||Chr(10);
      vStr := vStr||'        INSERT INTO tb_signs_history (table_name,col_name,dt,os_user,ip_addr,dml_type,old_val,new_val,table_id)'||Chr(10);
      vStr := vStr||'          VALUES('''||UPPER(inTable)||''','''||col.column_name||''',SYSDATE,sys_context(''userenv'',''OS_USER''),sys_context(''userenv'',''IP_ADDRESS''),vDML_Type,'||
        CASE WHEN col.data_type = 'NUMBER' THEN 'to_char(:Old.'||col.column_name||',''FM999999999999999D999999999'',''nls_numeric_characters='''', '''''')'
             WHEN col.data_type = 'DATE' THEN 'to_char(:Old.'||col.column_name||',''DD.MM.YYYY HH24:MI:SS'')'
        ELSE ':Old.'||col.column_name
        END||','||
        CASE WHEN col.data_type = 'NUMBER' THEN 'to_char(:New.'||col.column_name||',''FM999999999999999D999999999'',''nls_numeric_characters='''', '''''')'
             WHEN col.data_type = 'DATE' THEN 'to_char(:New.'||col.column_name||',''DD.MM.YYYY HH24:MI:SS'')'
        ELSE ':New.'||col.column_name
        END||',vTableID);'||Chr(10);
      vStr := vStr||'    END IF;'||Chr(10);
    END LOOP;
    vStr := vStr||'  END IF;'||Chr(10);
    --vStr := vStr||'EXCEPTION WHEN OTHERS THEN NULL;'||Chr(10);
    vStr := vStr||'END;';
    EXECUTE IMMEDIATE vStr;
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE tb_signs_history ADD PARTITION '||UPPER(REPLACE(inTable,'.','#'))||' VALUES('''||UPPER(inTable)||''') STORAGE(INITIAL 64K NEXT 1M)';
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
    EXECUTE IMMEDIATE 'GRANT SELECT,INSERT ON tb_signs_history TO '||SUBSTR(inTable,1,INSTR(inTable,'.',1,1) - 1);
    vRes := 'SUCCESSFULLY :: История по таблице "'||inTable||'" успешно включена';
  ELSE
    vStr := 'DROP TRIGGER '||SUBSTR(inTable,1,24)||'_h_trg';
    EXECUTE IMMEDIATE vStr;
    BEGIN
      EXECUTE IMMEDIATE 'REVOKE SELECT,INSERT ON tb_signs_history FROM '||SUBSTR(inTable,1,INSTR(inTable,'.',1,1) - 1);
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
    vRes := 'SUCCESSFULLY :: История по таблице "'||inTable||'" успешно отключена';
  END IF;
  RETURN vRes;
EXCEPTION WHEN OTHERS THEN
  RETURN 'ERROR :: Не удалось включить/отключить историю по таблице "'||inTable||'" :: '||SQLERRM||CHR(10)||'---------------------'||CHR(10)||vStr;
END call_hist;

FUNCTION CanHaveHistory(inTable IN VARCHAR2) RETURN BOOLEAN
  IS
    vCou INTEGER := 0;
BEGIN
  WITH
    a AS (
      SELECT LOWER(GetVarValue('vOwner')||'.'||fct_table_name) AS table_name
        FROM tb_entity
      UNION ALL
      SELECT LOWER(GetVarValue('vOwner')||'.'||hist_table_name)
        FROM tb_entity
      UNION ALL
      SELECT LOWER(GetVarValue('vOwner')||'.'||tmp_table_name)
        FROM tb_entity
      UNION ALL
      SELECT LOWER(GetVarValue('vOwner')||'.'||table_name)
        FROM TABLE(pkg_etl_signs.StarGetFldList(TRUNC(SYSDATE,'DD'),88))
      GROUP BY table_name
      /*SELECT LOWER(pkg_etl_signs.GetVarValue('vOwner')||'.'||'fct_'||group_id)
        FROM tb_signs_group
        WHERE parent_group_id IS NOT NULL
      UNION ALL
      SELECT LOWER(pkg_etl_signs.GetVarValue('vOwner')||'.'||'dim_'||g.group_id||'#'||e.id)
        FROM tb_signs_group g
             CROSS JOIN tb_entity e
        WHERE g.parent_group_id IS NULL
      UNION ALL
      SELECT LOWER(pkg_etl_signs.GetVarValue('vOwner')||'.'||'anltline_'||g.group_id||'#'||a.anlt_alias)
        FROM tb_signs_anlt a
             INNER JOIN tb_signs_group g ON g.parent_group_id IS NULL
        WHERE EXISTS (SELECT NULL FROM tb_signs_anlt_spec WHERE anlt_id = a.id)*/
    )
   ,b AS (
     SELECT LOWER(inTable) AS table_name FROM dual
    )
  SELECT COUNT(1) INTO vCou FROM a INNER JOIN b ON a.table_name = b.table_name;
  IF vCou > 0 THEN RETURN FALSE; ELSE RETURN TRUE; END IF;
END CanHaveHistory;

PROCEDURE SetFlag
  (inName IN VARCHAR2
  ,inDate IN DATE
  ,inVal IN VARCHAR2 DEFAULT NULL
  ,inAction NUMBER DEFAULT 1 -- 1 - UPSERT, 0 - DELETE
  )
  IS
    vMes VARCHAR2(4000);
    vOwner VARCHAR2(256) := getvarvalue('vOwner');
BEGIN
  IF inAction = 1 THEN
    MERGE INTO tb_flags_pool dest
      USING (SELECT inName AS NAME,inDate AS dt, inVal AS val FROM dual) src
         ON (dest.name = src.name AND dest.dt = src.dt)
      WHEN MATCHED THEN
        UPDATE SET dest.val = src.val
      WHEN NOT MATCHED THEN
        INSERT (dest.id,dest.name,dest.dt,dest.val)
          VALUES (tb_flags_pool_id_seq.nextval,src.name,src.dt,src.val);
  ELSE
    DELETE FROM tb_flags_pool WHERE dt = inDate AND NAME = inName;
  END IF;

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||vOwner||'.pkg_etl_signs.set_flag" :: '||SQLERRM;
  pr_log_write(vOwner||'.pkg_etl_signs.set_flag',vMes);
  COMMIT;
END SetFlag;

FUNCTION GetFlag(inFlagName IN VARCHAR2, inDate IN DATE) RETURN VARCHAR2
  IS
    vRes VARCHAR2(4000) := NULL;
BEGIN
  SELECT val INTO vRes FROM tb_flags_pool WHERE dt = inDate AND NAME = inFlagName;
  RETURN vRes;
EXCEPTION WHEN OTHERS THEN
  RETURN vRes;
END GetFlag;

PROCEDURE LastFlag(inFlagName IN VARCHAR2,inValue IN VARCHAR2,inDate IN OUT DATE)
  IS
    vOwner VARCHAR2(256) := GetVarValue('vOwner');
    vDate DATE;
BEGIN
  SELECT MAX(dt) INTO vDate FROM tb_flags_pool WHERE NAME = inFlagName AND dt >= inDate AND val = inValue;
  inDate := vDate;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.LastFlag','INFORMATION :: Флаг с указанными параметрами не найден в таблице "'||LOWER(vOwner)||'.tb_flags_pool"');
    inDate := NULL;
  WHEN OTHERS THEN
    inDate := NULL;
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.LastFlag','ERROR :: '||SQLERRM);
END LastFlag;

FUNCTION HaveFlagReady(inFlagName IN VARCHAR2,inFlagValue IN VARCHAR2 DEFAULT NULL,inStartDate DATE DEFAULT NULL) RETURN BOOLEAN
  IS
    vCou INTEGER := 0;
    vStartDate DATE := NVL(inStartDate,TRUNC(SYSDATE,'DD') - 2);
    vFlagValue VARCHAR2(4000) := NVL(inFlagValue,'READY');
BEGIN
  SELECT COUNT(1) INTO vCou FROM tb_flags_pool WHERE NAME = inFlagName AND dt >= vStartDate AND val = vFlagValue;
  RETURN vCou > 0;
EXCEPTION WHEN OTHERS THEN
  pr_log_write(GetVarValue('vOwner')||'.pkg_etl_signs.HaveFlagReady','ERROR :: '||SQLERRM);
  RETURN FALSE;
END HaveFlagReady;

FUNCTION SQLasHTML(inSQL IN CLOB,inColNames IN VARCHAR2,inColAliases IN VARCHAR2,inStyle IN VARCHAR2 DEFAULT NULL,inShowLogo BOOLEAN DEFAULT FALSE,inTabHeader VARCHAR2 DEFAULT NULL) RETURN CLOB
IS
  vSQL CLOB;
  vOut CLOB;
  vBuff VARCHAR2(32700);
  vCur SYS_REFCURSOR;
  vKey NUMBER;
  vKeyName VARCHAR2(30);
  vKeyVal VARCHAR2(4000);
  vPrevRowNum NUMBER := 0;
  --
  errSqlIsNull EXCEPTION;
BEGIN
  IF inSQL IS NULL OR inColNames IS NULL OR inColAliases IS NULL THEN RAISE errSqlIsNull; END IF;
  vSQL := 'SELECT * FROM (SELECT row_number() OVER (ORDER BY null) AS rownum_key,'||REPLACE(inColNames,'#!#',',')||' FROM ('||inSQL||')) UNPIVOT INCLUDE NULLS (key_val FOR key_name IN ('||REPLACE(inColNames,'#!#',',')||'))';
  dbms_lob.createtemporary(vOut,FALSE);
  vBuff :=
  CASE WHEN inStyle IS NULL THEN pkg_etl_signs.GetVarValue('HTMLTableStyle') ELSE inStyle END||Chr(10)||
  CASE WHEN inTabHeader IS NOT NULL THEN '<br/><b>'||inTabHeader||'</b><br/>' ELSE NULL END||
  '<table>'||Chr(10);
  dbms_lob.writeappend(vOut,LENGTH(vBuff),vBuff);
  vBuff := '<tr><th>'||REPLACE(inColAliases,'#!#','</th><th>')||'</th>';
  dbms_lob.writeappend(vOut,LENGTH(vBuff),vBuff);

  OPEN vCur FOR vSQL;
  IF (vCur IS NOT NULL) THEN
    LOOP
      FETCH vCur INTO vKey,vKeyName,vKeyVal;
      EXIT WHEN vCur%NOTFOUND;
      vKeyVal := '<td>'||vKeyVal||'</td>';
      IF vPrevRowNum < vKey THEN vKeyVal := '</tr>'||CHR(10)||'<tr>'||vKeyVal; END IF;
      vBuff := vKeyVal;
      dbms_lob.writeappend(vOut,LENGTH(vBuff),vBuff);
      vPrevRowNum := vKey;
    END LOOP;
    CLOSE vCur;
  END IF;
  vBuff := '</tr></table>';
  dbms_lob.writeappend(vOut,LENGTH(vBuff),vBuff);
  RETURN vOut;
EXCEPTION
  WHEN errSqlIsNull THEN RETURN ('NULL');
  WHEN OTHERS THEN RETURN SQLERRM;
END SQLasHTML;

FUNCTION ReplGetImpScript(inGroupID IN NUMBER,inMask IN VARCHAR2 DEFAULT '11111111') RETURN CLOB
  IS
  vOwner VARCHAR2(30) := GetVarValue('vOwner');
  --
  getImpEntity BOOLEAN := SUBSTR(inMask,1,1) = '1';
  getImpGroups BOOLEAN := SUBSTR(inMask,2,1) = '1';
  getImpAggr BOOLEAN   := SUBSTR(inMask,3,1) = '1';
  getImpSigns BOOLEAN := SUBSTR(inMask,4,1) = '1';
  getImpAnlt BOOLEAN := SUBSTR(inMask,5,1) = '1';
  getImpSgn2Grp BOOLEAN := SUBSTR(inMask,6,1) = '1';
  getImpAnlt2Grp BOOLEAN := SUBSTR(inMask,7,1) = '1';
  getImpSgn2Anlt BOOLEAN := SUBSTR(inMask,8,1) = '1';
  --
  vIds VARCHAR2(4000);
  vBuff VARCHAR2(32700);
  vBegDML VARCHAR2(4000);
  vEntDML CLOB;
  vGrpDML CLOB;
  vAggrDML CLOB;
  vSgnDML CLOB;
  vAnltDML CLOB;
  vSgn2GrpDML CLOB;
  vAnlt2GrpDML CLOB;
  vSgn2AnltDML CLOB;
  vEndDML VARCHAR2(4000);
  --
  vBegTime DATE := SYSDATE;
  vEndTime DATE;
  vMes VARCHAR2(4000);

BEGIN
  dbms_lob.createtemporary(vEntDML,FALSE);
  
  vBegDML :=
  'DECLARE'||CHR(10)||
  '  vEntID NUMBER;'||CHR(10)||
  '  vParentID NUMBER;'||CHR(10)||CHR(10)||
  '  vID NUMBER;'||CHR(10)||
  '  vSignSQL CLOB;'||CHR(10)||
  '  vMassSQL CLOB;'||CHR(10)||
  '  vExtPLSQL CLOB;'||CHR(10)||
  '  vCondition CLOB;'||CHR(10)||
  '  vAnltID NUMBER;'||CHR(10)||
  '  vAnltSQL CLOB;'||CHR(10)||
  '  vAnltSpecCond CLOB;'||CHR(10)||
  '  vGrpID NUMBER;'||CHR(10)||
  '  vOldGrpID NUMBER;'||CHR(10)||
  '  vFctGrpID NUMBER;'||CHR(10)||
  '  vOldFctGrpID NUMBER;'||CHR(10)||
  '  vAggrID NUMBER;'||CHR(10)||
  '  vAggrSQL CLOB;'||CHR(10)||
  '  vAggrPVal CLOB;'||CHR(10)||
  '  vSgnID NUMBER;'||CHR(10)||

  '  FUNCTION GetEntIdByName(inName VARCHAR2) RETURN NUMBER'||CHR(10)||
  '    IS'||CHR(10)||
  '      vRes NUMBER;'||CHR(10)||
  '  BEGIN'||CHR(10)||
  '    SELECT ID INTO vRes FROM '||LOWER(vOwner)||'.tb_entity WHERE entity_name = inName;'||CHR(10)||
  '    RETURN vRes;'||CHR(10)||
  '  EXCEPTION WHEN NO_DATA_FOUND THEN RETURN NULL;'||CHR(10)||
  '  END GetEntIdByName;'||CHR(10)||CHR(10)||
  '  FUNCTION GetGrpIdByName(inName VARCHAR2) RETURN NUMBER'||CHR(10)||
  '    IS'||CHR(10)||
  '      vRes NUMBER;'||CHR(10)||
  '  BEGIN'||CHR(10)||
  '    SELECT group_id INTO vRes FROM '||LOWER(vOwner)||'.tb_signs_group WHERE group_name = inName;'||CHR(10)||
  '    RETURN vRes;'||CHR(10)||
  '  EXCEPTION WHEN NO_DATA_FOUND THEN RETURN NULL;'||CHR(10)||
  '  END GetGrpIdByName;'||CHR(10)||
  '  FUNCTION GetAggrIdByName(inName VARCHAR2) RETURN NUMBER'||CHR(10)||
  '    IS'||CHR(10)||
  '      vRes NUMBER;'||CHR(10)||
  '  BEGIN'||CHR(10)||
  '    SELECT id INTO vRes FROM '||LOWER(vOwner)||'.tb_signs_aggrs WHERE aggr_name = inName;'||CHR(10)||
  '    RETURN vRes;'||CHR(10)||
  '  EXCEPTION WHEN NO_DATA_FOUND THEN RETURN NULL;'||CHR(10)||
  '  END GetAggrIdByName;'||CHR(10)||
  '  FUNCTION GetAnltIdByCode(inCode VARCHAR2) RETURN NUMBER'||CHR(10)||
  '    IS'||CHR(10)||
  '      vRes NUMBER;'||CHR(10)||
  '  BEGIN'||CHR(10)||
  '    SELECT ID INTO vRes FROM '||LOWER(vOwner)||'.tb_signs_anlt WHERE anlt_code = inCode AND sysdate BETWEEN effective_start AND effective_end;'||CHR(10)||
  '    RETURN vRes;'||CHR(10)||
  '  EXCEPTION WHEN NO_DATA_FOUND THEN RETURN NULL;'||CHR(10)||
  '  END GetAnltIdByCode;'||CHR(10)||CHR(10)||
  '  FUNCTION GetSgnIdByName(inName VARCHAR2) RETURN NUMBER'||CHR(10)||
  '    IS'||CHR(10)||
  '      vRes NUMBER;'||CHR(10)||
  '  BEGIN'||CHR(10)||
  '    SELECT id INTO vRes FROM '||LOWER(vOwner)||'.tb_signs_pool WHERE sign_name = inName;'||CHR(10)||
  '    RETURN vRes;'||CHR(10)||
  '  EXCEPTION WHEN NO_DATA_FOUND THEN RETURN NULL;'||CHR(10)||
  '  END GetSgnIdByName;'||CHR(10)||
  'BEGIN'||CHR(10);
  --dbms_lob.writeappend(vEntDML,LENGTH(vBuff),vBuff);
    
  -- Cкрипт для импорта сущностей
  IF getImpEntity THEN
    -- Сохраняем ИД всех сущностей, участвующих в звезде в строковую переменную через разделитель
    SELECT LISTAGG(entity_id,'#!#') WITHIN GROUP (ORDER BY entity_id)
      INTO vIds
      FROM (
          SELECT p.entity_id
            FROM tb_signs_pool p
                 LEFT JOIN tb_entity e
                   ON e.id = p.entity_id
            WHERE p.sign_name IN (SELECT sign_name
                                    FROM tb_signs_2_group
                                    WHERE group_id IN (SELECT group_id
                                                         FROM tb_signs_group
                                                         where level <= 2
                                                       CONNECT BY PRIOR group_id = parent_group_id
                                                       START WITH group_id = inGroupID))
          UNION
          SELECT a.entity_id
            FROM tb_signs_anlt a
            WHERE anlt_code IN (SELECT anlt_code
                                  FROM tb_anlt_2_group
                                  WHERE group_id IN (SELECT group_id
                                                       FROM tb_signs_group
                                                       WHERE level = 3
                                                     CONNECT BY PRIOR group_id = parent_group_id
                                                     START WITH group_id = inGroupID))  
      );
    vBuff := '/***** Сущности *****/'||CHR(10);
    dbms_lob.writeappend(vEntDML,LENGTH(vBuff),vBuff);
    
    FOR idx IN (
      SELECT e.id
            ,e.entity_name
            ,e.fct_table_name
            ,e.hist_table_name
            ,e.tmp_table_name
            ,p.entity_name AS parent_name
            ,NVL(to_char(e.anlt_flg),'NULL') AS anlt_flg
        FROM tb_entity e
             LEFT JOIN tb_entity p
               ON p.id = e.parent_id
      CONNECT BY e.id = PRIOR e.parent_id
      START WITH e.id IN (SELECT str FROM TABLE(parse_str(vIds,'#!#')))
      GROUP BY e.id,e.entity_name,e.fct_table_name,e.hist_table_name,e.tmp_table_name,p.entity_name,NVL(to_char(e.anlt_flg),'NULL') ORDER BY e.id
    ) LOOP
      vBuff :=
      'vParentID := GetEntIdByName(q''['||idx.parent_name||']'');'||CHR(10)||
      'MERGE INTO '||LOWER(vOwner)||'.tb_entity dest'||CHR(10)||
      '  USING (SELECT q''['||idx.entity_name||']'' AS entity_name,q''['||idx.fct_table_name||']'' AS fct_table_name,q''['||idx.hist_table_name||']'' AS hist_table_name,q''['||idx.tmp_table_name||']'' AS tmp_table_name,'||idx.anlt_flg||' AS anlt_flg FROM dual) src'||CHR(10)||
      '    ON (dest.entity_name = src.entity_name)'||CHR(10)||
      '  WHEN MATCHED THEN'||CHR(10)||
      '    UPDATE SET'||CHR(10)||
      '      dest.fct_table_name = src.fct_table_name'||CHR(10)||
      '     ,dest.hist_table_name = src.hist_table_name'||CHR(10)||
      '     ,dest.tmp_table_name = src.tmp_table_name'||CHR(10)||
      '     ,dest.parent_id = vParentID'||CHR(10)||
      '     ,dest.anlt_flg = src.anlt_flg'||CHR(10)||
      '      WHERE '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.fct_table_name,src.fct_table_name) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.hist_table_name,src.hist_table_name) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.tmp_table_name,src.tmp_table_name) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.parent_id,vParentID) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.anlt_flg,src.anlt_flg) = 0'||CHR(10)||
      '  WHEN NOT MATCHED THEN'||CHR(10)||
      '    INSERT (dest.entity_name,dest.fct_table_name,dest.hist_table_name,dest.tmp_table_name,dest.parent_id,dest.anlt_flg)'||CHR(10)||
      '      VALUES(src.entity_name,src.fct_table_name,src.hist_table_name,src.tmp_table_name,vParentID,src.anlt_flg);'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Entity: '||idx.entity_name||'" - ]''||SQL%ROWCOUNT||'' rows merged into table "'||UPPER(vOWner)||'.TB_ENTITY"'');'||CHR(10);
      dbms_lob.writeappend(vEntDML,LENGTH(vBuff),vBuff);
    END LOOP;
    vBuff :=
    'COMMIT;'||CHR(10);
    dbms_lob.writeappend(vEntDML,LENGTH(vBuff),vBuff);
  END IF;

  -- Скрипт для импорта групп
  IF getImpGroups THEN
    dbms_lob.createtemporary(vGrpDML,FALSE);
    vBuff := '/***** Группы *****/'||CHR(10);
    dbms_lob.writeappend(vGrpDML,LENGTH(vBuff),vBuff);
    FOR idx IN (
      SELECT g.group_name
            ,p.group_name AS parent_name
            ,g.strg_period
            ,g.strg_period_type
            ,level AS lev
        FROM tb_signs_group g
             LEFT JOIN tb_signs_group p
               ON p.group_id = g.parent_group_id
      CONNECT BY PRIOR g.group_id = g.parent_group_id
      START WITH g.group_id = inGroupID
    ) LOOP
      vBuff :=
      'vParentID := GetGrpIdByName(q''['||idx.parent_name||']'');'||CHR(10)||
      'MERGE INTO '||LOWER(vOwner)||'.tb_signs_group dest'||CHR(10)||
      '  USING (SELECT q''['||idx.group_name||']'' AS group_name,'||idx.strg_period||' AS strg_period,q''['||idx.strg_period_type||']'' AS strg_period_type FROM dual) src'||CHR(10)||
      '    ON (dest.group_name = src.group_name)'||CHR(10)||
      '  WHEN MATCHED THEN'||CHR(10)||
      '    UPDATE SET'||CHR(10)||
      '      dest.parent_group_id = vParentID'||CHR(10)||
      '     ,dest.strg_period = src.strg_period'||CHR(10)||
      '     ,dest.strg_period_type = src.strg_period_type'||CHR(10)||
      '      WHERE '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.parent_group_id,vParentID) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.strg_period,src.strg_period) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.strg_period_type,src.strg_period_type) = 0'||CHR(10)||
      '  WHEN NOT MATCHED THEN'||CHR(10)||
      '    INSERT (dest.group_name,dest.parent_group_id,dest.strg_period,dest.strg_period_type)'||CHR(10)||
      '      VALUES(src.group_name,vParentID,src.strg_period,src.strg_period_type);'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Group: '||idx.group_name||'" - ]''||SQL%ROWCOUNT||'' rows merged into table "'||UPPER(vOWner)||'.TB_SIGNS_GROUP"'');'||CHR(10);
      dbms_lob.writeappend(vGrpDML,LENGTH(vBuff),vBuff);
    END LOOP;
    vBuff :=
    'COMMIT;'||CHR(10);
    dbms_lob.writeappend(vGrpDML,LENGTH(vBuff),vBuff);
  END IF;
  
  -- Скрипт для импорта предагрегатов
  IF getImpAggr THEN
    dbms_lob.createtemporary(vAggrDML,FALSE);
    vBuff := '/***** Предагрегаты *****/'||CHR(10);
    --dbms_output.put_line(vBuff);
    dbms_lob.writeappend(vAggrDML,LENGTH(vBuff),vBuff);
    
    FOR idx IN (
      SELECT ag.id
            ,ag.aggr_sql
            ,ag.aggr_name
            ,g.group_name
            ,fg.group_name AS fct_group_name
            ,fg.group_id AS fct_group_id
        FROM tb_signs_aggrs ag
             LEFT JOIN tb_signs_group g
               ON g.group_id = ag.group_id
             LEFT JOIN tb_signs_group fg
               ON fg.parent_group_id = ag.group_id
        WHERE ag.group_id = inGroupID
    ) LOOP
      vBuff :=
      'vGrpID := GetGrpIdByName(q''['||idx.group_name||']'');'||CHR(10)||
      'vOldGrpID := '||inGroupID||';'||CHR(10)||
      'vFctGrpID := GetGrpIdByName(q''['||idx.fct_group_name||']'');'||CHR(10)||
      'vOldFctGrpID := '||idx.fct_group_id||';'||CHR(10)||
      --'vAggrSQL := REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(q''['||idx.aggr_sql||']'',''\.dim_''||vOldGrpID||''#|\.DIM_''||vOldGrpID||''#'',''.dim_''||vGrpID||''#''),''\.anltline_''||vOldGrpID||''#|\.ANLTLINE_''||vOldGrpID||''#'',''.anltline_''||vGrpID||''#''),''\.fct_''||vOldFctGrpID||''|\.FCT_''||vOldFctGrpID,''.fct_''||vFctGrpID);'||CHR(10)||
      'vAggrSQL := REGEXP_REPLACE(REGEXP_REPLACE(q''['||idx.aggr_sql||']'',''\.anltline_''||vOldGrpID||''#|\.ANLTLINE_''||vOldGrpID||''#'',''.anltline_''||vGrpID||''#''),''\.fct_''||vOldFctGrpID||''|\.FCT_''||vOldFctGrpID,''.fct_''||vFctGrpID);'||CHR(10)||
      'FOR dim IN ('||CHR(10)||
      '  WITH'||CHR(10)||
      '    t AS ('||CHR(10)||
      '      SELECT table_name,entity_id,entity_name FROM '||LOWER(vOwner)||'.vw_tables_with_entity'||GetVarValue('vMyDBLink')||' WHERE group_id = '||inGroupID||' AND entity_id IS NOT NULL'||CHR(10)||
      '    )'||CHR(10)||
      '    SELECT LOWER(t.table_name) AS old_table_name'||CHR(10)||
      '          ,LOWER(''DIM_''||vGrpID||''#''||e.id) AS new_table_name'||CHR(10)||
      '      FROM t'||CHR(10)||
      '           LEFT JOIN '||LOWER(vOwner)||'.tb_entity e'||CHR(10)||
      '             ON e.entity_name = t.entity_name'||CHR(10)||
      ') LOOP'||CHR(10)||
      '  vAggrSQL := REPLACE(REPLACE(vAggrSQL,dim.old_table_name,dim.new_table_name),UPPER(dim.old_table_name),dim.new_table_name);'||CHR(10)||
      'END LOOP;'||CHR(10)||
      'MERGE INTO '||LOWER(vOwner)||'.tb_signs_aggrs dest'||CHR(10)||
      '  USING (SELECT vGrpID AS group_id,q''['||idx.aggr_name||']'' AS aggr_name FROM dual) src'||CHR(10)||
      '    ON (dest.aggr_name = src.aggr_name)'||CHR(10)||
      '  WHEN NOT MATCHED THEN'||CHR(10)||
      '    INSERT (dest.group_id,dest.aggr_sql,dest.aggr_name)'||CHR(10)||
      '      VALUES(src.group_id,vAggrSQL,src.aggr_name)'||CHR(10)||
      '  WHEN MATCHED THEN'||CHR(10)||
      '    UPDATE SET'||CHR(10)||
      '      dest.group_id = src.group_id'||CHR(10)||
      '     ,dest.aggr_sql = vAggrSQL'||CHR(10)||
      '      WHERE '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.group_id,src.group_id) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.aggr_sql,vAggrSQL) = 0;'||CHR(10)||
      'vAggrID := GetAggrIdByName(q''['||idx.aggr_name||']'');'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Aggregation: '||idx.aggr_name||'" - ]''||SQL%ROWCOUNT||'' rows merged into table "'||UPPER(vOwner)||'.TB_SIGNS_AGGRS"'');'||CHR(10);
      --dbms_output.put_line(vBuff);
      dbms_lob.writeappend(vAggrDML,LENGTH(vBuff),vBuff);
      FOR agg_p IN (
        SELECT ap.aggr_id
              ,ap.p_name
              ,ap.p_value
          FROM tb_signs_aggrs_p ap
          WHERE ap.aggr_id = idx.id
      ) LOOP
        vBuff :=
        'vAggrPVal := q''['||agg_p.p_value||']'';'||CHR(10)||
        'MERGE INTO '||LOWER(vOwner)||'.tb_signs_aggrs_p dest'||CHR(10)||
        '  USING (SELECT vAggrID AS aggr_id,q''['||agg_p.p_name||']'' AS p_name FROM dual) src'||CHR(10)||
        '    ON (dest.aggr_id = src.aggr_id AND dest.p_name = src.p_name)'||CHR(10)||
        '  WHEN MATCHED THEN'||CHR(10)||
        '    UPDATE SET dest.p_value = vAggrPVal'||CHR(10)||
        '      WHERE '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.p_value,vAggrPVal) = 0'||CHR(10)||
        '  WHEN NOT MATCHED THEN'||CHR(10)||
        '    INSERT (dest.aggr_id,dest.p_name,dest.p_value)'||CHR(10)||
        '      VALUES(src.aggr_id,src.p_name,vAggrPVal);'||CHR(10)||
        LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Aggregation: '||idx.aggr_name||' -> Parameter: '||agg_p.p_name||'" - ]''||SQL%ROWCOUNT||'' rows merged into table "'||UPPER(vOwner)||'.TB_SIGNS_AGGRS_P"'');'||CHR(10);
        --dbms_output.put_line(vBuff);
        dbms_lob.writeappend(vAggrDML,LENGTH(vBuff),vBuff);
      END LOOP;
    END LOOP;
    vBuff :=
    'COMMIT;'||CHR(10);
    dbms_lob.writeappend(vAggrDML,LENGTH(vBuff),vBuff);
  END IF;
  
  -- Скрипт для импорта показателей
  IF getImpSigns THEN
    dbms_lob.createtemporary(vSgnDML,FALSE);
    vBuff := '/***** Показатели *****/'||CHR(10);
    --dbms_output.put_line(vBuff);
    dbms_lob.writeappend(vSgnDML,LENGTH(vBuff),vBuff);
    
    FOR idx IN (
      SELECT p.sign_name
            ,p.sign_descr
            ,1 AS archive_flg -- !!!Импортированные показатели не должны включаться автоматом в ежедневный автоматический расчет!!!!
                              -- !!!Архивный флаг должен отключаться ОСОЗНАННО разработчиком при помощи рук и головы!!!
            ,p.data_type
            ,p.hist_flg
            ,p.sign_sql
            ,p.mass_sql
            ,p.ext_plsql
            ,p.condition
            ,e.entity_name
        FROM tb_signs_pool p
             LEFT JOIN tb_entity e
               ON e.id = p.entity_id
        WHERE p.sign_name IN (SELECT sign_name
                                FROM tb_signs_2_group
                                WHERE group_id IN (SELECT group_id
                                                     FROM tb_signs_group
                                                     WHERE level <= 2
                                                   CONNECT BY PRIOR group_id = parent_group_id
                                                   START WITH group_id = inGroupID)) ORDER BY p.id
    ) LOOP
      vBuff :=
      'vID := '||LOWER(vOwner)||'.pkg_etl_signs.get_empty_sign_id;'||CHR(10)||
      'vSignSQL := q''['||idx.sign_sql||']'';'||CHR(10)||
      'vMassSQL := q''['||idx.mass_sql||']'';'||CHR(10)||
      'vExtPLSQL := q''['||idx.ext_plsql||']'';'||CHR(10)||
      'vCondition := q''['||idx.condition||']'';'||CHR(10)||
      'vEntID := GetEntIdByName(q''['||idx.entity_name||']'');'||CHR(10)||
      'MERGE INTO '||LOWER(vOwner)||'.tb_signs_pool dest'||CHR(10)||
      '  USING (SELECT q''['||idx.sign_name||']'' AS sign_name,q''['||idx.sign_descr||']'' AS sign_descr,'||idx.archive_flg||' AS archive_flg,q''['||idx.data_type||']'' AS data_type,'||idx.hist_flg||' AS hist_flg FROM dual) src'||CHR(10)||
      '    ON (dest.sign_name = src.sign_name)'||CHR(10)||
      '  WHEN NOT MATCHED THEN'||CHR(10)||
      '    INSERT (dest.id,dest.sign_name,dest.sign_descr,dest.archive_flg,dest.data_type,dest.hist_flg,dest.entity_id,dest.sign_sql,dest.mass_sql,dest.ext_plsql,dest.condition)'||CHR(10)||
      '      VALUES(vID,src.sign_name,src.sign_descr,src.archive_flg,src.data_type,src.hist_flg,vEntID,vSignSQL,vMassSQL,vExtPLSQL,vCondition)'||CHR(10)||
      '  WHEN MATCHED THEN'||CHR(10)||
      '    UPDATE SET'||CHR(10)||
      '      dest.sign_descr = src.sign_descr'||CHR(10)||
      '     ,dest.archive_flg = src.archive_flg'||CHR(10)||
      '     ,dest.data_type = src.data_type'||CHR(10)||
      '     ,dest.hist_flg = src.hist_flg'||CHR(10)||
      '     ,dest.entity_id = vEntID'||CHR(10)||
      '     ,dest.sign_sql = vSignSQL'||CHR(10)||
      '     ,dest.mass_sql = vMassSQL'||CHR(10)||
      '     ,dest.ext_plsql = vExtPLSQL'||CHR(10)||
      '     ,dest.condition = vCondition'||CHR(10)||
      '      WHERE '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.sign_descr,src.sign_descr) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.archive_flg,src.archive_flg) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.data_type,src.data_type) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.hist_flg,src.hist_flg) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.entity_id,vEntID) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.sign_sql,vSignSQL) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.mass_sql,vMassSQL) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.ext_plsql,vExtPLSQL) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.condition,vCondition) = 0;'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Sign: '||idx.sign_name||'" - ]''||SQL%ROWCOUNT||'' rows merged into table "'||UPPER(vOwner)||'.TB_SIGNS_POOL"'');'||CHR(10);
      --dbms_output.put_line(vBuff);
      dbms_lob.writeappend(vSgnDML,LENGTH(vBuff),vBuff);
    END LOOP;
    vBuff :=
    'COMMIT;'||CHR(10);
    dbms_lob.writeappend(vSgnDML,LENGTH(vBuff),vBuff);
  END IF;

  -- Скрипт для импорта аналитик
  IF getImpAnlt THEN
    dbms_lob.createtemporary(vAnltDML,FALSE);
    vBuff := '/***** Аналитики *****/'||CHR(10);
    --dbms_output.put_line(vBuff);
    dbms_lob.writeappend(vAnltDML,LENGTH(vBuff),vBuff);
    
    FOR idx IN (
      SELECT a.id
             ,to_char(a.effective_start,'DD.MM.RRRR HH24:MI:SS') AS effective_start
             ,to_char(a.effective_end,'DD.MM.RRRR HH24:MI:SS') AS effective_end
             ,a.anlt_code
             ,a.anlt_name
             ,a.archive_flg
             ,a.entity_id
             ,a.anlt_alias
             ,a.data_type
             ,a.anlt_alias_descr
             ,a.anlt_sql
             ,e.entity_name
         FROM tb_signs_anlt a
              LEFT JOIN tb_entity e
                ON e.id = a.entity_id
         WHERE SYSDATE BETWEEN a.effective_start AND a.effective_end
           AND anlt_code IN (SELECT anlt_code
                               FROM tb_anlt_2_group
                               WHERE group_id IN (SELECT group_id
                                                        FROM tb_signs_group
                                                        WHERE level = 3
                                                        CONNECT BY PRIOR group_id = parent_group_id
                                                        START WITH group_id = inGroupID)) ORDER BY a.id
    ) LOOP
      vBuff :=
      'vAnltSQL := q''['||idx.anlt_sql||']'';'||CHR(10)||
      'vEntID := GetEntIdByName(q''['||idx.entity_name||']'');'||CHR(10)||
      'MERGE INTO '||LOWER(vOwner)||'.tb_signs_anlt dest'||CHR(10)||
      '  USING (SELECT q''['||idx.anlt_code||']'' AS anlt_code,q''['||idx.anlt_name||']'' AS anlt_name,to_date('''||idx.effective_start||''',''DD.MM.RRRR HH24:MI:SS'') AS effective_start,to_date('''||idx.effective_end||''',''DD.MM.RRRR HH24:MI:SS'') AS effective_end,'||idx.archive_flg||' AS archive_flg,q''['||idx.anlt_alias||']'' AS anlt_alias,q''['||idx.data_type||']'' AS data_type,q''['||idx.anlt_alias_descr||']'' AS anlt_alias_descr FROM dual) src'||CHR(10)||
      '    ON (dest.anlt_code = src.anlt_code)'||CHR(10)||
      '  WHEN NOT MATCHED THEN'||CHR(10)||
      '    INSERT (dest.effective_start,dest.effective_end,dest.anlt_code,dest.anlt_name,dest.archive_flg,dest.entity_id,dest.anlt_alias,dest.data_type,dest.anlt_alias_descr,dest.anlt_sql)'||CHR(10)||
      '      VALUES(src.effective_start,src.effective_end,src.anlt_code,src.anlt_name,src.archive_flg,vEntID,src.anlt_alias,src.data_type,src.anlt_alias_descr,vAnltSQL)'||CHR(10)||
      '  WHEN MATCHED THEN'||CHR(10)||
      '    UPDATE SET'||CHR(10)||
      '      dest.effective_start = src.effective_start'||CHR(10)||
      '     ,dest.effective_end = src.effective_end'||CHR(10)||
      '     ,dest.anlt_name = src.anlt_name'||CHR(10)||
      '     ,dest.archive_flg = src.archive_flg'||CHR(10)||
      '     ,dest.entity_id = vEntID'||CHR(10)||
      '     ,dest.anlt_alias = src.anlt_alias'||CHR(10)||
      '     ,dest.data_type = src.data_type'||CHR(10)||
      '     ,dest.anlt_alias_descr = src.anlt_alias_descr'||CHR(10)||
      '     ,dest.anlt_sql = vAnltSQL'||CHR(10)||
      '      WHERE '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.effective_start,src.effective_start) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.effective_end,src.effective_end) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.anlt_name,src.anlt_name) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.archive_flg,src.archive_flg) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.entity_id,vEntID) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.anlt_alias,src.anlt_alias) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.data_type,src.data_type) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.anlt_alias_descr,src.anlt_alias_descr) = 0'||CHR(10)||
      '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.anlt_sql,vAnltSQL) = 0;'||CHR(10)||
      'vAnltID := GetAnltIdByCode(q''['||idx.anlt_code||']'');'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Analytic: '||idx.anlt_code||'" - ]''||SQL%ROWCOUNT||'' rows merged into table "'||UPPER(vOwner)||'.TB_SIGNS_ANLT"'');'||CHR(10);
      --dbms_output.put_line(vBuff);
      dbms_lob.writeappend(vAnltDML,LENGTH(vBuff),vBuff);
      FOR spec IN (
        SELECT s.anlt_spec_name
              ,s.anlt_spec_val
              ,s.parent_val
              ,s.block_import
              ,s.condition
          FROM tb_signs_anlt_spec s
          WHERE s.anlt_id = idx.id
        ORDER BY s.id
      ) LOOP
        vBuff :=
        'vAnltSpecCond := q''['||spec.condition||']'';'||CHR(10)||
        'MERGE INTO '||LOWER(vOwner)||'.tb_signs_anlt_spec dest'||CHR(10)||
        '  USING (SELECT vAnltID AS anlt_id,q''['||spec.anlt_spec_name||']'' AS anlt_spec_name,q''['||spec.anlt_spec_val||']'' AS anlt_spec_val,q''['||spec.parent_val||']'' AS parent_val,'||spec.block_import||' AS block_import FROM dual) src'||CHR(10)||
        '    ON (dest.anlt_id = src.anlt_id AND dest.anlt_spec_val = src.anlt_spec_val)'||CHR(10)||
        '  WHEN MATCHED THEN'||CHR(10)||
        '    UPDATE SET dest.anlt_spec_name = src.anlt_spec_name'||CHR(10)||
        '     ,dest.parent_val = src.parent_val'||CHR(10)||
        '     ,dest.block_import = src.block_import'||CHR(10)||
        '     ,dest.condition = vAnltSpecCond'||CHR(10)||
        '      WHERE '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.anlt_spec_name,src.anlt_spec_name) = 0'||CHR(10)||
        '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.parent_val,src.parent_val) = 0'||CHR(10)||
        '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.block_import,src.block_import) = 0'||CHR(10)||
        '             OR '||LOWER(vOwner)||'.pkg_etl_signs.IsEqual(dest.condition,vAnltSpecCond) = 0'||CHR(10)||
        '  WHEN NOT MATCHED THEN'||CHR(10)||
        '    INSERT (dest.anlt_id,dest.anlt_spec_name,dest.anlt_spec_val,dest.parent_val,dest.block_import,dest.condition)'||CHR(10)||
        '      VALUES(src.anlt_id,src.anlt_spec_name,src.anlt_spec_val,src.parent_val,src.block_import,vAnltSpecCond);'||CHR(10)||
        LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Analytic: '||idx.anlt_code||' -> Node: '||spec.anlt_spec_name||'" - ]''||SQL%ROWCOUNT||'' rows merged into table "'||UPPER(vOwner)||'.TB_SIGNS_ANLT_SPEC"'');'||CHR(10);
        --dbms_output.put_line(vBuff);
        dbms_lob.writeappend(vAnltDML,LENGTH(vBuff),vBuff);
      END LOOP;
    END LOOP;
    vBuff :=
    'COMMIT;'||CHR(10);
    dbms_lob.writeappend(vAnltDML,LENGTH(vBuff),vBuff);
  END IF;
  
  -- Скрипт для импорта привязок показателей к группам
  IF getImpSgn2Grp THEN
    dbms_lob.createtemporary(vSgn2GrpDML,FALSE);
    vBuff := '/***** Привязка показателей к группам *****/'||CHR(10);
    dbms_lob.writeappend(vSgn2GrpDML,LENGTH(vBuff),vBuff);
    
    -- Новые привязки
    FOR idx IN (
      SELECT s2g.sign_name
            ,s2g.sgn_alias
            ,s2g.sign_id
            ,1 AS active_flg
            ,s2g.preaggr_flg
            ,g.group_name
        FROM tb_signs_2_group s2g
             LEFT JOIN tb_signs_group g
               ON g.group_id = s2g.group_id
             LEFT JOIN tb_signs_pool p
               ON p.sign_name = s2g.sign_name
        WHERE s2g.group_id IN (SELECT group_id
                                 FROM tb_signs_group
                               CONNECT BY PRIOR group_id = parent_group_id
                               START WITH group_id = inGroupID) ORDER BY s2g.id
    ) LOOP
      --
      vBuff :=
      'vGrpID := GetGrpIdByName(q''['||idx.group_name||']'');'||CHR(10)||
      'vSgnID := GetSgnIdByName(q''['||idx.sign_name||']'');'||CHR(10)||
      'MERGE INTO '||LOWER(vOwner)||'.tb_signs_2_group dest'||CHR(10)||
      '  USING (SELECT q''['||idx.sign_name||']'' AS sign_name,vGrpID AS group_id,q''['||idx.active_flg||']'' AS active_flg,q''['||idx.sgn_alias||']'' AS sgn_alias,vSgnID AS sign_id,'||idx.preaggr_flg||' AS preaggr_flg FROM dual) src'||CHR(10)||
      '    ON (dest.sign_name = src.sign_name AND dest.group_id = src.group_id)'||CHR(10)||
      '  WHEN MATCHED THEN UPDATE SET dest.active_flg = 1, dest.sgn_alias = src.sgn_alias, dest.preaggr_flg = src.preaggr_flg'||CHR(10)||
      '  WHEN NOT MATCHED THEN'||CHR(10)||
      '    INSERT (dest.sign_name,dest.group_id,dest.active_flg,dest.sgn_alias,dest.sign_id,dest.preaggr_flg)'||CHR(10)||
      '      VALUES(src.sign_name,src.group_id,src.active_flg,src.sgn_alias,src.sign_id,src.preaggr_flg);'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Sign: '||idx.sign_name||' -> Group: '||idx.group_name||'" - ]''||SQL%ROWCOUNT||'' rows merged into table "'||UPPER(vOwner)||'.TB_SIGNS_2_GROUP"'');'||CHR(10);
      --dbms_output.put_line(vBuff);
      dbms_lob.writeappend(vSgn2GrpDML,LENGTH(vBuff),vBuff);
    END LOOP;

    -- Удалим все неактивные привязки к группам
    FOR grp IN (
      SELECT group_name
        FROM tb_signs_group
        --WHERE LEVEL IN (1,2)
      CONNECT BY PRIOR group_id = parent_group_id
      START WITH group_id = inGroupID
      --ORDER BY LEVEL DESC
    ) LOOP
      vBuff :=
      'vGrpID := GetGrpIdByName(q''['||grp.group_name||']'');'||CHR(10)||
      'DELETE FROM '||LOWER(vOwner)||'.tb_signs_2_group WHERE group_id = vGrpID AND active_flg = 0;'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Group: '||grp.group_name||'" - ]''||SQL%ROWCOUNT||'' rows deleted from table "'||UPPER(vOwner)||'.TB_SIGNS_2_GROUP"'');'||CHR(10)||
      'UPDATE '||LOWER(vOwner)||'.tb_signs_2_group SET active_flg = 0 WHERE group_id = vGrpID AND active_flg = 1;'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Group: '||grp.group_name||'" - ]''||SQL%ROWCOUNT||'' rows (ACTIVE_FLG from 1 to 0) updated in table "'||UPPER(vOwner)||'.TB_SIGNS_2_GROUP"'');'||CHR(10);
      dbms_lob.writeappend(vSgn2GrpDML,LENGTH(vBuff),vBuff);
    END LOOP;
    
    vBuff :=
    'COMMIT;'||CHR(10);
    dbms_lob.writeappend(vSgn2GrpDML,LENGTH(vBuff),vBuff);
  END IF;
  
  -- Скрипт для импорта привязок аналитик к группам
  IF getImpAnlt2Grp THEN
    dbms_lob.createtemporary(vAnlt2GrpDML,FALSE);
    vBuff := '/***** Привязка аналитик к группам *****/'||CHR(10);
    --dbms_output.put_line(vBuff);
    dbms_lob.writeappend(vAnlt2GrpDML,LENGTH(vBuff),vBuff);
    
    -- Новые привязки
    FOR idx IN (
      SELECT a2g.anlt_code
            ,1 AS active_flg
            ,g.group_name
        FROM tb_anlt_2_group a2g
             LEFT JOIN tb_signs_group g
               ON g.group_id = a2g.group_id
        WHERE a2g.group_id IN (SELECT group_id
                                 FROM tb_signs_group
                                 WHERE LEVEL = 3
                               CONNECT BY PRIOR group_id = parent_group_id
                               START WITH group_id = inGroupID) ORDER BY a2g.id
    ) LOOP
      vBuff :=
      'vGrpID := GetGrpIdByName(q''['||idx.group_name||']'');'||CHR(10)||
      'MERGE INTO '||LOWER(vOwner)||'.tb_anlt_2_group dest'||CHR(10)||
      '  USING (SELECT q''['||idx.anlt_code||']'' AS anlt_code,vGrpID AS group_id,q''['||idx.active_flg||']'' AS active_flg FROM dual) src'||CHR(10)||
      '    ON (dest.anlt_code = src.anlt_code AND dest.group_id = src.group_id)'||CHR(10)||
      '  WHEN MATCHED THEN UPDATE SET dest.active_flg = 1'||CHR(10)||
      '  WHEN NOT MATCHED THEN'||CHR(10)||
      '    INSERT (dest.anlt_code,dest.group_id,dest.active_flg)'||CHR(10)||
      '      VALUES(src.anlt_code,src.group_id,src.active_flg);'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Analytic: '||idx.anlt_code||' -> Group: '||idx.group_name||'" - ]''||SQL%ROWCOUNT||'' rows merged into table "'||UPPER(vOwner)||'.TB_ANLT_2_GROUP"'');'||CHR(10);
      --dbms_output.put_line(vBuff);
      dbms_lob.writeappend(vAnlt2GrpDML,LENGTH(vBuff),vBuff);
    END LOOP;

    -- Удалим все неактивные привязки к группам
    FOR grp IN (
      SELECT group_name
        FROM tb_signs_group
        WHERE LEVEL = 3
      CONNECT BY PRIOR group_id = parent_group_id
      START WITH group_id = inGroupID
      ORDER BY LEVEL DESC
    ) LOOP
      vBuff :=
      'vGrpID := GetGrpIdByName(q''['||grp.group_name||']'');'||CHR(10)||
      'DELETE FROM '||LOWER(vOwner)||'.tb_anlt_2_group WHERE group_id = vGrpID AND active_flg = 0;'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Group: '||grp.group_name||'" - ]''||SQL%ROWCOUNT||'' rows deleted from table "'||UPPER(vOwner)||'.TB_ANLT_2_GROUP"'');'||CHR(10)||
      'UPDATE '||LOWER(vOwner)||'.tb_anlt_2_group SET active_flg = 0 WHERE group_id = vGrpID AND active_flg = 1;'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Group: '||grp.group_name||'" - ]''||SQL%ROWCOUNT||'' rows (ACTIVE_FLG from 1 to 0) updated in table "'||UPPER(vOwner)||'.TB_ANLT_2_GROUP"'');'||CHR(10);
      dbms_lob.writeappend(vAnlt2GrpDML,LENGTH(vBuff),vBuff);
    END LOOP;

    vBuff :=
    'COMMIT;'||CHR(10);
    dbms_lob.writeappend(vAnlt2GrpDML,LENGTH(vBuff),vBuff);
  END IF;
  
 -- Скрипт для импорта привязок показателей к аналитикам
  IF GetImpSgn2Anlt THEN
    dbms_lob.createtemporary(vSgn2AnltDML,FALSE);
    vBuff := '/***** Привязка показателей к аналитикам *****/'||CHR(10);
    dbms_lob.writeappend(vSgn2AnltDML,LENGTH(vBuff),vBuff);
    
    FOR idx IN (
      WITH
        s AS (
          SELECT s2g.sign_name
              FROM tb_signs_2_group s2g
              WHERE s2g.group_id IN (SELECT group_id
                                       FROM tb_signs_group
                                       WHERE LEVEL = 2
                                     CONNECT BY PRIOR group_id = parent_group_id
                                     START WITH group_id = inGroupID) ORDER BY s2g.id
        )
       ,a AS (
          SELECT a2g.anlt_code
            FROM tb_anlt_2_group a2g
            WHERE a2g.group_id IN (SELECT group_id
                                     FROM tb_signs_group
                                     WHERE LEVEL = 3
                                   CONNECT BY PRIOR group_id = parent_group_id
                                   START WITH group_id = inGroupID) ORDER BY a2g.id
        )
        SELECT s2a.id,s2a.sign_name,s2a.anlt_code,1 AS active_flg
          FROM tb_sign_2_anlt s2a
          WHERE s2a.sign_name IN (SELECT sign_name FROM s)
            AND s2a.anlt_code IN (SELECT anlt_code FROM a)
    ) LOOP
      vBuff :=
      'MERGE INTO '||LOWER(vOwner)||'.tb_sign_2_anlt dest'||CHR(10)||
      '  USING (SELECT q''['||idx.sign_name||']'' AS sign_name,q''['||idx.anlt_code||']'' AS anlt_code,q''['||idx.active_flg||']'' AS active_flg FROM dual) src'||CHR(10)||
      '    ON (dest.sign_name = src.sign_name AND dest.anlt_code = src.anlt_code)'||CHR(10)||
      '  WHEN MATCHED THEN UPDATE SET dest.active_flg = 1'||CHR(10)||
      '  WHEN NOT MATCHED THEN'||CHR(10)||
      '    INSERT (dest.sign_name,dest.anlt_code,dest.active_flg)'||CHR(10)||
      '      VALUES(src.sign_name,src.anlt_code,src.active_flg);'||CHR(10)||
      LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplGetImpScript'',q''["Sign: '||idx.sign_name||' -> Analytic: '||idx.anlt_code||'" - ]''||SQL%ROWCOUNT||'' rows merged into table "'||UPPER(vOwner)||'.TB_SIGN_2_ANLT"'');'||CHR(10);
      dbms_lob.writeappend(vSgn2AnltDML,LENGTH(vBuff),vBuff);
    END LOOP;
    vBuff :=
    'COMMIT;'||CHR(10);
    dbms_lob.writeappend(vSgn2AnltDML,LENGTH(vBuff),vBuff);
  END IF;
  
  vEndDML := 'END;'||CHR(10);
  --dbms_lob.writeappend(vEndDML,LENGTH(vBuff),vBuff);
  
  
  RETURN vBegDML||vEntDML||vGrpDML||vAggrDML||vSgnDML||vAnltDML||vSgn2GrpDML||vAnlt2GrpDML||vSgn2AnltDML||vEndDML;
EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplGetImpScript" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplGetImpScript',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplGetImpScript" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplGetImpScript',vMes);
  RETURN NULL;
END ReplGetImpScript;

PROCEDURE ReplAnltOnDate(inDate IN DATE,inGroupID IN NUMBER,inGroupIdOld IN NUMBER,inDBLink IN VARCHAR2,inAnltCode IN VARCHAR2)
  IS
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vBuff VARCHAR2(32700);
    vStmt VARCHAR2(32700);
    vAnltAlias VARCHAR2(256);
    vGroupName VARCHAR2(4000);
    vAnltName VARCHAR2(4000);
    vAnltTableName VARCHAR2(256);
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    --
    errGroupNotFound EXCEPTION;
    errAnltNotFound EXCEPTION;
BEGIN
  BEGIN
    SELECT g.group_name
      INTO vGroupName
      FROM tb_signs_group g
      WHERE g.group_id = inGroupID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errGroupNotFound;
  END;

  BEGIN
    SELECT anlt_alias,anlt_name
      INTO vAnltAlias,vAnltName
      FROM tb_signs_anlt
      WHERE anlt_code = UPPER(inAnltCode)
        AND inDate BETWEEN effective_start AND effective_end;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errAnltNotFound;
  END;

  vMes := 'START :: "'||vAnltName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);

  vAnltName := 'Иерархическое измерение: "'||UPPER(vOwner)||'.ANLTLINE_'||inGroupID||'#'||vAnltAlias||'"';
  StarPrepareAnlt(inDate,inGroupID,inAnltCode);
  vAnltTableName := UPPER(vOwner)||'.ANLTLINE_'||inGroupID||'#'||vAnltAlias;
  
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE '||vAnltTableName||' TRUNCATE PARTITION P'||to_char(inDate,'RRRRMMDD');
    vMes := 'SUCCESSFULLY :: "'||vAnltName||'" - Table "'||LOWER(vAnltTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' truncated';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);
  EXCEPTION WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vAnltName||'" - Table "'||LOWER(vAnltTableName)||'"  - Partition P'||to_char(inDate,'RRRRMMDD')||' not truncated :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);
  END;
  
  vBuff :=
  'DECLARE'||CHR(10)||
  '  vStmt VARCHAR2(32700);'||CHR(10)||
  'BEGIN'||CHR(10)||
  '  SELECT ''BEGIN''||CHR(10)||'||CHR(10)||
  '         ''INSERT INTO '||vAnltTableName||' (''||LISTAGG(c.column_name,'','') WITHIN GROUP (ORDER BY c.column_id)||'')''||CHR(10)||'||CHR(10)||
  '         ''  SELECT /*+ DRIVING_SITE */ ''||LISTAGG(c.column_name,'','') WITHIN GROUP (ORDER BY c.column_id)||CHR(10)||'||CHR(10)||
  '         ''    FROM '||LOWER(vOwner)||'.ANLTLINE_'||inGroupIdOld||'#'||vAnltAlias||inDBLink||'''||CHR(10)||'||CHR(10)||
  '         ''    WHERE as_of_date = to_date('''''||to_char(inDate,'DD.MM.RRRR')||''''',''''DD.MM.RRRR'''');''||CHR(10)||'||CHR(10)||
  '         ''  '||LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''''||LOWER(vOwner)||'.pkg_etl_signs.ReplStarDataOld'''',''''SUCCESSFULLY :: ''''||SQL%ROWCOUNT||'''' rows inserted into "'||vAnltTableName||'"'''');''||CHR(10)||'||CHR(10)||
  '         ''  COMMIT;''||CHR(10)||'||CHR(10)||
  '         ''END;'' AS params'||CHR(10)||
  '    INTO vStmt'||CHR(10)||
  '    FROM all_tab_columns c'||CHR(10)||
  '    WHERE c.owner = '''||UPPER(vOwner)||''''||CHR(10)||
  '      AND c.table_name = ''ANLTLINE_'||inGroupID||'#'||UPPER(vAnltAlias)||''';'||CHR(10)||
  '  :1 := vStmt;'||CHR(10)||
  'EXCEPTION WHEN OTHERS THEN'||CHR(10)||
  '  :1 := NULL;'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplAggrOnDate'',''ERROR :: Не удалось сформировать DML для вставки :: ''||SQLERRM);'||CHR(10)||
  'END;';
  
  EXECUTE IMMEDIATE vBuff USING OUT vStmt;
  --dbms_output.put_line(vBuff);
  EXECUTE IMMEDIATE vStmt;
  --dbms_output.put_line(vStmt);
  
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE '||vAnltTableName||' MOVE PARTITION P'||to_char(inDate,'RRRRMMDD')||' COMPRESS';
    vMes := 'SUCCESSFULLY :: "'||vAnltName||'" - Table "'||LOWER(vAnltTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' compressed';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);
  EXCEPTION WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vAnltName||'" - Table "'||LOWER(vAnltTableName)||'"  - Partition P'||to_char(inDate,'RRRRMMDD')||' not compressed :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);
  END;
  
  dbms_stats.gather_table_stats(ownname => UPPER(vOwner),tabname =>  UPPER(SUBSTR(vAnltTableName,INSTR(vAnltTableName,'.') + 1)),partname =>  'P'||to_char(inDate,'RRRRMMDD'),granularity =>  'PARTITION');
  vMes := 'SUCCESSFULLY :: "'||vAnltName||'" - Table "'||LOWER(vAnltTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' stats gathered';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||vAnltName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);

EXCEPTION
  WHEN errGroupNotFound THEN
    vMes := 'ERROR :: "'||vGroupName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate" :: Описание группы не найдено в таблице "'||lower(vOwner)||'.tb_signs_group"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: "'||vGroupName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);
  WHEN errAnltNotFound THEN
    vMes := 'ERROR :: "'||vAnltName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate" :: Описание аналитики не найдено в таблице "'||lower(vOwner)||'.tb_signs_anlt"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: "'||vAnltName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);
  WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vAnltName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate" :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: "'||vAnltName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAnltOnDate',vMes);
END ReplAnltOnDate;

PROCEDURE ReplDimOnDate(inDate IN DATE,inGroupID IN NUMBER,inGroupIdOld IN NUMBER,inDBLink IN VARCHAR2,inEntityID IN NUMBER)
  IS
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vBuff VARCHAR2(32700);
    vStmt VARCHAR2(32700);
    vDimName VARCHAR2(4000);
    vEntName VARCHAR2(4000);
    vDimTableName VARCHAR2(256);
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    --
    errDimNotFound EXCEPTION;
    errEntNotFound EXCEPTION;
BEGIN
  BEGIN
    SELECT g.group_name
      INTO vDimName
      FROM tb_signs_group g
      WHERE g.group_id = inGroupID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errDimNotFound;
  END;
 
  BEGIN
    SELECT e.entity_name
      INTO vEntName
      FROM tb_entity e
      WHERE e.id = inEntityID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errEntNotFound;
  END;
  
  vMes := 'START :: "'||vDimName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);

  vDimName := 'Измерение: Группа - "'||vDimName||'"; Сущность - "'||vEntName||'"';
  StarPrepareDim(inDate,inGroupID,inEntityID);
  vDimTableName := UPPER(vOwner)||'.DIM_'||inGroupID||'#'||inEntityID;
  
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE '||vDimTableName||' TRUNCATE PARTITION P'||to_char(inDate,'RRRRMMDD');
    vMes := 'SUCCESSFULLY :: "'||vDimName||'" - Table "'||LOWER(vDimTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' truncated';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);
  EXCEPTION WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vDimName||'" - Table "'||LOWER(vDimTableName)||'"  - Partition P'||to_char(inDate,'RRRRMMDD')||' not truncated :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);
  END;
  
  vBuff :=
  'DECLARE'||CHR(10)||
  '  vStmt VARCHAR2(32700);'||CHR(10)||
  'BEGIN'||CHR(10)||
  '  WITH'||CHR(10)||
  '    t AS ('||CHR(10)||
  '      SELECT table_name FROM '||LOWER(vOwner)||'.vw_tables_with_entity'||inDBLink||' WHERE group_id = '||inGroupIdOld||' AND entity_name = '''||vEntName||''')'||CHR(10)||
  '   ,tt AS ('||CHR(10)||
  '      SELECT LOWER(t.table_name) AS old_table_name'||CHR(10)||
  '            ,LOWER(''DIM_'||inGroupID||'#'||inEntityID||''') AS new_table_name'||CHR(10)||
  '        FROM t)'||CHR(10)||
  '    SELECT ''BEGIN''||CHR(10)||'||CHR(10)||
  '           ''INSERT INTO '||LOWER(vOwner)||'.''||LOWER(tt.new_table_name)||'' (''||LISTAGG(c.column_name,'','') WITHIN GROUP (ORDER BY c.column_id)||'')''||CHR(10)||'||CHR(10)||
  '           ''  SELECT /*+ DRIVING_SITE */ ''||LISTAGG(c.column_name,'','') WITHIN GROUP (ORDER BY c.column_id)||CHR(10)||'||CHR(10)||
  '           ''    FROM '||LOWER(vOwner)||'.''||tt.old_table_name||'''||inDBLink||'''||CHR(10)||'||CHR(10)||
  '           ''    WHERE as_of_date = to_date('''''||to_char(inDate,'DD.MM.RRRR')||''''',''''DD.MM.RRRR'''');''||CHR(10)||'||CHR(10)||
  '           ''  '||LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''''||LOWER(vOwner)||'.pkg_etl_signs.ReplStarDataOld'''',''''SUCCESSFULLY :: ''''||SQL%ROWCOUNT||'''' rows inserted into "'||LOWER(vOwner)||'.''||LOWER(tt.new_table_name)||''"'''');''||CHR(10)||'||CHR(10)||
  '           ''  COMMIT;''||CHR(10)||'||CHR(10)||
  '           ''END;'' AS params'||CHR(10)||
  '  INTO vStmt'||CHR(10)||
  '  FROM tt'||CHR(10)||
  '       INNER JOIN all_tab_columns c'||CHR(10)||
  '         ON c.owner = '''||UPPER(vOwner)||''''||CHR(10)||
  '            AND c.table_name = UPPER(tt.new_table_name)'||CHR(10)||
  'GROUP BY tt.new_table_name,tt.old_table_name;'||CHR(10)||
  '  :1 := vStmt;'||CHR(10)||
  'EXCEPTION WHEN OTHERS THEN'||CHR(10)||
  '  :1 := NULL;'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplAggrOnDate'',''ERROR :: Не удалось сформировать DML для вставки :: ''||SQLERRM);'||CHR(10)||
  'END;';
  
  EXECUTE IMMEDIATE vBuff USING OUT vStmt;
  --dbms_output.put_line(vBuff);
  EXECUTE IMMEDIATE vStmt;
  --dbms_output.put_line(vStmt);
  
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE '||vDimTableName||' MOVE PARTITION P'||to_char(inDate,'RRRRMMDD')||' COMPRESS';
    vMes := 'SUCCESSFULLY :: "'||vDimName||'" - Table "'||LOWER(vDimTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' compressed';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);
  EXCEPTION WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vDimName||'" - Table "'||LOWER(vDimTableName)||'"  - Partition P'||to_char(inDate,'RRRRMMDD')||' not compressed :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);
  END;
  
  dbms_stats.gather_table_stats(ownname => UPPER(vOwner),tabname =>  UPPER(SUBSTR(vDimTableName,INSTR(vDimTableName,'.') + 1)),partname =>  'P'||to_char(inDate,'RRRRMMDD'),granularity =>  'PARTITION');
  vMes := 'SUCCESSFULLY :: "'||vDimName||'" - Table "'||LOWER(vDimTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' stats gathered';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||vDimName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);

EXCEPTION
  WHEN errDimNotFound THEN
    vMes := 'ERROR :: "'||vDimName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate" :: Описание группы не найдено в таблице "'||lower(vOwner)||'.tb_signs_group"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: "'||vDimName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);
  WHEN errEntNotFound THEN
    vMes := 'ERROR :: "'||vEntName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate" :: Описание сущности не найдено в таблице "'||lower(vOwner)||'.tb_entity"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: "'||vEntName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);
  WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vDimName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate" :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: "'||vDimName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplDimOnDate',vMes);
END ReplDimOnDate;

PROCEDURE ReplAggrOnDate(inDate IN DATE,inGroupID IN NUMBER,inGroupIdOld IN NUMBER,inDBLink IN VARCHAR2,inAggrID IN NUMBER)
  IS
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vBuff VARCHAR2(32700);
    vStmt VARCHAR2(32700);
    vAggrName VARCHAR2(4000);
    vGroupID NUMBER;
    vAggrTableName VARCHAR2(256);
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    --
    errNotFound EXCEPTION;
BEGIN
  BEGIN
    SELECT aggr_name,group_id INTO vAggrName,vGroupID FROM tb_signs_aggrs WHERE id = inAggrID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errNotFound;
  END;
  vMes := 'START :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);

  StarPrepareAggrTable(inDate,inAggrID);
  vAggrTableName := UPPER(vOwner)||'.AGGR_'||vGroupID||'#'||inAggrID;
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE '||vAggrTableName||' TRUNCATE PARTITION P'||to_char(inDate,'RRRRMMDD');
    vMes := 'SUCCESSFULLY :: "'||vAggrName||'" - Table "'||LOWER(vAggrTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' truncated';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);
  EXCEPTION WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vAggrName||'" - Table "'||LOWER(vAggrTableName)||'"  - Partition P'||to_char(inDate,'RRRRMMDD')||' not truncated :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);
  END;
  
  vBuff :=
  'DECLARE'||CHR(10)||
  '  vStmt VARCHAR2(32700);'||CHR(10)||
  'BEGIN'||CHR(10)||
  'WITH'||CHR(10)||
  'aggrs AS ('||CHR(10)||
  '  SELECT /*+ materialize */ aggr_name,old_aggr_id,aggr_id FROM ('||CHR(10)||
  '    SELECT /*+ DRIVING_SITE */'||CHR(10)||
  '           ''OLD'' AS TYPE,ID,aggr_name'||CHR(10)||
  '      FROM '||LOWER(vOwner)||'.tb_signs_aggrs'||inDBLink||CHR(10)||
  '      WHERE group_id = '||inGroupIdOld||CHR(10)||
  '    UNION ALL'||CHR(10)||
  '    SELECT ''NEW'',ID,aggr_name'||CHR(10)||
  '      FROM '||LOWER(vOwner)||'.tb_signs_aggrs'||CHR(10)||
  '      WHERE group_id = '||inGroupID||CHR(10)||
  '  ) PIVOT (MAX(ID) FOR TYPE IN (''OLD'' AS old_aggr_id,''NEW'' AS aggr_id)))'||CHR(10)||
  ' ,t AS ('||CHR(10)||
  '  SELECT table_name AS tbl'||CHR(10)||
  '        ,REPLACE(''AGGR_''||'||inGroupID||'||''#'',''AGGR_''||'||inGroupIdOld||'||''#'') AS tbl_old'||CHR(10)||
  '        ,aggrs.old_aggr_id,aggrs.aggr_id'||CHR(10)||
  '    FROM TABLE('||LOWER(vOwner)||'.pkg_etl_signs.StarGetFldList(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'||inGroupID||')) aa'||CHR(10)||
  '         INNER JOIN '||LOWER(vOwner)||'.tb_signs_aggrs agr'||CHR(10)||
  '           ON aa.table_name LIKE ''AGGR%'' AND to_char(agr.id) = SUBSTR(aa.table_name,INSTR(aa.table_name,''#'') + 1)'||CHR(10)||
  '              AND agr.id = '||inAggrID||CHR(10)||
  '         LEFT JOIN aggrs ON aggrs.aggr_name = agr.aggr_name'||CHR(10)||
  '  GROUP BY table_name,aggrs.old_aggr_id,aggrs.aggr_id)'||CHR(10)||
  '    SELECT ''BEGIN''||CHR(10)||'||CHR(10)||
  '           ''INSERT INTO '||LOWER(vOwner)||'.''||LOWER(t.tbl)||'' (''||LISTAGG(c.column_name,'','') WITHIN GROUP (ORDER BY c.column_id)||'')''||CHR(10)||'||CHR(10)||
  '           ''  SELECT /*+ DRIVING_SITE */ ''||LISTAGG(c.column_name,'','') WITHIN GROUP (ORDER BY c.column_id)||CHR(10)||'||CHR(10)||
  '           ''    FROM '||LOWER(vOwner)||'.''||LOWER(NVL2(t.old_aggr_id,LOWER(''AGGR_'||inGroupIdOld||'#''||t.old_aggr_id),LOWER(t.tbl_old))||'''||inDBLink||''')||CHR(10)||'||CHR(10)||
  '           ''    WHERE as_of_date = to_date('''''||to_char(inDate,'DD.MM.RRRR')||''''',''''DD.MM.RRRR'''');''||CHR(10)||'||CHR(10)||
  '           ''  '||LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''''||LOWER(vOwner)||'.pkg_etl_signs.ReplStarDataOld'''',''''SUCCESSFULLY :: ''''||SQL%ROWCOUNT||'''' rows inserted into "''||LOWER(t.tbl)||''"'''');''||CHR(10)||'||CHR(10)||
  '           ''  COMMIT;''||CHR(10)||'||CHR(10)||
  '           ''END;'' AS params'||CHR(10)||
  '  INTO vStmt'||CHR(10)||
  '  FROM t'||CHR(10)||
  '       INNER JOIN all_tab_columns c'||CHR(10)||
  '         ON c.owner = '''||UPPER(vOWner)||''''||CHR(10)||
  '            AND c.table_name = t.tbl'||CHR(10)||
  'GROUP BY t.tbl,t.tbl_old,t.old_aggr_id,t.aggr_id;'||CHR(10)||
  '  :1 := vStmt;'||CHR(10)||
  'EXCEPTION WHEN OTHERS THEN'||CHR(10)||
  '  :1 := NULL;'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''||LOWER(vOwner)||'.pkg_etl_signs.ReplAggrOnDate'',''ERROR :: Не удалось сформировать DML для вставки :: ''||SQLERRM);'||CHR(10)||
  'END;';
  
  EXECUTE IMMEDIATE vBuff USING OUT vStmt;
  --dbms_output.put_line(vBuff);
  EXECUTE IMMEDIATE vStmt;
  
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE '||vAggrTableName||' MOVE PARTITION P'||to_char(inDate,'RRRRMMDD')||' COMPRESS';
    vMes := 'SUCCESSFULLY :: "'||vAggrName||'" - Table "'||LOWER(vAggrTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' compressed';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);
  EXCEPTION WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vAggrName||'" - Table "'||LOWER(vAggrTableName)||'"  - Partition P'||to_char(inDate,'RRRRMMDD')||' not compressed :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);
  END;
  dbms_stats.gather_table_stats(ownname => UPPER(vOwner),tabname =>  UPPER(SUBSTR(vAggrTableName,INSTR(vAggrTableName,'.') + 1)),partname =>  'P'||to_char(inDate,'RRRRMMDD'),granularity =>  'PARTITION');
  vMes := 'SUCCESSFULLY :: "'||vAggrName||'" - Table "'||LOWER(vAggrTableName)||'" - Partition P'||to_char(inDate,'RRRRMMDD')||' stats gathered';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);

EXCEPTION
  WHEN errNotFound THEN
    vMes := 'ERROR :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate" :: Описание агрегата не найдено в таблице "'||lower(vOwner)||'.tb_signs_aggrs"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);
  WHEN OTHERS THEN
    vMes := 'ERROR :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate" :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: "'||vAggrName||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrOnDate',vMes);
END ReplAggrOnDate;

PROCEDURE ReplAggrsOnDate(inDate IN DATE,inGroupID IN NUMBER,inGroupIdOld IN NUMBER,inDBLink IN VARCHAR2)
  IS
  vOwner VARCHAR2(256) := GetVarValue('vOwner');
  vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'REPLAGGRSJOB_'||tb_signs_job_id_seq.nextval;
  vMes VARCHAR2(4000);
  vBegTime DATE := SYSDATE;
  vEndTime DATE;
  vGroupName VARCHAR2(4000);
  vCou INTEGER := 0;
  vBuff VARCHAR2(32700) :=   
  'SELECT '''||to_char(inDate,'DD.MM.RRRR')||'|''||agr.id AS ID
       ,NULL AS parent_id
       ,'''||LOWER(vOwner)||'.pkg_etl_signs.ReplAggrOnDate'' AS unit
       ,'''||to_char(inDate,'DD.MM.RRRR')||'#!#'||inGroupID||'#!#'||inGroupIdOld||'#!#'||inDBLink||'#!#''||agr.id AS params
       ,0 AS SKIP
    FROM '||LOWER(vOwner)||'.tb_signs_aggrs agr
    WHERE agr.group_id = '||inGroupID;
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAggrsOnDate" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrsOnDate',vMes);
  
  SELECT COUNT(1) INTO vCou FROM tb_signs_aggrs WHERE group_id = inGroupID;
  SELECT group_name INTO vGroupName FROM tb_signs_group WHERE group_id = inGroupID;

  IF vCou > 0 THEN
    load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
  ELSE
    vMes := 'INFORMATION :: У группы "'||vGroupName||'" отсутствуют агрегаты. Пересчет не требуется';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrsOnDate',vMes);
  END IF;

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAggrsOnDate" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrsOnDate',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAggrsOnDate" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrsOnDate',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplAggrsOnDate" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplAggrsOnDate',vMes);
END ReplAggrsOnDate;


PROCEDURE ReplStarDataOld(inDate IN DATE,inGroupID IN NUMBER,inGroupIdOld IN NUMBER,inFctGroupIdOld IN NUMBER,inDBLink IN VARCHAR2)
  IS
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'REPLDATAJOB_'||tb_signs_job_id_seq.nextval;
    vPrepare CLOB;
    vCompress CLOB;
    vBuff VARCHAR2(32700);
    vFctGroupID NUMBER;
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: "'||to_char(inDate,'DD.MM.YYYY')||'; Группа = '||inGroupID||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplStarData" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplStarData',vMes);
  
  SELECT group_id INTO vFctGroupID FROM tb_signs_group WHERE parent_group_id = inGroupID;
  
  dbms_lob.createtemporary(vPrepare,FALSE);
  vBuff :=
  'BEGIN'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.StarPrepare(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'||inGroupID||');'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.StarPrepareAggrs(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'||inGroupID||');'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.StarClear(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'||inGroupID||');'||CHR(10)||
  'END;';
  dbms_lob.writeappend(vPrepare,LENGTH(vBuff),vBuff);
  
  EXECUTE IMMEDIATE vPrepare;

  vBuff :=
  'WITH'||CHR(10)||
  'aggrs AS ('||CHR(10)||
  '  SELECT /*+ materialize */ aggr_name,old_aggr_id,aggr_id FROM ('||CHR(10)||
  '    SELECT /*+ DRIVING_SITE */'||CHR(10)||
  '           ''OLD'' AS TYPE,ID,aggr_name'||CHR(10)||
  '      FROM '||LOWER(vOwner)||'.tb_signs_aggrs'||inDBLink||CHR(10)||
  '      WHERE group_id = '||inGroupIdOld||CHR(10)||
  '    UNION ALL'||CHR(10)||
  '    SELECT ''NEW'',ID,aggr_name'||CHR(10)||
  '      FROM '||LOWER(vOwner)||'.tb_signs_aggrs'||CHR(10)||
  '      WHERE group_id = '||inGroupID||CHR(10)||
  '  ) PIVOT (MAX(ID) FOR TYPE IN (''OLD'' AS old_aggr_id,''NEW'' AS aggr_id)))'||CHR(10)||
  ' ,t AS ('||CHR(10)||
  '  SELECT table_name AS tbl'||CHR(10)||
  '        ,REPLACE(REPLACE(REPLACE(REPLACE(table_name,''DIM_''||'||inGroupID||'||''#'',''DIM_''||'||inGroupIdOld||'||''#''),''ANLTLINE_''||'||inGroupID||'||''#'',''ANLTLINE_''||'||inGroupIdOld||'||''#''),''AGGR_''||'||inGroupID||'||''#'',''AGGR_''||'||inGroupIdOld||'||''#''),''FCT_''||'||vFctGroupID||',''FCT_''||'||inFctGroupIdOld||') AS tbl_old'||CHR(10)||
  '    FROM TABLE('||LOWER(vOwner)||'.pkg_etl_signs.StarGetFldList(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'||inGroupID||')) aa'||CHR(10)||
  '         LEFT JOIN '||LOWER(vOwner)||'.tb_signs_aggrs agr'||CHR(10)||
  '           ON aa.table_name LIKE ''AGGR%'' AND to_char(agr.id) = SUBSTR(aa.table_name,INSTR(aa.table_name,''#'') + 1)'||CHR(10)||
  '         LEFT JOIN aggrs ON aggrs.aggr_name = agr.aggr_name'||CHR(10)||
  '    WHERE aa.entity_id IS NULL'||CHR(10)||
  '  GROUP BY table_name,aggrs.old_aggr_id'||CHR(10)||
  '  UNION ALL'||CHR(10)||
  '  SELECT t_new.table_name'||CHR(10)||
  '        ,t_old.table_name'||CHR(10)||
  '    FROM TABLE('||LOWER(vOwner)||'.pkg_etl_signs.StarGetFldList(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'||inGroupID||')) t_new'||CHR(10)||
  '         INNER JOIN '||LOWER(vOwner)||'.vw_tables_with_entity'||inDBLink||' t_old'||CHR(10)||
  '           ON t_old.entity_name = t_new.entity_name'||CHR(10)||
  '              AND t_old.group_id = '||inGroupIdOld||CHR(10)||
  '         LEFT JOIN '||LOWER(vOwner)||'.tb_signs_aggrs agr'||CHR(10)||
  '           ON t_new.table_name LIKE ''AGGR%'' AND to_char(agr.id) = SUBSTR(t_new.table_name,INSTR(t_new.table_name,''#'') + 1)'||CHR(10)||
  '         LEFT JOIN aggrs ON aggrs.aggr_name = agr.aggr_name'||CHR(10)||
  '    WHERE t_new.entity_id IS NOT NULL'||CHR(10)||
  '   GROUP BY t_new.table_name,t_old.table_name'||CHR(10)||
  ')'||CHR(10)||
  '    SELECT t.tbl AS ID'||CHR(10)||
  '          ,NULL AS parent_id'||CHR(10)||
  '          ,'''||LOWER(vOwner)||'.pkg_etl_signs.MyExecute'' AS unit'||CHR(10)||
  '          ,''BEGIN''||CHR(10)||'||CHR(10)||
  '           ''INSERT INTO '||LOWER(vOwner)||'.''||LOWER(t.tbl)||'' (''||LISTAGG(c.column_name,'','') WITHIN GROUP (ORDER BY c.column_id)||'')''||CHR(10)||'||CHR(10)||
  '           ''  SELECT /*+ DRIVING_SITE */ ''||LISTAGG(c.column_name,'','') WITHIN GROUP (ORDER BY c.column_id)||CHR(10)||'||CHR(10)||
  '           ''    FROM '||LOWER(vOwner)||'.''||LOWER(t.tbl_old)||'''||inDBLink||'''||CHR(10)||'||CHR(10)||
  '           ''    WHERE as_of_date = to_date('''''||to_char(inDate,'DD.MM.RRRR')||''''',''''DD.MM.RRRR'''');''||CHR(10)||'||CHR(10)||
  '           ''  '||LOWER(vOwner)||'.pkg_etl_signs.pr_log_write('''''||LOWER(vOwner)||'.pkg_etl_signs.ReplStarDataOld'''',''''SUCCESSFULLY :: ''''||SQL%ROWCOUNT||'''' rows inserted into "''||LOWER(t.tbl)||''"'''');''||CHR(10)||'||CHR(10)||
  '           ''  COMMIT;''||CHR(10)||'||CHR(10)||
  '           ''END;'' AS params'||CHR(10)||
  '          ,0 AS skip'||CHR(10)||
  '  FROM t'||CHR(10)||
  '       INNER JOIN all_tab_columns c'||CHR(10)||
  '         ON c.owner = '''||UPPER(vOWner)||''''||CHR(10)||
  '            AND c.table_name = t.tbl'||CHR(10)||
  'GROUP BY t.tbl,t.tbl_old'||CHR(10);
  
  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);
  
  
  dbms_lob.createtemporary(vCompress,FALSE);
  vBuff :=
  'BEGIN'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.StarCompress(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'||inGroupID||');'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.StarAggrsCompress(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'||inGroupID||');'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.StarGatherStats(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'||inGroupID||');'||CHR(10)||
  '  '||LOWER(vOwner)||'.pkg_etl_signs.StarAggrsGatherStats(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'||inGroupID||');'||CHR(10)||
  'END;'||CHR(10);
  dbms_lob.writeappend(vCompress,LENGTH(vBuff),vBuff);
  
  EXECUTE IMMEDIATE vCompress;
  --dbms_output.put_line(vCompress);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'; Группа = '||inGroupID||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplStarData" finished successfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplStarData',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: "'||to_char(inDate,'DD.MM.YYYY')||'; Группа = '||inGroupID||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplStarData" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplStarData',vMes);
  
  vEndTime := SYSDATE;
  vMes := 'FINISH :: "'||to_char(inDate,'DD.MM.YYYY')||'; Группа = '||inGroupID||'" - Procedure "'||lower(vOwner)||'.pkg_etl_signs.ReplStarData" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.ReplStarData',vMes);
END ReplStarDataOld;

PROCEDURE ReplStart(inOPTP IN NUMBER,inDBLink IN VARCHAR2,inOPID NUMBER DEFAULT NULL)
  IS
    -- 1412 OPNM = 'REPLICA :: РЕПЛИКА ДАННЫХ ЗВЕЗДЫ "Клиентская аналитика"'    COMM = 'DWHLegator "Клиентская аналитика" реплика данных звезды после автоматического расчета'
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vCLOB CLOB;
    vBuff VARCHAR2(32700);
    cur SYS_REFCURSOR;
    ------------------ 
    l_stid NUMBER;
    l_opid NUMBER;
    ------------------
    vDate DATE;
    vGroupID NUMBER;
    vFctGroupID NUMBER;
    vGroupName VARCHAR2(4000);
    vGroupIdNew NUMBER;
    vOPID NUMBER;
BEGIN
  l_opid := iskra.lg_pkg.StartOper(inOPTP,SYSDATE);
  IF inOPID IS NULL THEN
    vBuff :=
    q'{DECLARE
         vOPID NUMBER;
         vDate DATE;
         vGroupID NUMBER;
         vFctGroupID NUMBER;
         vGroupName VARCHAR2(4000);
       BEGIN
         SELECT vOPID
              ,to_date(vDate,'DD.MM.RRRR') AS vDate
              ,vGroupID
              ,vFctGroupID
              ,vGroupName
          INTO vOPID,vDate,vGroupID,vFctGroupID,vGroupName
          FROM (
            SELECT MAX(o.opid) KEEP (dense_rank LAST ORDER BY o.stdt,o.opid) AS vOPID
                  ,p.pnam
                  ,MAX(p.pval) KEEP (dense_rank LAST ORDER BY o.stdt,o.opid) AS pval
              FROM lg_oper}'||inDBLink||q'{ o
                   LEFT JOIN lg_pars}'||inDBLink||q'{ p
                     ON p.opid = o.opid
              WHERE o.optp = }'||inOPTP||q'{
                AND o.stdt >= TRUNC(SYSDATE,'DD') - 10
                AND o.stat = 0
                AND p.pnam IN ('inBegDate','inGroupID','inFctGroupID','inGroupName')
            GROUP BY p.pnam
          ) PIVOT (MAX(pval) FOR pnam IN ('inBegDate' AS vDate,'inGroupID' AS vGroupID,'inFctGroupID' AS vFctGroupID,'inGroupName' AS vGroupName));
          :1 := vOPID;
          :2 := vDate;
          :3 := vGroupID;
          :4 := vFctGroupID;
          :5 := vGroupName;
        END;
      }';
  ELSE
    vBuff :=
    q'{DECLARE
         vOPID NUMBER;
         vDate DATE;
         vGroupID NUMBER;
         vFctGroupID NUMBER;
         vGroupName VARCHAR2(4000);
       BEGIN
         SELECT vOPID
              ,to_date(vDate,'DD.MM.RRRR') AS vDate
              ,vGroupID
              ,vFctGroupID
              ,vGroupName
          INTO vOPID,vDate,vGroupID,vFctGroupID,vGroupName
          FROM (
            SELECT }'||inOPID||q'{ AS vOPID
                  ,p.pnam
                  ,p.pval AS pval
              FROM lg_oper}'||inDBLink||q'{ o
                   LEFT JOIN lg_pars}'||inDBLink||q'{ p
                     ON p.opid = o.opid
              WHERE o.optp = }'||inOPTP||q'{
                AND o.opid = }'||inOPID||q'{
                AND p.pnam IN ('inBegDate','inGroupID','inFctGroupID','inGroupName')
          ) PIVOT (MAX(pval) FOR pnam IN ('inBegDate' AS vDate,'inGroupID' AS vGroupID,'inFctGroupID' AS vFctGroupID,'inGroupName' AS vGroupName));
          :1 := vOPID;
          :2 := vDate;
          :3 := vGroupID;
          :4 := vFctGroupID;
          :5 := vGroupName;
        END;}';
    END IF;    
    EXECUTE IMMEDIATE vBuff USING OUT vOPID,OUT vDate,OUT vGroupID,OUT vFctGroupID,OUT vGroupName;
    LG_PKG.setparam(l_opid,'vOPID',to_char(vOPID),'N');
  
  -- Фазы
  l_stid := iskra.lg_pkg.RegPhase(l_opid,'01.ПОДГОТОВКА МЕТАДАННЫХ');
  vBuff :=
  q'{DECLARE
       cur SYS_REFCURSOR;
     BEGIN
       OPEN cur FOR SELECT ord,str FROM }'||vOwner||q'{.vw_splitted_imp_script}'||inDBLink||q'{ WHERE group_id = }'||vGroupID||q'{ ORDER BY 1;
       :1 := cur;
     END;}';
  BEGIN
    EXECUTE IMMEDIATE vBuff USING OUT cur;
    vCLOB := gather_clob(cur);
    
    EXECUTE IMMEDIATE vCLOB;
    --dbms_output.put_line('!!!ПОДГОТОВКА ЦЕЛЕВЫХ ТАБЛИЦ!!!'||CHR(10)||'----------');
    
    CLOSE cur;   
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 01.ПОДГОТОВКА МЕТАДАННЫХ :: OPID = ' ||vOPID);
    LG_PKG.ENDPHASE(l_stid);
  EXCEPTION WHEN OTHERS THEN
    CLOSE cur;
    LG_PKG.AddLog(l_stid,'E','ERROR :: 01.ПОДГОТОВКА МЕТАДАННЫХ :: OPID = ' ||vOPID||' :: '||SQLERRM);
    LG_PKG.ENDPHASE(l_stid);
  END;  

  l_stid := iskra.lg_pkg.RegPhase(l_opid,'02.РЕПЛИКАЦИЯ ДАННЫХ');
  -- Получим ИД группы на целевом сервере
  vGroupIdNew := GetGroupIdByName(vGroupName);
  BEGIN
    ReplStarDataOld(vDate,vGroupIdNew,vGroupID,vFctGroupID,inDBLink);
    --dbms_output.put_line(CHR(10)||'!!!РЕПЛИКАЦИЯ ДАННЫХ!!!'||CHR(10)||'----------');
    
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 02.РЕПЛИКАЦИЯ ДАННЫХ :: OPID = ' ||vOPID);
    LG_PKG.ENDPHASE(l_stid);
  EXCEPTION WHEN OTHERS THEN
    LG_PKG.AddLog(l_stid,'E','ERROR :: 02.РЕПЛИКАЦИЯ ДАННЫХ :: OPID = ' ||vOPID||' :: '||SQLERRM);
    LG_PKG.ENDPHASE(l_stid);
  END;
  
  iskra.lg_pkg.EndOper(l_opid);
EXCEPTION WHEN OTHERS THEN
  LG_PKG.AddLog(l_stid,'E','ERROR :: OPID = ' ||vOPID||' :: '||SQLERRM);
  iskra.lg_pkg.EndOper(l_opid);
END ReplStart;

PROCEDURE ReplAggrsStart(inOPTP IN NUMBER,inDBLink IN VARCHAR2,inOPID NUMBER DEFAULT NULL)
  IS
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vCLOB CLOB;
    vBuff VARCHAR2(32700);
    cur SYS_REFCURSOR;
    ------------------ 
    l_stid NUMBER;
    l_opid NUMBER;
    ------------------
    vDate DATE;
    vGroupID NUMBER;
    vFctGroupID NUMBER;
    vGroupName VARCHAR2(4000);
    vGroupIdNew NUMBER;
    vOPID NUMBER;
BEGIN
  l_opid := iskra.lg_pkg.StartOper(inOPTP,SYSDATE);
  IF inOPID IS NULL THEN
    vBuff :=
    q'{DECLARE
         vOPID NUMBER;
         vDate DATE;
         vGroupID NUMBER;
         vFctGroupID NUMBER;
         vGroupName VARCHAR2(4000);
       BEGIN
         SELECT vOPID
              ,to_date(vDate,'DD.MM.RRRR') AS vDate
              ,vGroupID
              ,vFctGroupID
              ,vGroupName
          INTO vOPID,vDate,vGroupID,vFctGroupID,vGroupName
          FROM (
            SELECT MAX(o.opid) KEEP (dense_rank LAST ORDER BY o.stdt,o.opid) AS vOPID
                  ,p.pnam
                  ,MAX(p.pval) KEEP (dense_rank LAST ORDER BY o.stdt,o.opid) AS pval
              FROM lg_oper}'||inDBLink||q'{ o
                   LEFT JOIN lg_pars}'||inDBLink||q'{ p
                     ON p.opid = o.opid
              WHERE o.optp = }'||inOPTP||q'{
                AND o.stdt >= TRUNC(SYSDATE,'DD') - 10
                AND o.stat = 0
                AND p.pnam IN ('inBegDate','inGroupID','inFctGroupID','inGroupName')
            GROUP BY p.pnam
          ) PIVOT (MAX(pval) FOR pnam IN ('inBegDate' AS vDate,'inGroupID' AS vGroupID,'inFctGroupID' AS vFctGroupID,'inGroupName' AS vGroupName));
          :1 := vOPID;
          :2 := vDate;
          :3 := vGroupID;
          :4 := vFctGroupID;
          :5 := vGroupName;
        END;
      }';
  ELSE
    vBuff :=
    q'{DECLARE
         vOPID NUMBER;
         vDate DATE;
         vGroupID NUMBER;
         vFctGroupID NUMBER;
         vGroupName VARCHAR2(4000);
       BEGIN
         SELECT vOPID
              ,to_date(vDate,'DD.MM.RRRR') AS vDate
              ,vGroupID
              ,vFctGroupID
              ,vGroupName
          INTO vOPID,vDate,vGroupID,vFctGroupID,vGroupName
          FROM (
            SELECT }'||inOPID||q'{ AS vOPID
                  ,p.pnam
                  ,p.pval AS pval
              FROM lg_oper}'||inDBLink||q'{ o
                   LEFT JOIN lg_pars}'||inDBLink||q'{ p
                     ON p.opid = o.opid
              WHERE o.optp = }'||inOPTP||q'{
                AND o.opid = }'||inOPID||q'{
                AND p.pnam IN ('inBegDate','inGroupID','inFctGroupID','inGroupName')
          ) PIVOT (MAX(pval) FOR pnam IN ('inBegDate' AS vDate,'inGroupID' AS vGroupID,'inFctGroupID' AS vFctGroupID,'inGroupName' AS vGroupName));
          :1 := vOPID;
          :2 := vDate;
          :3 := vGroupID;
          :4 := vFctGroupID;
          :5 := vGroupName;
        END;}';
    END IF;    
    EXECUTE IMMEDIATE vBuff USING OUT vOPID,OUT vDate,OUT vGroupID,OUT vFctGroupID,OUT vGroupName;
    LG_PKG.setparam(l_opid,'vOPID',to_char(vOPID),'N');
  
  -- Фазы
  l_stid := iskra.lg_pkg.RegPhase(l_opid,'01.ПОДГОТОВКА МЕТАДАННЫХ');
  vBuff :=
  q'{DECLARE
       cur SYS_REFCURSOR;
     BEGIN
       OPEN cur FOR SELECT ord,str FROM }'||vOwner||q'{.vw_splitted_imp_script}'||inDBLink||q'{ WHERE group_id = }'||vGroupID||q'{ ORDER BY 1;
       :1 := cur;
     END;}';
  BEGIN
    EXECUTE IMMEDIATE vBuff USING OUT cur;
    vCLOB := gather_clob(cur);
    
    EXECUTE IMMEDIATE vCLOB;
    --dbms_output.put_line('!!!ПОДГОТОВКА ЦЕЛЕВЫХ ТАБЛИЦ!!!'||CHR(10)||'----------');
    
    CLOSE cur;   
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 01.ПОДГОТОВКА МЕТАДАННЫХ :: OPID = ' ||vOPID);
    LG_PKG.ENDPHASE(l_stid);
  EXCEPTION WHEN OTHERS THEN
    CLOSE cur;
    LG_PKG.AddLog(l_stid,'E','ERROR :: 01.ПОДГОТОВКА МЕТАДАННЫХ :: OPID = ' ||vOPID||' :: '||SQLERRM);
    LG_PKG.ENDPHASE(l_stid);
  END;  

  l_stid := iskra.lg_pkg.RegPhase(l_opid,'02.РЕПЛИКАЦИЯ АГРЕГАТОВ');
  -- Получим ИД группы на целевом сервере
  vGroupIdNew := GetGroupIdByName(vGroupName);
  BEGIN
    --ReplStarDataOld(vDate,vGroupIdNew,vGroupID,vFctGroupID,inDBLink);
    ReplAggrsOnDate(vDate,vGroupIdNew,vGroupID,inDBLink);
    dbms_output.put_line(CHR(10)||'!!!РЕПЛИКАЦИЯ АГРЕГАТОВ!!!'||CHR(10)||'----------');
    
    LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: 02.РЕПЛИКАЦИЯ АГРЕГАТОВ :: OPID = ' ||vOPID);
    LG_PKG.ENDPHASE(l_stid);
  EXCEPTION WHEN OTHERS THEN
    LG_PKG.AddLog(l_stid,'E','ERROR :: 02.РЕПЛИКАЦИЯ АГРЕГАТОВ :: OPID = ' ||vOPID||' :: '||SQLERRM);
    LG_PKG.ENDPHASE(l_stid);
  END;
  
  iskra.lg_pkg.EndOper(l_opid);
EXCEPTION WHEN OTHERS THEN
  LG_PKG.AddLog(l_stid,'E','ERROR :: OPID = ' ||vOPID||' :: '||SQLERRM);
  iskra.lg_pkg.EndOper(l_opid);
END ReplAggrsStart;

FUNCTION Daemon(inCondition IN CLOB,inExecute IN CLOB,inCondParams IN VARCHAR2,inExecParams IN VARCHAR2,inComment IN VARCHAR2,inForce NUMBER DEFAULT 0) RETURN VARCHAR2
  IS
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vOut VARCHAR2(4000) := NULL;
    vCondResult VARCHAR2(1) := '0';
BEGIN
  IF inForce = 1 THEN
    vCondResult := '1';
  ELSE
    IF inCondition IS NOT NULL THEN
      vCondResult := AnyExecute(inCondition,inCondParams);
    ELSE
      vCondResult := '1';
    END IF;
  END IF;
  
  IF vCondResult = '1'/*GetConditionResult(inCondition,inCondParams,'DAEMON :: NAME = "'||inComment||'"') = 1*/ THEN
    vMes := 'START :: '||inComment||' :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.Daemon" started.';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.Daemon',vMes);

    vOut := AnyExecute(inExecute,inExecParams);

    vEndTime := SYSDATE;
    vMes := 'FINISH :: '||inComment||' :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.Daemon" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.Daemon',vMes);
  END IF;
  
  RETURN vOut;

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: '||inComment||' :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.Daemon" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.Daemon',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: '||inComment||' :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.Daemon" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.Daemon',vMes);
  RETURN vOut;
END Daemon;

PROCEDURE ExecuteDaemon(inIdentifier IN VARCHAR2,inCondParams IN VARCHAR2 DEFAULT NULL,inExecParams VARCHAR2 DEFAULT NULL,inForce NUMBER DEFAULT 0)
  IS
    vCond CLOB;
    vExec CLOB;
    vCondParams VARCHAR2(32700);
    vExecParams VARCHAR2(32700);
    vName VARCHAR2(4000);
    vCondResult VARCHAR2(1);
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    --
    logOPTP NUMBER;
    logErr VARCHAR2(32700);
    l_opid NUMBER;
    l_stid NUMBER;
    --
    errNotFound EXCEPTION;
BEGIN
  BEGIN
    IF REGEXP_LIKE(inIdentifier,'^\d+$') THEN
      SELECT cond_block
            ,exec_block
            ,NAME
            ,NVL(inCondParams,def_cond_params)
            ,NVL(inExecParams,def_exec_params)
        INTO vCond,vExec,vName,vCondParams,vExecParams
        FROM tb_signs_daemons
        WHERE id = to_number(inIdentifier);
    ELSE 
      SELECT cond_block
            ,exec_block
            ,NAME
            ,NVL(inCondParams,def_cond_params)
            ,NVL(inExecParams,def_exec_params)
        INTO vCond,vExec,vName,vCondParams,vExecParams
        FROM tb_signs_daemons
        WHERE NAME = inIdentifier;
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE errNotFound;
  END;
  IF inForce = 1 THEN
    vCondResult := '1';
  ELSE
    IF vCond IS NOT NULL THEN
      vCondResult := AnyExecute(vCond,vCondParams);
    ELSE
      vCondResult := '1';
    END IF;
  END IF;
  --IF vCond IS NOT NULL THEN vCondResults := AnyExecute(vCond,vCondParams); ELSE vCondResults := '1'; END IF;

  IF vCondResult = '1' THEN
    BEGIN    
      logOPTP := LG_PKG.CreateTypeOper(vName,'Демон репликации витрины');
    EXCEPTION WHEN OTHERS THEN
      logErr := SQLERRM;
      SELECT TRIM(REGEXP_SUBSTR(logErr,' \d+ ')) INTO logOPTP FROM dual;
    END;
  -- Создаем новое логирование
    vMes := 'BEGIN OF LOGGING :: OPTP = '||logOPTP;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon',vMes);
      
    l_opid := LG_PKG.StartOper(logOPTP,sysdate); -- Генерируем запись об операции
    IF vExecPArams IS NOT NULL THEN
      BEGIN
        FOR idx IN (
          SELECT ord,str
            FROM TABLE(parse_str(vExecParams,'#!#'))
        ) LOOP
          LG_PKG.setparam(l_opid,'p'||idx.ord,idx.str,'S');
        END LOOP;
      EXCEPTION WHEN OTHERS THEN
        vMes := 'INFORMATION :: OPTP = '||logOPTP||'; OPID = '||l_opid||' ::Parameter values not writed into logs with error :: '||SQLERRM;
        pr_log_write(lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon',vMes);
      END;
    END IF;
    l_stid := LG_PKG.RegPhase(l_opid,vName);
    BEGIN
      vMes := Daemon(vCond,vExec,vCondParams,vExecParams,vName,inForce);
      LG_PKG.AddLog(l_stid,'I','SUCCESSFULLY :: '||vName||' :: OPID = ' ||l_opid||' :: '||vMes);
    EXCEPTION WHEN OTHERS THEN
      LG_PKG.AddLog(l_stid,'E','ERROR :: '||vName||' :: OPID = ' ||l_opid||' :: '||SQLERRM);
    END;
    LG_PKG.ENDPHASE(l_stid);
    LG_PKG.EndOper(l_opid);

    vMes := 'END OF LOGGING :: OPTP = '||logOPTP;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon',vMes);
    
    SendMainLogs(l_opid);
  END IF;
EXCEPTION 
  WHEN errNotFound THEN
    vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon" :: Демон с ид/наименованием '||inIdentifier||' не найден в таблице "'||LOWER(vOwner)||'.tb_signs_daemons"';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon',vMes);
    vEndTime := SYSDATE;
    vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon',vMes);
  WHEN OTHERS THEN
    vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon" :: '||SQLERRM;
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon',vMes);
    vEndTime := SYSDATE;
    vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.ExecuteDaemon',vMes);
END ExecuteDaemon;

PROCEDURE DaemonsRun
  IS
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'DAEMONS_'||tb_signs_job_id_seq.nextval;
    vAct VARCHAR2(32700) :=
'DECLARE
   vBuff VARCHAR2(32700) :=
   ''SELECT to_char(ID) AS id
           ,to_char(parent_id) AS parent_id
           ,'''''||LOWER(vOwner)||'.pkg_etl_signs.ExecuteDaemon'''' AS unit
           ,to_char(ID)||''''#!##!##!#1'''' AS params
           ,CASE WHEN archive_flg = 0 AND '||LOWER(vOwner)||'.pkg_etl_signs.AnyExecute(cond_block,def_cond_params) = ''''1'''' THEN 0 ELSE 1 END AS SKIP
       FROM '||LOWER(vOwner)||'.tb_signs_daemons'';
BEGIN
  '||LOWER(vOwner)||'.pkg_etl_signs.load_new(vBuff,'''||vJobName||''');
END;';
BEGIN
 --load_new(vBuff,vJobName);
 sys.dbms_scheduler.create_job(job_name            => UPPER(vOwner)||'.DAEMONS_RUN_'||to_char(SYSDATE,'RRRRMMDDHH24MISS'),
                              job_type            => 'PLSQL_BLOCK',
                              job_action          => vAct,
                              start_date          => SYSDATE,
                              end_date            => to_date(null),
                              job_class           => 'DEFAULT_JOB_CLASS',
                              enabled             => true,
                              auto_drop           => TRUE,
                              comments            => 'DWHLegator - головной джоб для запуска демонов');
  --dbms_output.put_line(vAct);
END DaemonsRun;

PROCEDURE DSPrepareTable(inModelName IN VARCHAR2,inDate IN DATE,inTableType IN VARCHAR2 DEFAULT 'MD')
  IS
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vMID NUMBER;
    vMName VARCHAR2(256) := inModelName;
    vBuff VARCHAR2(32700);
    vDML CLOB;
    vCou INTEGER := 0;
    --
    vMes VARCHAR2(32700);
    vTIBegin DATE;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vTableType VARCHAR2(30);
BEGIN
  vMes := 'START :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable" started.';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);
  
  -- Сохраняем в переменную ИД модели
  SELECT ID
        ,CASE UPPER(inTableType) WHEN 'MD' THEN 'mdl' WHEN 'FIT' THEN 'fit' WHEN 'PRD' THEN 'prd' END 
    INTO vMID,vTableType 
    FROM tb_ds_mdl WHERE model_name = vMName;
  dbms_lob.createtemporary(vDML,FALSE);
  vBuff :=
  'CREATE TABLE '||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||CHR(10)||
  ' (as_of_date DATE'||CHR(10)||
  ' ,obj_gid VARCHAR2(256)'||CHR(10)||
  ' ,source_system_id VARCHAR2(30)'||CHR(10)||
  ' ,sign_name VARCHAR2(256)'||CHR(10)||
  ' ,sign_val VARCHAR2(4000)'||CHR(10)||
  ' )'||CHR(10)||
  'PARTITION BY LIST (sign_name)'||CHR(10)||
  'SUBPARTITION BY LIST (as_of_date)'||CHR(10)||
  '(PARTITION EMPTY_FTR VALUES(''EMPTY_EFTR'') STORAGE(INITIAL 64K NEXT 1M) (SUBPARTITION SP0_19000101 VALUES(to_date(''01.01.1900'',''DD.MM.RRRR'')))'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
  
  vCou := 1;
  FOR idx IN (
    SELECT id,ftr_name
      FROM tb_ds_ftrs
      WHERE mdl_id = vMID
  ) LOOP
    vBuff := CASE WHEN vCou > 0 THEN ',' END||'PARTITION '||idx.ftr_name||' VALUES('''||idx.ftr_name||''') STORAGE(INITIAL 64K NEXT 1M) (SUBPARTITION SP'||idx.id||'_'||to_char(inDate,'RRRRMMDD')||' VALUES(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR'')))'||CHR(10);
    dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
    vCou := vCou + 1;
  END LOOP;
  vBuff :=
  ') COMPRESS NOLOGGING'||CHR(10);
  dbms_lob.writeappend(vDML,LENGTH(vBuff),vBuff);
  --dbms_output.put_line(vDML);
  
  vTIBegin := SYSDATE;
  BEGIN
    EXECUTE IMMEDIATE vDML;
    vEndTime := SYSDATE;
    vMes := 'SUCCESSFULLLY :: Table "'||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||'" created in '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);      
  EXCEPTION WHEN OTHERS THEN
    vMes := 'INFORMATION :: Создание таблицы "'||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||'" не требуется';
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);      

    FOR idx IN (
      SELECT id,ftr_name
        FROM tb_ds_ftrs
        WHERE mdl_id = vMID
    ) LOOP
      BEGIN
        vBuff := 'ALTER TABLE '||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||' ADD PARTITION '||idx.ftr_name||' VALUES('''||idx.ftr_name||''') STORAGE(INITIAL 64K NEXT 1M) (SUBPARTITION SP'||idx.id||'_'||to_char(inDate,'RRRRMMDD')||' VALUES(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR'')))';
        EXECUTE IMMEDIATE vBuff;
        vMes := 'SUCCESSFULLY :: Table "'||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||'" altered. Partition '||idx.ftr_name||' added. Subpartition SP'||idx.id||'_'||to_char(inDate,'RRRRMMDD')||' added.';
      EXCEPTION WHEN OTHERS THEN
        --pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable','ERROR DDL :: '||vBuff);
        BEGIN
          vBuff := 'ALTER TABLE '||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||' MODIFY PARTITION '||idx.ftr_name||' ADD SUBPARTITION SP'||idx.id||'_'||to_char(inDate,'RRRRMMDD')||' VALUES(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''))';
          EXECUTE IMMEDIATE vBuff;
          vMes := 'SUCCESSFULLY :: Table "'||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||'" altered. Partition '||idx.ftr_name||' modified. Subpartition SP'||idx.id||'_'||to_char(inDate,'RRRRMMDD')||' added';
          EXCEPTION WHEN OTHERS THEN
            --pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable','ERROR DDL :: '||vBuff);
            vBuff := 'ALTER TABLE '||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||' TRUNCATE SUBPARTITION SP'||idx.id||'_'||to_char(inDate,'RRRRMMDD');
            EXECUTE IMMEDIATE vBuff;
            vMes := 'SUCCESSFULLY :: Table "'||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||'" altered. Partition '||idx.ftr_name||'. Subpartition SP'||idx.id||'_'||to_char(inDate,'RRRRMMDD')||' truncated';
          END;
      END;
      pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);      
    END LOOP;
    
    /*EXECUTE IMMEDIATE 'ALTER TABLE '||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||' TRUNCATE SUBPARTITION SP'||idx.id||'_'||to_char(inDate,'RRRRMMDD');
    vEndTime := SYSDATE;
    vMes := 'SUCCESSFULLY :: Table "'||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||' - Subpartition SP'||idx.id||'_'||to_char(inDate,'RRRRMMDD')||'" truncated';
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);      
    FOR idx IN (
      SELECT id,ftr_name
        FROM tb_ds_ftrs
        WHERE mdl_id = vMID
    ) LOOP
      BEGIN
        --vBuff := CASE WHEN vCou > 0 THEN ',' END||'PARTITION '||idx.ftr_name||' VALUES('''||idx.ftr_name||''') STORAGE(INITIAL 64K NEXT 1M) (SUBPARTITION SP'||idx.id||'_TEACH VALUES(''TEACH''),SUBPARTITION SP'||idx.id||'_VALID VALUES(''VALID''))'||CHR(10);
        EXECUTE IMMEDIATE 'ALTER TABLE '||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||' ADD PARTITION '||idx.ftr_name||' VALUES('''||idx.ftr_name||''') STORAGE(INITIAL 64K NEXT 1M) (SUBPARTITION SP'||idx.id||'_'||to_char(inDate,'RRRRMMDD')||' VALUES(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR'')))';
        vEndTime := SYSDATE;
        vMes := 'SUCCESSFULLLY :: Table "'||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||'" altered - Partition '||idx.ftr_name||' added in '||get_ti_as_hms(vEndTime - vTIBegin);
        pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);
      EXCEPTION WHEN OTHERS THEN
        NULL;
      END;
    END LOOP;*/
  END;
  
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: Модель с именем "'||vMName||'" не найдена в таблице "'||LOWER(vOwner)||'.tb_ds_mdl"';
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);

    vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||SQLERRM;
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);

    vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSPrepareTable" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSPrepareTable',vMes);
END DSPrepareTable;

FUNCTION DSGetFtr(inDate IN DATE,inModelName IN VARCHAR2,inFtrName IN VARCHAR2) RETURN TTab PIPELINED
  IS
    vPrc NUMBER;
    vSQL CLOB;
    rec TRec;
    cur INTEGER;       -- хранит идентификатор (ID) курсора
    ret INTEGER;       -- хранит возвращаемое по вызову значение
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
BEGIN
  -- Сохраняем в переменные Наименование фичи, SQL фичи и Процент валидационной выборки модели
  SELECT f.ftr_sql,m.valid_prc
    INTO vSQL,vPrc
    FROM tb_ds_mdl m
         INNER JOIN tb_ds_ftrs f
           ON f.mdl_id = m.id
              AND f.ftr_name = UPPER(inFtrName)
    WHERE m.model_name = inModelName;

  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur, vSQL, dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.obj_gid,256);
  dbms_sql.define_column(cur,2,rec.source_system_id,30);
  dbms_sql.define_column(cur,3,rec.sign_name,256);
  dbms_sql.define_column(cur,4,rec.sign_val,4000);

  dbms_sql.bind_variable_char(cur,'inDate',to_char(inDate,'DD.MM.YYYY'));
  dbms_sql.bind_variable_char(cur,'inPrc',to_char(vPrc));
  dbms_sql.bind_variable_char(cur,'inFtrName',UPPER(inFtrName));

  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.obj_gid);
    dbms_sql.column_value(cur,2,rec.source_system_id);
    dbms_sql.column_value(cur,3,rec.sign_name);
    dbms_sql.column_value(cur,4,rec.sign_val);
    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSGetFtr','ERROR :: "'||UPPER(inFtrName)||'"  - Фича не найдена в таблице "'||lower(vOwner)||'.tb_ds_ftrs"');
  WHEN OTHERS THEN
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSGetFtr','ERROR :: "'||UPPER(inFtrName)||'"  - '||SQLERRM||CHR(10)||'----------'||CHR(10)||vSQL);
END DSGetFtr;

FUNCTION DSFitGetFtrSQL(inDate IN DATE,inModelName IN VARCHAR2,inFtrName IN VARCHAR2,inTop IN INTEGER DEFAULT 3) RETURN VARCHAR2
  IS
    vOwner VARCHAR2(256) := GetVarValue('vOwner');
    vDate VARCHAR2(30) := to_char(inDate,'DD.MM.RRRR');
    vBuff VARCHAR2(32700);
    vVals VARCHAR2(32700);
    vDownPIVOT VARCHAR2(32700);
    vMID NUMBER;
    vTarget VARCHAR2(256);
    --vTableType VARCHAR2(30);
BEGIN
  SELECT m.id
        --,m.model_name
        ,f.ftr_name
    INTO vMID--,vTableType
    ,vTarget
    FROM tb_ds_mdl m
         INNER JOIN tb_ds_ftrs f
           ON f.mdl_id = m.id
              AND ftr_type = 'T'
    WHERE model_name = inModelName;
  
  SELECT LISTAGG(''''||str||'''',',') WITHIN GROUP (ORDER BY ord) AS vVals
        ,LISTAGG(''''||str||''' AS '||LOWER(str),',') WITHIN GROUP (ORDER BY ord) AS vDownPIVOT
    INTO vVals,vDownPIVOT
    FROM TABLE(parse_str(inFtrName,','));
  
  vBuff :=
q'{SELECT to_date('}'||vDate||q'{','DD.MM.RRRR') AS as_of_date
      ,}'||REPLACE(LOWER(inFtrName),',','||''#!#''||')||q'{||'#!#'||to_char(rn) AS obj_gid
      ,source_system_id
      ,src_name AS sign_name
      ,}'||LOWER(vTarget)||q'{||'#!#'||to_char(trg_prt,'FM999999999999999D999999999','nls_numeric_characters='', ''') AS sign_val
  FROM (
    SELECT source_system_id,
           }'||LOWER(vTarget)||','||LOWER(inFtrName)||q'{
          ,src_name
          ,trg_prt
          ,row_number() OVER (PARTITION BY }'||LOWER(inFtrName)||q'{ ORDER BY trg_prt DESC) AS rn
      FROM (
        SELECT source_system_id,
              }'||LOWER(vTarget)||','||LOWER(inFtrName)||q'{
              ,src_name
              ,cou/SUM(cou) OVER (PARTITION BY }'||LOWER(inFtrName)||q'{) AS trg_prt
          FROM (
            SELECT source_system_id,
                   }'||LOWER(vTarget)||','||LOWER(inFtrName)||q'{
                  ,'}'||REPLACE(inFtrName,',','#!#')||q'{' AS src_name
                  ,COUNT(1) AS cou
              FROM (
                SELECT src.obj_gid
                      ,src.source_system_id
                      ,src.sign_name
                      ,src.sign_val
                  FROM }'||LOWER(vOwner)||'.ptb_ds_mdl_'||vMID||q'{ src
                  WHERE src.sign_name IN ('}'||vTarget||q'{',}'||vVals||q'{)
                    AND src.source_system_id = 'TEACH'
                    AND src.as_of_date =  to_date('}'||vDate||q'{','DD.MM.RRRR')
          ) PIVOT (MAX(sign_val) FOR sign_name IN ('}'||vTarget||q'{' AS }'||LOWER(vTarget)||','||vDownPIVOT||q'{))
          GROUP BY source_system_id,}'||LOWER(vTarget)||','||LOWER(inFtrName)||q'{
    ))) WHERE rn <= }'||inTop;
    RETURN vBuff;
EXCEPTION WHEN OTHERS THEN
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitGetFtrSQL','ERROR :: '||inFtrName||' :: '||SQLERRM);
  RETURN NULL;
END DSFitGetFtrSQL;

PROCEDURE DSFtrOnDate(inDate IN DATE,inModelName IN VARCHAR2,inFtrName IN VARCHAR2)
  IS
    vOwner VARCHAR2(30) := pkg_etl_signs.GetVarValue('vOwner');
    vMID NUMBER;
    vMName VARCHAR2(256);
    vCou INTEGER := 0;
    vBuff VARCHAR2(32700);
    --vTableType VARCHAR2(30);
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFtrOnDate" started.';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFtrOnDate',vMes);
  
  -- Сохраняем в переменные ИД,Наименование и Процент валидационной выборки модели
  SELECT m.id--,m.model_name
    INTO vMID--,vTableType
    FROM tb_ds_mdl m
         INNER JOIN tb_ds_ftrs f
           ON f.mdl_id = m.id
              AND f.ftr_name = UPPER(inFtrName)
    WHERE m.model_name = inModelName;

  --vDML := 'INSERT INTO '||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||' (as_of_date,obj_gid,source_system_id,sign_name,sign_val) '||CHR(10)||vDML||CHR(10)||';'||CHR(10)||'COMMIT';
  vBuff :=
  'DECLARE'||CHR(10)||
  '  vCou INTEGER := 0;'||CHR(10)||
  'BEGIN'||CHR(10)||
  '  INSERT INTO '||LOWER(vOwner)||'.ptb_ds_mdl_'||vMID||' (as_of_date,obj_gid,source_system_id,sign_name,sign_val)'||CHR(10)||
  '   SELECT  to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR'') AS as_of_date,obj_gid,source_system_id,sign_name,sign_val'||CHR(10)||
  '       FROM TABLE('||LOWER(vOwner)||'.pkg_etl_signs.DSGetFtr(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),'''||inModelName||''','''||UPPER(inFtrName)||'''));'||CHR(10)||
  '  :1 := SQL%ROWCOUNT;'||CHR(10)||
  'END;';
    
  --dbms_output.put_line(vBuff);
  EXECUTE IMMEDIATE vBuff USING OUT vCou;
  COMMIT;

  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: "'||UPPER(inFtrName)||'" - "'||to_char(inDate,'DD.MM.RRRR')||'" :: '||vCou||' rows inserted into table "'||LOWER(vOwner)||'.ptb_ds_mdl_'||vMID||'"';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFtrOnDate',vMes);
  
  vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFtrOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFtrOnDate',vMes);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: Совокупность Модель: "'||vMName||'" - Фича: "'||UPPER(inFtrName)||'" не найдена в справочниках';
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFtrOnDate',vMes);

    vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFtrOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFtrOnDate',vMes);
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||SQLERRM;
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFtrOnDate',vMes);

    vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFtrOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFtrOnDate',vMes);
END DSFtrOnDate;

PROCEDURE DSFitFtrOnDate(inDate IN DATE,inModelName IN VARCHAR2,inFtrName IN VARCHAR2,inTopPrdCou IN INTEGER DEFAULT 3)
  IS
    vOwner VARCHAR2(30) := pkg_etl_signs.GetVarValue('vOwner');
    vMID NUMBER;
    vFtrName VARCHAR2(256);
    vTrgName VARCHAR2(256);
    vDSAlgo VARCHAR2(30);
    vA NUMBER;
    vB NUMBER;
    vCou INTEGER := 0;
    vBuff VARCHAR2(32700);
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    --vTableType VARCHAR2(30);
BEGIN
  vMes := 'START :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate" started.';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate',vMes);
  
  -- Сохраняем в переменные ИД модели
  SELECT m.id,ds_algo INTO vMID,vDSAlgo FROM tb_ds_mdl m WHERE m.model_name = inModelName;

  IF vDSAlgo = 'CLASS_TE' THEN
    SELECT f.ftr_name,t.ftr_name
      INTO vFtrName
          ,vTrgName
      FROM tb_ds_ftrs f
           LEFT JOIN tb_ds_ftrs t
             ON t.mdl_id = vMID
                AND t.ftr_type = 'T'
      WHERE f.mdl_id = vMID
        AND f.ftr_type = 'F'
        AND f.ftr_name = UPPER(inFtrName);

    --vDML := 'INSERT INTO '||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||' (as_of_date,obj_gid,source_system_id,sign_name,sign_val) '||CHR(10)||vDML||CHR(10)||';'||CHR(10)||'COMMIT';
    vBuff :=
    'DECLARE'||CHR(10)||
    '  vCou INTEGER := 0;'||CHR(10)||
    'BEGIN'||CHR(10)||
    '  INSERT INTO '||LOWER(vOwner)||'.ptb_ds_fit_'||vMID||' (as_of_date,obj_gid,source_system_id,sign_name,sign_val)'||CHR(10)||
    DSFitGetFtrSQL(inDate,inModelName,UPPER(inFtrName),inTopPrdCou)||CHR(10)||';'||CHR(10)||
    '  :1 := SQL%ROWCOUNT;'||CHR(10)||
    'END;';
  ELSIF vDSAlgo = 'REGR_LTR' THEN
    DSRegrLTRGetKoef(
    q'{SELECT row_number() OVER (ORDER BY as_of_date) AS x
      ,to_number(sign_val,'FM999999999999999D999999999','nls_numeric_characters='', ''') AS y
  FROM }'||LOWER(vOwner)||'.ptb_ds_mdl_'||vMID||q'{
  WHERE sign_name = '}'||UPPER(inFtrName)||q'{'
    AND source_system_id = 'TEACH'}',vA,vB);

    vBuff :=
    'DECLARE'||CHR(10)||
    '  vCou INTEGER := 0;'||CHR(10)||
    'BEGIN'||CHR(10)||
    '  INSERT INTO '||LOWER(vOwner)||'.ptb_ds_fit_'||vMID||' (as_of_date,obj_gid,source_system_id,sign_name,sign_val)'||CHR(10)||
    '    VALUES(to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR''),''1'',''TEACH'','''||UPPER(inFtrName)||''','''||to_char(vA,'FM999999999999999D999999999','nls_numeric_characters='', ''')||'#!#'||to_char(vB,'FM999999999999999D999999999','nls_numeric_characters='', ''')||''');'||CHR(10)||
    '  :1 := SQL%ROWCOUNT;'||CHR(10)||
    'END;';
  END IF;
    
  --dbms_output.put_line(vBuff);
  EXECUTE IMMEDIATE vBuff USING OUT vCou;
  COMMIT;

  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: "'||UPPER(inFtrName)||'" - "'||to_char(inDate,'DD.MM.RRRR')||'" :: '||vCou||' rows inserted into table "'||LOWER(vOwner)||'.ptb_ds_fit_'||vMID||'"';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate',vMes);
  
  vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate',vMes);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: Совокупность Модель: ид = "'||vMID||'" - Фича: "'||UPPER(inFtrName)||'" не найдена в справочниках';
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate',vMes);

    vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate',vMes);
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||vA||';'||vB||' :: '||SQLERRM;
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate',vMes);

    vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFitFtrOnDate',vMes);
END DSFitFtrOnDate;

PROCEDURE DSCompressTable(inModelName IN VARCHAR2,inDate IN DATE,inTableType IN VARCHAR2 DEFAULT 'MD')
  IS
    vOwner VARCHAR2(4000) := GetVarValue('vOwner');
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'DSCOMPRESSJOB_'||tb_signs_job_id_seq.nextval;
    vBuff VARCHAR2(32700);
    vTableType VARCHAR2(30);
    vMID NUMBER;
    --
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
    vMes VARCHAR2(4000);
BEGIN
  vMes := 'START :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.DSCompressTable" started.';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSCompressTable',vMes);

  -- Сохраняем в переменную ИД модели
  SELECT ID
        ,CASE UPPER(inTableType) WHEN 'MD' THEN 'mdl' WHEN 'FIT' THEN 'fit' WHEN 'PRD' THEN 'prd' END 
    INTO vMID,vTableType 
    FROM tb_ds_mdl
    WHERE model_name = inModelName;

  vBuff :=
    q'{SELECT to_char(ROWNUM) AS ID
      ,NULL AS parent_id
      ,'}'||LOWER(vOwner)||q'{.pkg_etl_signs.MyExecute' AS unit
      ,'ALTER TABLE }'||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||q'{ MOVE SUBPARTITION SP'||f.id||'_'||}'||to_char(inDate,'RRRRMMDD')||q'{||' COMPRESS' AS params
      ,0 AS SKIP
  FROM }'||LOWER(vOwner)||q'{.tb_ds_ftrs f
       INNER JOIN }'||LOWER(vOwner)||q'{.tb_ds_mdl m
         ON m.id = f.mdl_id
            AND m.model_name = '}'||inModelName||'''';

  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.DSCompressTable" finished sucessfully in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSCompressTable',vMes);

EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.DSCompressTable" :: '||SQLERRM;
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSCompressTable',vMes);
  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||lower(vOwner)||'.pkg_etl_signs.DSCompressTable" finished with errors in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSCompressTable',vMes);
END DSCompressTable;

PROCEDURE DSFitFtrSetWeight(inDate IN DATE,inModelName IN VARCHAR2,inFtrName IN VARCHAR2)
  IS
    vOwner VARCHAR2(30) := pkg_etl_signs.GetVarValue('vOwner');
    vMID NUMBER;
    vFtrID NUMBER;
    vFtrName VARCHAR2(256);
    vTrgName VARCHAR2(256);
    vCou INTEGER := 0;
    vBuff VARCHAR2(32700);
    --vTableType VARCHAR2(30);
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight" started.';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight',vMes);
  
  -- Сохраняем в переменные ИД модели
  SELECT m.id/*,m.model_name*/,f.ftr_name,t.ftr_name,f.id
    INTO vMID--,vTableType
        ,vFtrName,vTrgName,vFtrID
    FROM tb_ds_mdl m
         INNER JOIN tb_ds_ftrs f
           ON f.mdl_id = m.id
              AND f.ftr_type = 'F'
              AND f.ftr_name = UPPER(inFtrName)
         LEFT JOIN tb_ds_ftrs t
           ON t.mdl_id = m.id
              AND t.ftr_type = 'T'
    WHERE m.model_name = inModelName;

  --vDML := 'INSERT INTO '||LOWER(vOwner)||'.ptb_ds_'||vTableType||'_'||vMID||' (as_of_date,obj_gid,source_system_id,sign_name,sign_val) '||CHR(10)||vDML||CHR(10)||';'||CHR(10)||'COMMIT';
  vBuff :=
  'DECLARE'||CHR(10)||
  '  vCou INTEGER := 0;'||CHR(10)||
  'BEGIN'||CHR(10)||
  --DSFitGetFtrSQL(inDate,UPPER(vTrgName),UPPER(inFtrName),inTopPrdCou)||CHR(10)||';'||CHR(10)||
  'FOR idx IN ('||CHR(10)||
  'SELECT /*+ leading(trg src prd)*/'||CHR(10)||
  '       '''||vFtrName||''' AS sign_name'||CHR(10)||
  '      ,SUM(CASE WHEN trg.sign_val = SUBSTR(prd.sign_val,1,INSTR(prd.sign_val,''#!#'') - 1) THEN 1 ELSE 0 END)/COUNT(1) AS sign_val'||CHR(10)||
  '  FROM '||LOWER(vOwner)||'.ptb_ds_mdl_'||vMID||' trg'||CHR(10)||
  '       INNER JOIN '||LOWER(vOwner)||'.ptb_ds_mdl_'||vMID||' src'||CHR(10)||
  '         ON src.sign_name = '''||vFtrName||''''||CHR(10)||
  '            AND src.source_system_id = ''VALID'''||CHR(10)||
  '            AND src.obj_gid = trg.obj_gid'||CHR(10)||
  '            AND src.as_of_date = to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR'')'||CHR(10)||
  '       INNER JOIN '||LOWER(vOwner)||'.ptb_ds_fit_'||vMID||' prd'||CHR(10)||
  '         ON prd.sign_name = '''||vFtrName||''''||CHR(10)||
  '            AND prd.source_system_id = ''TEACH'''||CHR(10)||
  '            AND prd.as_of_date = to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR'')'||CHR(10)||
  '            AND SUBSTR(prd.obj_gid,1,INSTR(prd.obj_gid,''#!#'') - 1) = src.sign_val'||CHR(10)||
  '  WHERE trg.sign_name = '''||vTrgName||''''||CHR(10)||
  '    AND trg.source_system_id = ''VALID'''||CHR(10)||
  '    AND trg.as_of_date = to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR'')'||CHR(10)||
  ') LOOP'||CHR(10)||
  --'  UPDATE '||LOWER(vOwner)||'.tb_ds_ftrs SET ftr_weight = idx.sign_val WHERE mdl_id = '||vMID||' AND ftr_name = '''||vFtrName||''';'||CHR(10)||
  'MERGE INTO '||LOWER(vOwner)||'.tb_ds_ftr_weight dest'||CHR(10)||
  '  USING (SELECT '||vFtrID||' AS ftr_id,to_date('''||to_char(inDate,'DD.MM.RRRR')||''',''DD.MM.RRRR'') AS dt,idx.sign_val AS ftr_weight FROM dual) src'||CHR(10)||
  '  ON (dest.ftr_id = src.ftr_id AND dest.dt = src.dt)'||CHR(10)||
  '  WHEN NOT MATCHED THEN INSERT(dest.ftr_id,dest.dt,dest.ftr_weight) VALUES(src.ftr_id,src.dt,src.ftr_weight)'||CHR(10)||
  '  WHEN MATCHED THEN UPDATE SET dest.ftr_weight = src.ftr_weight;'||CHR(10)||
  '  vCou := vCou + SQL%ROWCOUNT;'||CHR(10)||
  'END LOOP;'||CHR(10)||
  '  :1 := vCou;'||CHR(10)||
  'END;'||CHR(10);
    
  --dbms_output.put_line(vBuff);
  EXECUTE IMMEDIATE vBuff USING OUT vCou;
  COMMIT;

  vEndTime := SYSDATE;
  vMes := 'SUCCESSFULLY :: "'||UPPER(inFtrName)||'" - "'||to_char(inDate,'DD.MM.RRRR')||'" :: '||vCou||' rows merged into table "'||LOWER(vOwner)||'.tb_ds_ftr_weight"';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight',vMes);
  
  vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' successfully';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight',vMes);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: Совокупность Модель: ид = "'||vMID||'" - Фича: "'||UPPER(inFtrName)||'" не найдена в справочниках';
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight',vMes);

    vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight',vMes);
  WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||SQLERRM||CHR(10)||vBuff;
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight',vMes);

    vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFitFtrSetWeight',vMes);
END DSFitFtrSetWeight;

PROCEDURE DSMDOnDate(inDate IN DATE,inModelName IN VARCHAR2)
  IS
    vOwner VARCHAR2(30) := pkg_etl_signs.GetVarValue('vOwner');
    vMID NUMBER;
    vBuff VARCHAR2(32700);
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'LOADDSMDJOB_'||tb_signs_job_id_seq.nextval;
    --
    vMes VARCHAR2(32700);
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSMDOnDate" started.';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSMDOnDate',vMes);
  
  SELECT id INTO vMID FROM tb_ds_mdl WHERE model_name = inModelName;

  DSPrepareTable(inModelName,inDate,'MD');

  vBuff :=
  q'{SELECT ftr_name AS ID
        ,NULL AS parent_id
        ,'}'||LOWER(vOwner)||q'{.pkg_etl_signs.DSFtrOnDate' AS unit
        ,'}'||to_char(inDate,'DD.MM.RRRR')||'#!#'||inModelName||q'{#!#'||ftr_name AS params
        ,0 AS SKIP
    FROM }'||LOWER(vOwner)||q'{.tb_ds_ftrs
    WHERE mdl_id = }'||vMID;
    
  load_new(vBuff,vJobName);
  --dbms_output.put_line(vBuff);

  DSCompressTable(inModelName,inDate,'MD');

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSMDOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSMDOnDate',vMes);
EXCEPTION WHEN OTHERS THEN
  vEndTime := SYSDATE;
  vMes := 'ERROR :: '||SQLERRM;
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSMDOnDate',vMes);

  vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSMDOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSMDOnDate',vMes);
END DSMDOnDate;

PROCEDURE DSFitOnDate(inDate IN DATE,inModelName IN VARCHAR2,inTop IN INTEGER DEFAULT 3)
  IS
    vOwner VARCHAR2(30) := GetVarValue('vOwner');
    vMID NUMBER;
    vBuff VARCHAR2(32700);
    vJobName VARCHAR2(256) := UPPER(vOwner)||'.'||'LOADDSFITJOB_'||tb_signs_job_id_seq.nextval;
    vDSAlgo VARCHAR2(30);
    --
    vMes VARCHAR2(32700);
    vTIBegin DATE;
    vBegTime DATE := SYSDATE;
    vEndTime DATE;
BEGIN
  vMes := 'START :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitOnDate" started.';
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitOnDate',vMes);

  SELECT id,ds_algo INTO vMID,vDSAlgo FROM tb_ds_mdl WHERE model_name = inModelName;

  -- Обучение
  vTIBegin := SYSDATE;
  vMes := 'CONTINUE :: ------- "'||inModelName||'" - обучение --------';
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFitOnDate',vMes);

  DSPrepareTable(inModelName,inDate,'FIT');

  vBuff :=
  q'{SELECT ftr_name AS ID
        ,NULL AS parent_id
        ,'}'||LOWER(vOwner)||q'{.pkg_etl_signs.DSFitFtrOnDate' AS unit
        ,'}'||to_char(inDate,'DD.MM.RRRR')||'#!#'||inModelName||q'{#!#'||ftr_name||'#!#}'||inTop||q'{' AS params
        ,0 AS SKIP
    FROM }'||LOWER(vOwner)||q'{.tb_ds_ftrs
    WHERE mdl_id = }'||vMID||'
      AND ftr_type IN ('||CASE WHEN vDSAlgo IN ('CLASS_TE') THEN '''F''' ELSE '''F'',''T'''END||')';
    
  load_new(vBuff,vJobName);

  DSCompressTable(inModelName,inDate,'FIT');
  
  vEndTime := SYSDATE;
  vMes := 'CONTINUE :: ------- "'||inModelName||'" - окончание обучения. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFitOnDate',vMes);

  IF vDSAlgo = 'CLASS_TE' THEN
    -- Установка весов фичей
    vTIBegin := SYSDATE;
    vMes := 'CONTINUE :: ------- "'||inModelName||'" - установка весов фичей --------';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFitOnDate',vMes);
    
    vBuff :=
    q'{SELECT ftr_name AS ID
          ,NULL AS parent_id
          ,'}'||LOWER(vOwner)||q'{.pkg_etl_signs.DSFitFtrSetWeight' AS unit
          ,'}'||to_char(inDate,'DD.MM.RRRR')||'#!#'||inModelName||q'{#!#'||ftr_name AS params
          ,0 AS SKIP
      FROM }'||LOWER(vOwner)||q'{.tb_ds_ftrs
      WHERE mdl_id = }'||vMID||'
        AND ftr_type = ''F''';
    
    load_new(vBuff,UPPER(vOwner)||'.LOADDSFITWEIGHTSJOB_'||tb_signs_job_id_seq.nextval);
    
    vEndTime := SYSDATE;
    vMes := 'CONTINUE :: ------- "'||inModelName||'" - окончание установки весов фичей. Время выполнения - '||get_ti_as_hms(vEndTime - vTIBegin);
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFitOnDate',vMes);
  END IF;
  

  vEndTime := SYSDATE;
  vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime);
  pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFitOnDate',vMes);
EXCEPTION WHEN OTHERS THEN
    vEndTime := SYSDATE;
    vMes := 'ERROR :: '||SQLERRM;
    pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSFitOnDate',vMes);

    vMes := 'FINISH :: Procedure "'||LOWER(vOwner)||'.pkg_etl_signs.DSFitOnDate" finished in '||get_ti_as_hms(vEndTime - vBegTime)||' with errors';
    pr_log_write(lower(vOwner)||'.pkg_etl_signs.DSFitOnDate',vMes);
END DSFitOnDate;

FUNCTION DSRegrGetData(inSQL CLOB) RETURN TTabRegrData PIPELINED
  IS
    rec TRecRegrData;
    cur INTEGER;       
    ret INTEGER;
BEGIN
  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur, inSQL, dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.X);
  dbms_sql.define_column(cur,2,rec.Y);
  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.X);
    dbms_sql.column_value(cur,2,rec.Y);
    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);
END DSRegrGetData;

FUNCTION DSRegrGetAvg(inSQL IN VARCHAR2,inDeep IN INTEGER,inIter IN INTEGER DEFAULT 2) RETURN TTabRegrData PIPELINED
  IS
    vOwner VARCHAR2(256) := GetVarValue('vOwner');
  --количество значений (глубина) в каждую сторону, но не более 10 при вычислении скользящего среднего
    rec TRecRegrData;
    cur INTEGER;       
    ret INTEGER;
    vSQL CLOB;
    vLAG VARCHAR2(16000);
    vLEAD VARCHAR2(16000);
    vUNPLAG VARCHAR2(16000);
    vUNPLEAD VARCHAR2(16000);
    vMes VARCHAR2(32700);
BEGIN
  FOR idx IN 1..inDeep LOOP
    vLAG := vLAG||CHR(10)||',LAG(y,'||idx||') OVER (ORDER BY x) AS prev'||idx;
    vLEAD := vLEAD||CHR(10)||',LEAD(y,'||idx||') OVER (ORDER BY x) AS next'||idx;
    vUNPLAG := vUNPLAG||',prev'||idx;
    vUNPLEAD := vUNPLEAD||',next'||idx;
  END LOOP;
  
  FOR idx IN 1..inIter LOOP
    vSQL := vSQL||'WITH
    n AS (SELECT '||inDeep||' AS n FROM dual)
    SELECT x,AVG(val) AS y
      FROM (
        SELECT x
              ,col_name
              ,REGEXP_SUBSTR(col_name,''[[:alpha:]]+'') AS col_type
              ,CASE WHEN REGEXP_SUBSTR(col_name,''[[:alpha:]]+'') = ''PREV'' THEN  -to_number(REGEXP_SUBSTR(col_name,''[[:digit:]]+''))
                 ELSE to_number(REGEXP_SUBSTR(col_name,''[[:digit:]]+'')) END AS rn
              ,val
              ,y
          FROM (
            SELECT x
                  ,y
                  ,y AS n0'||vLAG||vLEAD||CHR(10)||' FROM (';
  END LOOP;
  vSQL := vSQL||CHR(10)||inSQL||CHR(10);
  
  FOR idx IN 1..inIter LOOP
    vSQL := vSQL||')
        ) UNPIVOT (val FOR col_name IN (n0'||vUNPLAG||vUNPLEAD||'))
      ) a 
        INNER JOIN n
          ON n.n >= ABS(a.rn)
    GROUP BY x,y';
  END LOOP;
  
  vSQL := vSQL||CHR(10)||' ORDER BY x';
  --dbms_output.put_line(vSQL);
  
  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur, vSQL, dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.X);
  dbms_sql.define_column(cur,2,rec.Y);
  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.X);
    dbms_sql.column_value(cur,2,rec.Y);
    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);
EXCEPTION WHEN OTHERS THEN
  vMes := 'ERROR :: '||SQLERRM/*||CHR(10)||'-----------'||CHR(10)||'vSQL = '||vSQL*/;
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.DSRegrGetAvg',vMes);
END DSRegrGetAvg;

FUNCTION DSRegrLTRGetData(inSQL CLOB) RETURN TTabRegrData PIPELINED
  IS
    rec TRecRegrData;
    cur INTEGER;       
    ret INTEGER;
    vSQL CLOB;
BEGIN
  vSQL := 'SELECT /*+ materialize */
                  x
                 ,REGR_INTERCEPT( y, x ) OVER () + x * REGR_SLOPE( y, x ) OVER () AS y
             FROM ('||CHR(10)||inSQL||CHR(10)||')';

  cur := dbms_sql.open_cursor;
  dbms_sql.parse(cur, vSQL, dbms_sql.native);
  dbms_sql.define_column(cur,1,rec.X);
  dbms_sql.define_column(cur,2,rec.Y);
  ret := dbms_sql.execute(cur);
  LOOP
    EXIT WHEN dbms_sql.fetch_rows(cur) = 0;
    dbms_sql.column_value(cur,1,rec.X);
    dbms_sql.column_value(cur,2,rec.Y);
    PIPE ROW(rec);
  END LOOP;
  dbms_sql.close_cursor(cur);
END DSRegrLTRGetData;

FUNCTION DSRegrLTRGetKoef(inSQL IN CLOB,inDeep IN INTEGER DEFAULT 3,inIter IN INTEGER DEFAULT 3) RETURN TrecRegrData DETERMINISTIC
 IS
   rec TrecRegrData;
BEGIN
  SELECT AVG((y-prev_y)/(x-prev_x)) AS a,AVG(prev_y-(y-prev_y)/(x-prev_x)*prev_x) AS b
    INTO rec.x,rec.y
    FROM (
      SELECT x,trn AS y,LAG(x) OVER (ORDER BY x) AS prev_x,LAG(trn) OVER (ORDER BY x) AS prev_y FROM (
      SELECT x,trn FROM (
          SELECT trn.x,t.y,trn.y AS trn,MAX(trn.x) OVER () AS max_x 
            FROM TABLE(DSRegrGetAvg(inSQL,inDeep,inIter)) trn
                 LEFT JOIN TABLE(DSRegrGetData(inSQL)) t ON t.x = trn.x
        ) WHERE CASE WHEN x BETWEEN inDeep+1 AND max_x - inDeep THEN trn ELSE NULL END IS NOT NULL)
  ) WHERE prev_x IS NOT NULL;
  RETURN rec;
END DSRegrLTRGetKoef;

PROCEDURE DSRegrLTRGetKoef(inSQL IN CLOB,outM OUT NUMBER,outB OUT NUMBER)
 IS
BEGIN
        SELECT AVG(m) AS m
            ,AVG(b) AS b
        INTO outM,outB
        FROM (
          SELECT (LAG(y) OVER (ORDER BY x) - y)/(LAG(x) OVER (ORDER BY x) - x) AS m
                ,LAG(y) OVER (ORDER BY x) - (LAG(y) OVER (ORDER BY x) - y)/(LAG(x) OVER (ORDER BY x) - x)*LAG(x) OVER (ORDER BY x) AS b
            FROM (
              SELECT /*+ materialize */
                     x
                    ,REGR_INTERCEPT( y, x ) OVER () + x * REGR_SLOPE( y, x ) OVER () AS y
                FROM TABLE(DSRegrGetData(inSQL))
        ))
;
END DSRegrLTRGetKoef;

FUNCTION DSRegrLTRPredict(inSQL IN CLOB,inX IN NUMBER,inDeep IN INTEGER DEFAULT 3,inIter IN INTEGER DEFAULT 3) RETURN NUMBER
  IS
    vRes NUMBER;
BEGIN
  vRes := DSRegrLTRGetKoef(inSQL,inDeep,inIter).x*inX + DSRegrLTRGetKoef(inSQL,inDeep,inIter).y;
  RETURN vRes;  
END DSRegrLTRPredict;

FUNCTION RegistryCreateObjDDL(inTableName IN VARCHAR2,inTableComment IN VARCHAR2,inCreatingPart IN NUMBER DEFAULT 0) RETURN VARCHAR2
  --inCreatingPart: 0 - создание таблиц,последовательностей,первичных ключей,индексов,триггеров; 1 - создание внешних ключей
  IS
    vOwner VARCHAR2(256) := GetVarValue('vOwner');
    vRes VARCHAR2(32700);
    --
    vBuff VARCHAR2(32700);
    vFirst INTEGER := 0;
    vSeqName VARCHAR2(256) := NULL;
    vLUPDField BOOLEAN := FALSE;
BEGIN
  IF inCreatingPart = 0 THEN
    vBuff := 'BEGIN';
    vBuff := vBuff||CHR(10)||'EXECUTE IMMEDIATE q''{CREATE TABLE '||inTableName||' (';
    FOR idx IN (
      SELECT field_name,field_type,data_length,default_value,not_null,sequence_name
        FROM (
          SELECT f.ord,f.field_name,f.field_type,f.sequence_name,o.opt_name,o.opt_val
            FROM tb_tables_registry t
                 INNER JOIN tb_tfield_registry f
                   ON f.table_id = t.id
                 LEFT JOIN tb_tfoption_registry o
                   ON o.field_id = f.id
            WHERE t.table_name = inTableName
        ) PIVOT (MAX(opt_val) FOR opt_name IN ('DATA_LENGTH' AS data_length,'DEFAULT_VALUE' AS default_value,'NOT_NULL' AS not_null))
      ORDER BY ord
    ) LOOP
      vBuff := vBuff||CHR(10)||CASE WHEN vFirst > 0 THEN ',' ELSE ' ' END||idx.field_name||' '||idx.field_type||CASE WHEN idx.data_length IS NOT NULL AND idx.field_type != 'CLOB' THEN '('||idx.data_length||')' END||CASE WHEN idx.default_value IS NOT NULL THEN ' DEFAULT '||idx.default_value END||CASE WHEN idx.not_null = '1' THEN ' NOT NULL' END;
      IF idx.sequence_name IS NOT NULL THEN
        vSeqName := idx.sequence_name;
      END IF;
      IF idx.field_name = 'LASTUPDATE' THEN vLUPDField := TRUE; END IF;
      vFirst := vFirst + 1;
    END LOOP;
    vBuff := vBuff||')}'';';
    vBuff := vBuff||CHR(10)||'EXECUTE IMMEDIATE q''{COMMENT ON TABLE '||inTableName||' IS '''||inTableComment||'''}'';';
    IF vSeqName IS NOT NULL THEN
      vBuff := vBuff||CHR(10)||'EXECUTE IMMEDIATE q''{CREATE SEQUENCE '||SUBSTR(inTableName,1,INSTR(inTableName,'.',1,1) - 1)||'.'||vSeqName||' MINVALUE 1 MAXVALUE 9999999999999999999999999 START WITH 1 NOCACHE}'';';
    END IF;
    IF vLUPDField THEN
      vBuff := vBuff||CHR(10)||'EXECUTE IMMEDIATE q''{CREATE OR REPLACE TRIGGER '||inTableName||'_LUPD_TRG BEFORE INSERT OR UPDATE ON '||inTableName||' FOR EACH ROW BEGIN :NEW.LASTUPDATE := SYSDATE; END '||SUBSTR(inTableName,INSTR(inTableName,'.',1,1) + 1,LENGTH(inTableName) - INSTR(inTableName,'.',1,1))||'_LUPD_TRG;}'';';
    END IF;
    vBuff := vBuff||CHR(10)||'END;'||CHR(10)||'/'||CHR(10);
  END IF;
  vRes := vBuff;
    
  IF inCreatingPart = 0 THEN
    vBuff := 'BEGIN';
    FOR idx IN (
      SELECT t.table_name
            ,k.key_name
            ,k.key_type
            ,k.key_fields
            ,k.fk_table
            ,k.fk_ondelete
            ,kk.key_fields AS r_fields
            ,kk.key_name AS kk_key_name
            ,pk.key_name AS pk_key_name
        FROM tb_tables_registry t
             INNER JOIN tb_tablekey_registry k
               ON k.table_id = t.id
             LEFT JOIN tb_tables_registry r
               ON r.table_name = k.fk_table
             LEFT JOIN tb_tablekey_registry kk
               ON kk.table_id = r.id AND kk.key_type = 'PRIMARY KEY'
             LEFT JOIN tb_tablekey_registry pk
               ON pk.table_id = t.id AND k.key_type = 'UNIQUE INDEX' AND pk.key_type = 'PRIMARY KEY' AND pk.key_name = SUBSTR(k.key_name,INSTR(k.key_name,'.',1,1) + 1,LENGTH(k.key_name) - INSTR(k.key_name,'.',1,1))
        WHERE t.table_name = inTableName
          AND k.key_type != 'FOREIGN KEY'
    ) LOOP
        IF idx.key_type = 'PRIMARY KEY' THEN
          vBuff := vBuff||CHR(10)||'EXECUTE IMMEDIATE q''{CREATE OR REPLACE TRIGGER '||inTableName||'_ID_TRG BEFORE INSERT ON '||inTableName||' FOR EACH ROW BEGIN :NEW.'||idx.key_fields||' := '||SUBSTR(inTableName,1,INSTR(inTableName,'.',1,1) - 1)||'.'||vSeqName||'.NEXTVAL; END '||SUBSTR(inTableName,INSTR(inTableName,'.',1,1) + 1,LENGTH(inTableName) - INSTR(inTableName,'.',1,1))||'_ID_TRG;}'';';
        END IF;
        IF idx.pk_key_name IS NULL THEN
          vBuff := vBuff||CHR(10)||CASE WHEN idx.key_type LIKE '%INDEX' THEN 'EXECUTE IMMEDIATE q''{CREATE '||idx.key_type||' '||idx.key_name||' ON '||idx.table_name||' ('||idx.key_fields||') STORAGE(INITIAL 64K NEXT 1M)}'';'
                                   ELSE 'EXECUTE IMMEDIATE q''{ALTER TABLE '||idx.table_name||' ADD CONSTRAINT '||idx.key_name||' '||idx.key_type||' ('||idx.key_fields||')'||CASE WHEN idx.key_type = 'FOREIGN KEY' THEN ' REFERENCES '||idx.fk_table||' ('||idx.r_fields||')'||CASE WHEN idx.fk_ondelete = 'CASCADE' THEN ' ON DELETE '||idx.fk_ondelete END||'}'';' ELSE '}'';' END END;
        END IF;
    END LOOP;
    vBuff := vBuff||CHR(10)||'END;'||CHR(10)||'/'||CHR(10);
  ELSIF inCreatingPart = 1 THEN 
    vBuff := 'BEGIN'||CHR(10)||'  NULL;';
    FOR idx IN (
      SELECT t.table_name
            ,k.key_name
            ,k.key_type
            ,k.key_fields
            ,k.fk_table
            ,k.fk_ondelete
            ,kk.key_fields AS r_fields
            ,kk.key_name AS kk_key_name
            ,pk.key_name AS pk_key_name
        FROM tb_tables_registry t
             INNER JOIN tb_tablekey_registry k
               ON k.table_id = t.id
             LEFT JOIN tb_tables_registry r
               ON r.table_name = k.fk_table
             LEFT JOIN tb_tablekey_registry kk
               ON kk.table_id = r.id AND kk.key_type = 'PRIMARY KEY'
             LEFT JOIN tb_tablekey_registry pk
               ON pk.table_id = t.id AND k.key_type = 'UNIQUE INDEX' AND pk.key_type = 'PRIMARY KEY' AND pk.key_name = SUBSTR(k.key_name,INSTR(k.key_name,'.',1,1) + 1,LENGTH(k.key_name) - INSTR(k.key_name,'.',1,1))
        WHERE t.table_name = inTableName
          AND k.key_type = 'FOREIGN KEY'
    ) LOOP
        IF idx.pk_key_name IS NULL THEN
          vBuff := vBuff||CHR(10)||CASE WHEN idx.key_type LIKE '%INDEX' THEN 'EXECUTE IMMEDIATE q''{CREATE '||idx.key_type||' '||idx.key_name||' ON '||idx.table_name||' ('||idx.key_fields||') STORAGE(INITIAL 64K NEXT 1M)}'';'
                                   ELSE 'EXECUTE IMMEDIATE q''{ALTER TABLE '||idx.table_name||' ADD CONSTRAINT '||idx.key_name||' '||idx.key_type||' ('||idx.key_fields||')'||CASE WHEN idx.key_type = 'FOREIGN KEY' THEN ' REFERENCES '||idx.fk_table||' ('||idx.r_fields||')'||CASE WHEN idx.fk_ondelete = 'CASCADE' THEN ' ON DELETE '||idx.fk_ondelete END||'}'';' ELSE '}'';' END END;
        END IF;
    END LOOP;
    vBuff := vBuff||CHR(10)||'END;'||CHR(10)||'/'||CHR(10);
  END IF; 
  vRes := vRes||vBuff;
  
  RETURN vRes;
EXCEPTION WHEN OTHERS THEN
  vBuff := 'ERROR :: Ошибка при создании таблицы '||inTableName||' :: '||SQLERRM||' :: '||vBuff;
  pr_log_write(LOWER(vOwner)||'.pkg_etl_signs.RegistryCreateObjSQL',vBuff);
  RETURN NULL;
END RegistryCreateObjDDL;

END pkg_etl_signs;
/
