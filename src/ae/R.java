/*
 * Copyright (c) 2017. Aleksey Eremin
 * 16.03.17 21:03
 * 12.04.2019
 */

package ae;

import java.io.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by ae on 28.01.2017.
 * Ресурсный класс
*/
/*
Modify:
12.04.19 отсылаем письма используя только библ. mailx, убрал apache mail

*/

public class R {
  final static String Ver = "Ver. 1.6 15.04.2019"; // номер версии

  // рабочая БД
  static String WorkDB = "tasklog.db";   // /var/Gmir/ CentOs Linux (в Windows будет D:\var\Gmir\)
  static Database  db;

  // период (часов назад), за которое считываем задачи
  static int    HoursTasksBack  = 72;     // часы
  // временной интервал одного запроса
  static int    DeltaLoad       = 72;     // часы
  // время жизни записи в таблице лога, если ее флаг 0
  static int    LogRecordTTL    = 31*24;   // часы
  // время жизни записей о задачах
  static int    TasksTTL        = 30*24;   // часы
  // названия запланированных задач (2 и более символа), по которым будем загружать результаты (cписок через запятую)
  static String MetaTasks = _r.metatask;
  // номера отслеживаемых нод
  static String Nodes     = _r.nodes;
  // адрес отправки сообщения
  static String EmailTo = _r.emailto;

  // final static String sep = System.getProperty("file.separator"); // разделитель имени каталогов
  static String ProxyServer = _r.proxyserv;  // proxy сервер
  static int    ProxyPort   = _r.proxyport;  // порт proxy-сервера
  static int    TimeOut     = 30000;         // тайм-аут мс
  static String ProxyUser   = _r.proxyuser;
  static String ProxyPass   = _r.proxypass;
  static String siteRevizor = _r.site;     // сайт Р.
  static String SiteUsr     = _r.siteusr;  // пользователь на сайте
  static String SitePwd     = _r.sitepwd;  // пароль на сайт
  //
  // почтовые дела
  // адрес получателя почты (можно несколько с разделением по ;)
  static String SmtpMailCC     = _r.smtpmailcc;          // адрес получателя копии почты

  static String SmtpServer     = _r.smtpserver;       // адрес почтового сервера
  static int    SmtpServerPort = _r.smtpserverport;   // порт почтового сервера
  static String SmtpSender     = _r.smtpsender;       // адрес отправителя почты
  static String SmtpServerUser = _r.smtpserveruser;   // имя пользователя почтового сервера
  static String SmtpServerPwd  = _r.smtpserverpwd;    // пароль пользователя почтового сервера
  // подпись в письме
  static String MsgSignature   = _r.msgsgnature;
  // отладка почтового сообщения
  static boolean MAILDEBUG      = false;

  /**
   * Загрузить параметры по-умолчанию из БД таблицы "_Info"
   */
  static void loadDefault()
  {
    openDb();
    // прочитать из БД значения часов выдержки
    R.HoursTasksBack  = R.getInfo( "HoursTasksBack",  R.HoursTasksBack);  // период (часов назад), за которое считываем задачи
    R.DeltaLoad       = R.getInfo( "DeltaLoad",       R.DeltaLoad);       // временной интервал 1 запроса (часов назад), за которое считываем задачи
    R.MetaTasks       = R.getInfo( "MetaTasks",       R.MetaTasks);       // список мета-задач, которые будем смчитывать
    R.ProxyServer     = R.getInfo( "ProxyServer",     R.ProxyServer);     // прокси сервер
    R.ProxyPort       = R.getInfo( "ProxyPort",       R.ProxyPort);       // прокси порт
    R.ProxyUser       = R.getInfo( "ProxyUser",       R.ProxyUser);       // прокси пользователь
    R.ProxyPass       = R.getInfo( "ProxyPass",       R.ProxyPass);       // прокси пароль
    R.SiteUsr         = R.getInfo( "SiteUsr",         R.SiteUsr);         // пользователь на сайте
    R.SitePwd         = R.getInfo( "SitePwd",         R.SitePwd);         // пароль на сайте
    R.TimeOut         = R.getInfo( "TimeOut",         R.TimeOut);         // тайм-аут (мс)
    R.LogRecordTTL    = R.getInfo( "LogRecordTTL",    R.LogRecordTTL);    // время хранения записей в логах MySql (ч)
    R.TasksTTL        = R.getInfo( "TasksTTL",        R.TasksTTL);        // время жизни записей о задачах (дни)
    R.Nodes           = R.getInfo( "Nodes",           R.Nodes);           // номер(а) отслеживаемых нод
    R.EmailTo         = R.getInfo( "EmailTo",         R.EmailTo);         // адрес отправки сообщения
    R.SmtpMailCC      = R.getInfo( "SmtpMailCC",      R.SmtpMailCC);      // кому отсылать копии
    //
    String str        = R.getInfo( "MAILDEBUG",       ""+R.MAILDEBUG);       // отладка почтового сообщения
    R.MAILDEBUG = str.contentEquals("true");

    // System.out.println("HoursNotOnLine  = " + R.HoursNotOnLine);
    // System.out.println("HoursAfterEmail = " + R.HoursAfterEmail);
    // System.out.println("HoursExpLens    = " + R.HoursExpLens);
    // System.out.println("TaskQuestDelay  = " + R.TaskQuestDelay);
    // System.out.println("TaskFail        = " + R.TaskFail);
    System.out.println("MetaTasks: " + R.MetaTasks + "    Nodes: " + R.Nodes);
    System.out.println("EmailTo: " + R.EmailTo +   "    SmtpMailCC: " + R.SmtpMailCC);
    System.out.println("HoursTasksBack(ч): " + R.HoursTasksBack + "    DeltaLoad(ч): " + R.DeltaLoad);
    System.out.println("TasksTTL(ч): " + R.TasksTTL +       "    LogRecordTTL(ч): " + R.LogRecordTTL);
    // System.out.println("TimeOut (ms)    = " + R.TimeOut);
  }

  /**
     * Пауза выполнения программы
     * @param time   время задержки, мсек
     */
    static void sleep(long time)
    {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * прочитать ресурсный файл
     * by novel  http://skipy-ru.livejournal.com/5343.html
     * https://docs.oracle.com/javase/tutorial/deployment/webstart/retrievingResources.html
     * @param nameRes - имя ресурсного файла
     * @return -содержимое ресурсного файла
     */
    public String readRes(String nameRes)
    {
        String str = null;
        ByteArrayOutputStream buf = readResB(nameRes);
        if(buf != null) {
            str = buf.toString();
        }
        return str;
    }

    /**
     * Поместить ресурс в байтовый массив
     * @param nameRes - название ресурса (относительно каталога пакета)
     * @return - байтовый массив
     */
    private ByteArrayOutputStream readResB(String nameRes)
    {
        try {
            // Get current classloader
            InputStream is = getClass().getResourceAsStream(nameRes);
            if(is == null) {
                System.out.println("Not found resource: " + nameRes);
                return null;
            }
            // https://habrahabr.ru/company/luxoft/blog/278233/ п.8
            BufferedInputStream bin = new BufferedInputStream(is);
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            int len;
            byte[] buf = new byte[512];
            while((len=bin.read(buf)) != -1) {
                bout.write(buf,0,len);
            }
            return bout;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * Записать в файл текст из строки
     * @param strTxt - строка текста
     * @param fileName - имя файла
     * @return      true - записано, false - ошибка
     */
    public boolean writeStr2File(String strTxt, String fileName)
    {
        File f = new File(fileName);
        try {
            PrintWriter out = new PrintWriter(f);
            out.write(strTxt);
            out.close();
        } catch(IOException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     *  Записать в файл ресурсный файл
     * @param nameRes   имя ресурса (от корня src)
     * @param fileName  имя файла, куда записывается ресурс
     * @return  true - запись выполнена, false - ошибка
     */
    public boolean writeRes2File(String nameRes, String fileName)
    {
        boolean b = false;
        ByteArrayOutputStream buf = readResB(nameRes);
        if(buf != null) {
            try {
                FileOutputStream fout = new FileOutputStream(fileName);
                buf.writeTo(fout);
                fout.close();
                b = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return b;
    }
    
    /**
     * Загружает текстовый ресурс в заданной кодировке
     * @param name      имя ресурса
     * @param code_page кодировка, например "Cp1251"
     * @return          строка ресурса
     */
    public String getText(String name, String code_page)
    {
        StringBuilder sb = new StringBuilder();
        try {
            InputStream is = this.getClass().getResourceAsStream(name);  // Имя ресурса
            BufferedReader br = new BufferedReader(new InputStreamReader(is, code_page));
            String line;
            while ((line = br.readLine()) !=null) {
                sb.append(line);  sb.append("\n");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return sb.toString();
    }

    /**
     * Получить из таблицы _Info значение ключа, а если таблицы или ключа нет, то вернуть значение по-умолчанию
     * CREATE TABLE _Info(key text PRIMARY KEY, val text)
     * @param keyName       имя ключа
     * @param defaultValue  значение по-умолчанию
     * @return значение ключа
     */
    private static int getInfo(String keyName, int defaultValue)
    {
      String val = getInfo(keyName, ""+defaultValue);
      return Integer.parseInt(val);
    }

    /**
     * Получить из таблицы _Info значение ключа, а если таблицы или ключа нет, то вернуть значение по-умолчанию
     * CREATE TABLE _Info(key text PRIMARY KEY, val text)
     * @param keyName       имя ключа
     * @param defaultValue  значение по-умолчанию
     * @return значение ключа (действительное число)
     */
    private static double getInfo(String keyName, double defaultValue)
    {
      String val = getInfo(keyName, ""+defaultValue);
      return Double.parseDouble(val);
    }

    /**
     * Получить из таблицы _Info значение ключа, а если таблицы или ключа нет, то вернуть значение по-умолчанию
     * CREATE TABLE _Info(key text PRIMARY KEY, val text)
     * @param keyName       имя ключа
     * @param defaultValue  значение по-умолчанию
     * @return значение ключа (строка)
     */
    private static String getInfo(String keyName, String defaultValue)
    {
      String val = db.Dlookup("SELECT val FROM _Info WHERE key='" + keyName + "'");
      if(val == null || val.length() < 1) {
        return defaultValue;
      }
      return val;
    }

  /**
   * Копировать содержимое таблицы в другую аналогичную таблицу
   * @param db      база данных
   * @param tabSrc  исходная таблица
   * @param tabDst  таблица, куда записывают
   * @return  кол-во скопированных записей
   */
  static int copyTab2Tab(Database db, String tabSrc, String tabDst)
  {
    int a = 0;
    // получим набор полей
    try {
      Statement stm = db.getDbStatement();
      ResultSet rst = stm.executeQuery("SELECT * FROM " + tabSrc);
      ResultSetMetaData md = rst.getMetaData();
      int Narr = md.getColumnCount();
      StringBuilder nabor = new StringBuilder(256);
      for (int i = 1; i <= Narr; i++) {
        if(i > 1) nabor.append(",");
        nabor.append(md.getColumnName(i));
      }
      rst.close();
      // System.out.println(nabor);
      String sql;
      // синтаксис Sqlite!
      sql = "INSERT OR IGNORE INTO " + tabDst + "(" + nabor + ") SELECT " + nabor + " FROM " + tabSrc;
      a = db.ExecSql(sql);
    } catch (Exception e) {
      System.out.println("?-Error-don't copy table. " + e.getMessage());
    }
    return a;
  }

  /**
   * преобразовать секунды UNIX эпохи в строку даты
   * @param unix  секунды эпохи UNIX
   * @return дата и время в формате SQL (ГГГГ-ММ-ДД ЧЧ:ММ:СС)
   */
  public static String unix2datetimestr(int unix)
  {
    Date date = new Date(unix*1000L);
    // format of the date
    SimpleDateFormat jdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //jdf.setTimeZone(TimeZone.getTimeZone("GMT-4"));
    return jdf.format(date);
  }

  /*
   * Преобразование строки времени вида ЧЧ:ММ:СС в кол-во секунд
   * @param str   входная строка времени (0:0:2)
   * @return  кол-во секунд

  public static int hms2sec(String str)
  {
    String[] sar;
    int result = 0;
    try {
      sar = str.split(":", 3);
      int ih = Integer.parseInt(sar[0]);
      int im = Integer.parseInt(sar[1]);
      int is = Integer.parseInt(sar[2]);
      result = ih * 3600 + im * 60 + is;
    } catch (Exception e) {
      //e.printStackTrace();
      result = -1;
    }
    return result;
  }
*/

  /*

   */

  static void openDb()
  {
    final String create_tables =

        "CREATE TABLE _Info(key VARCHAR(32) primary key, val VARCHAR(255));" +
        "create table Tasks" +
        "(" +
        "id_task        INT primary key," +
        "id_task_meta   INT," +
        "name_task_meta VARCHAR(255)," +
        "node_id        INT," +
        "agent_name     VARCHAR(255)," +
        "result         VARCHAR(255)," +
        "ts_create      DATETIME," +
        "ts_start       DATETIME," +
        "ts_stop        DATETIME," +
        "status         VARCHAR(255)," +
        "pass           INT," +
        "fail           INT," +
        "flag           INT default 0," +
        "wdat           TIMESTAMP default (DATETIME('now', 'localtime'))" +
        ");" +

        "create table TasksLog (" +
        "dat DATETIME," +
        "id_task int," +
        "status varchar(255)," +
        "flag  int default 0," +
        "flag2 int" +
        ");" +

        "INSERT INTO _Info(key,val) VALUES('EmailTo',''); " +
        "INSERT INTO _Info(key,val) VALUES('MetaTasks',''); " +
        "INSERT INTO _Info(key,val) VALUES('Nodes',''); " +
        "INSERT INTO _Info(key,val) VALUES('SmtpMailCC',''); " +
        "INSERT INTO _Info(key,val) VALUES('HoursTasksBack',''); " +
        "INSERT INTO _Info(key,val) VALUES('DeltaLoad',''); " +
        "INSERT INTO _Info(key,val) VALUES('TasksTTL',''); " +
        "INSERT INTO _Info(key,val) VALUES('LogRecordTTL',''); " +
        "INSERT INTO _Info(key,val) VALUES('MAILDEBUG',''); " +
        "";
    if(db == null) {
      db = new DatabaseSqlite(WorkDB);
      //
      String str = db.Dlookup("SELECT COUNT(*) FROM _Info;");
      if (str == null) {
        // ошибка чтения из БД - создадим таблицу
        String[] ssql = create_tables.split(";"); // разобьем на отдельные операторы
        for (String ss: ssql)
          db.ExecSql(ss);
      }
    }
  }

  static void closeDb()
  {
    if(db != null) {
      db.close();
      db = null;
    }
  }

} // end of class
