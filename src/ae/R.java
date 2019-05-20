/*
 * Copyright (c) 2017. Aleksey Eremin
 * 16.03.17 21:03
 * 12.04.2019
 */

package ae;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by ae on 28.01.2017.
 * Ресурсный класс
*/
/*
Modify:
12.04.19 отсылаем письма используя только библ. mailx, убрал apache mail
15.04.19 время жизни задач TasksTTL ч, а у журнал TasksLog На 24 ч больше
15.05.19 в таблицу agenda пишем email куда писать и ноды за которыми следить
20.05.19 переход на Stream API

*/

class R {
  final static String Ver = "Ver. 1.4 20.05.2019"; // номер версии

  // рабочая БД
  static String WorkDB = "tasklog.db";   // /var/Gmir/ CentOs Linux (в Windows будет D:\var\Gmir\)
  static Database  db;

  // период (часов назад), за которое считываем задачи
  static int    HoursTasksBack  = 72;     // часы
  // временной интервал одного запроса
  static int    DeltaLoad       = 72;     // часы
  // время жизни записей о задачах (в логе на 24 дольше)
  static int    TasksTTL        = 30*24;   // часы
  // названия запланированных задач (2 и более символа), по которым будем загружать результаты (cписок через запятую)
  static String MetaTasks = _r.metatask;

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
  static String MailCC  = _r.mailcc;          // адрес получателя копии почты

  static String SmtpSender     = _r.smtpsender;       // адрес отправителя почты
  static String SmtpServer     = _r.smtpserver;       // адрес почтового сервера
  static int    SmtpServerPort = _r.smtpserverport;   // порт почтового сервера
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
    R.TasksTTL        = R.getInfo( "TasksTTL",        R.TasksTTL);        // время жизни записей о задачах (дни)


    R.MailCC          = R.getInfo( "MailCC",          R.MailCC);          // кому отсылать копии
    R.MAILDEBUG       = R.getInfo( "MAILDEBUG",       R.MAILDEBUG);       // отладка почтового сообщения

    System.out.println("MetaTasks: " + R.MetaTasks);
    System.out.println("MailCC: " + R.MailCC);
    System.out.println("HoursTasksBack(ч): " + R.HoursTasksBack +
                        "   DeltaLoad(ч): " + R.DeltaLoad +
                        "   TasksTTL(ч): " + R.TasksTTL);
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
   * @return значение ключа (логическое)
   */
  private static boolean getInfo(String keyName, boolean defaultValue)
  {
    String val = getInfo(keyName, ""+defaultValue);
    return val.compareToIgnoreCase("true") == 0;
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

  private static void openDb()
  {
    final String create_tables =
        "CREATE TABLE _Info (" +
          "key VARCHAR(32) primary key, " +
          "val VARCHAR(255) default '' " +
        ");" +

        "CREATE TABLE agenda (" +
          "email VARCHAR (255) DEFAULT ('@')," +
          "nodes VARCHAR (255) DEFAULT ('1')," +
          "prim  VARCHAR (255) DEFAULT (' ')," +
          "pk    INTEGER       PRIMARY KEY AUTOINCREMENT" +
        ");" +

        "create table Tasks (" +
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
          "dat     DATETIME," +
          "id_task int," +
          "status  varchar(255)," +
          "flag    int default 0," +
          "flag2   int" +
        ");" +

        "INSERT INTO _Info(key) VALUES('MetaTasks'); " +
        "INSERT INTO _Info(key) VALUES('MailCC'); " +
        "INSERT INTO _Info(key) VALUES('HoursTasksBack'); " +
        "INSERT INTO _Info(key) VALUES('DeltaLoad'); " +
        "INSERT INTO _Info(key) VALUES('TasksTTL'); " +
        "INSERT INTO _Info(key) VALUES('MAILDEBUG'); " +

        "";

    if(db == null) {
      db = new DatabaseSqlite(WorkDB);
      String str = db.Dlookup("SELECT COUNT(*) FROM _Info;");
      if (str == null) {
        // ошибка чтения из БД - создадим таблицу
        String[] ssql = create_tables.split(";"); // разобьем на отдельные операторы
        for (String ss: ssql)
          db.ExecSql(ss);
      }
    }
  }

  /**
   * Закрыть БД
   */
  static void closeDb()
  {
    if(db != null) {
      db.close();
      db = null;
    }
  }

  /**
   * Преобразовать строку с числами в список чисел
   * @param str строка с числами, разделенными запятыми, пробелами, точка с запятыми
   * @return набор чисел
   */
  static Set<Integer> setOfInt(String str)
  {
    Set<Integer> lst = new HashSet<>();
    try {
      // Stream API
      // @see https://annimon.com/article/2778
      lst =  Stream.of(str.split("[,; ]"))  // разделим запятыми, тчк-запятыми, пробелами
          .map(s->s.replaceAll("\\D",""))   // уберем нечисла
          .filter(x -> x.length() > 0)      // пустые строки уберем
          .map(Integer::parseInt)
          .collect(Collectors.toSet());
    } catch (NumberFormatException e) {
      System.err.println("Неверный формат числа " + e.getMessage());
    }
    return lst;
  }

  /**
   * Преобразовать строку со словами, разделенными запятой в набор со словами
   * @param str строка со словами, разделенными запятыми, пробелами, точка с запятыми
   * @return набор слов
   */
  static Set<String>  setOfStr(String str)
  {
    Set<String> set;
    // Stream API
    // @see https://annimon.com/article/2778
    set =  Stream.of(str.split("[,; ]"))
        .filter(x -> x.length() > 0)
        .collect(Collectors.toSet());
    return set;
  }

  /**
   * Соединить элементы набора чисел и вывести в строку
   * @param setInt  набор чисел
   * @return строка с числами, разделенная запятыми
   */
  static String concateInt(Set<Integer> setInt)
  {
    String s = setInt.stream()
        .sorted()
        .map(String::valueOf)
        .collect(Collectors.joining(",", "",""));
    return s;
  }

  /**
   * Соединить элементы набора слов и вывести в строку
   * @param setInt  набор слов
   * @return строка с числами, разделенная запятыми
   */
  static String concateStr(Set<String> setInt)
  {
    String s = setInt.stream()
        .collect(Collectors.joining(",", "",""));
    return s;
  }

} // end of class
