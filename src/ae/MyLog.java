/*
 * Copyright (c) 2018. Aleksey Eremin
 * 02.04.18 23:34
 */
/*
 * Пишем лог изменений в Tasks
 * Пишем таблицу задач проверки Tasks и накапливание этой информации в течении TasksTTL часов
 *
 */

/*

create table TasksLog (
  dat     DATETIME,
  id_task int,
  status  varchar(255),
  flag    int default 0,
  flag2   int
);

 */
package ae;

import java.text.SimpleDateFormat;
import java.util.*;

class MyLog
{
  private Database f_db;
  private Map<Integer,String> mapNodeEmails;  // арта нода - эл.адреса

  MyLog(Database db)
  {
    this.f_db = db;
    mapNodeEmails = makeMapIntStr(); // сделать карту соотьветствия
  }

  void work()
  {
    String sql;
    int    a;
    // ------------------------------------------------------------
    // обработка алгоритма
    //
    // обнулим flag2 - флаг последней вставки
    sql = "UPDATE TasksLog SET flag2=0 WHERE flag2!=0";
    a = f_db.ExecSql(sql);
    // запишем лог задач
    String fieldsCompare = "id_task,status";    // поля сравнения
    String fieldsCopy = "id_task,status,flag";  // список полей для копирования
    writeChangeLog("TasksLog", fieldsCompare, fieldsCopy, "Tasks", "id_task", R.TasksTTL+24);
    //
    // если есть новая запись задачи, у которой нет флага 0,
    // т.е только что, добавленная запись .
    sql = "UPDATE TasksLog SET flag2=0 WHERE flag2!=0 AND " +
        "id_task NOT IN (SELECT id_task FROM TasksLog WHERE flag=0)";
    a = f_db.ExecSql(sql);
    // ------------------------------------------------------------
    // список завершившихся задач
    sql = "SELECT " +
            "l.id_task," +
            "node_id," +
            "agent_name," +
            "ts_start," +
            "ts_stop," +
            "result," +
            "l.status " +
        "FROM TasksLog as l LEFT JOIN Tasks ON l.id_task=Tasks.id_task " +
        "WHERE l.flag2=2 AND l.status!='RUNNING'";
    ArrayList<String[]> arr = f_db.DlookupArray(sql);
    int cnt = 0;
    for(String[] r: arr) {
      //r[0] номер завершившаяся задачи
      System.out.println("Задача " + r[0] + " " + r[6] + "  ");
      a = sendMail(r[0],r[1],r[2],r[3],r[4],r[5],r[6]);
      cnt += a;
    }
    System.out.println("отправлено писем: " + cnt);
  }

  /**
   * отправка почты
   * @param id_task     код задачи
   * @param node_id     код ноды
   * @param agent_name  имя агента
   * @param ts_start    время старта задачи
   * @param ts_stop     время остановки задачи
   * @param result      результат
   * @param status      статус
   * @return  0 - письмо не отправили, 1 - отправили
   */
  private int sendMail(String id_task, String node_id, String agent_name, String ts_start, String ts_stop,
                        String result, String status)
  {
    String res = (result == null)? "-": result;
    String sbj = "Завершение задачи проверки " + agent_name + " (" + status + ")";
    String msg =
         "Агент: " + agent_name + ".\r\n" +
         "Задача: " + id_task + ".\r\n" +
         "Старт: " + ts_start + ".\r\n" +
         "Завершено: " + ts_stop + ".\r\n" +
         "Результат: " + res + ".\r\n" +
         "Статус: \"" + status + "\".\r\n \r\n" +
         R.MsgSignature + "\r\n";

    // по ноде найдем нужые адреса
    String  email;
    try {
      Integer ni = Integer.parseInt(node_id);
      email = this.mapNodeEmails.get(ni);     // по номеру ноды найдем строку с адресами
    } catch (NumberFormatException e) {
      System.err.println("Номер ноды не число: " + node_id);
      return 0;
    }
    String b;
    MailSend ms = new MailSend();
    b = ms.mailSend(email, sbj, msg, null);
    if(b == null) {
     System.err.println("Ошибка отправки почты.");
     return 0;
    }
    System.out.println("Письмо отправлено: " + 1 /*R.EmailTo*/);
    return 1;
  }

  /**
   * Формирование лога изменений контролируемой таблицы в таблицу лога по изменению значений
   * в списке полей для сравнения.
   * В списках полей должно быть ключевое поле (например, node_id),
   * а поля с NULL надо указать в конце списка.
   * Таблица лога должна содержать ключевое поле (например, node_id INT) и дополнительные
   * поля dat DATETIME и flag TINYINT.
   * В таблицу лога копируются поля копирования, список должен содержать
   * ключевое поле (например, node_id) и flag.
   * @param tab             таблица лога
   * @param fieldsCompare   список полей для сравнения
   * @param fieldsCopy      список полей для копирования
   * @param checkTab        проверяемая таблица (например, Lens)
   * @param keyField        ключевое поле (например, node_id)
   * @param ttl             время жизни записи в таблице лога, часы
   */
  private void writeChangeLog(String tab, String fieldsCompare, String fieldsCopy,
                                     String checkTab, String keyField, int ttl)
  {
    int    a;
    String concat, sql, str;
    // значения flag в таблице лога tab
    // -1 - удален
    //  0 - предыдыщий
    //  1 - текущий
    //  2 - вновь добавленный (на время обработки)
    //
    // найдем в таблице лога удаленные агенты, у них поставим флаг -1
    sql = "UPDATE "+tab+" SET flag=-1 WHERE flag=1 AND "+
        keyField+" NOT IN (SELECT "+keyField+" FROM "+checkTab+")";
    a = f_db.ExecSql(sql);
    //--------------------------------------------------
    System.out.println(tab + "] удалено: " + a);
    //--------------------------------------------------
    // подготовим флаги в Lens
    a = f_db.ExecSql("UPDATE " + checkTab + " SET flag=0 WHERE flag!=0");
    // найдем новые и измененные агенты и пометим их флагом 2
    // сливаем содержимое полей, чтобы сравнивать, поля с NULL в слиянии пропадают
    concat = strconcat(fieldsCompare) ;  //"CONCAT_WS('_'," + fieldsCompare + ")"; // слияние значений полей
    // пометим флагом 2 изменившиеся агенты в табл. Lens
    sql = "UPDATE "+checkTab+" SET flag=2 WHERE "+concat+" NOT IN " +
        "(SELECT "+concat+" FROM "+tab+" WHERE flag=1)";
    a = f_db.ExecSql(sql);
    // скопируем все агенты у которых флаг 2
    sql = "INSERT INTO "+tab+"("+fieldsCopy+") SELECT "+fieldsCopy+" FROM "+checkTab+" WHERE flag=2";
    a = f_db.ExecSql(sql);
    // переведем записи с флагом не 2 (т.е. не новые) в 0, если у агента есть еще запись с флагом 2
    // выбираем ид. по таблице Lens, т.к. MySql не дает исправлять ту же таблицу
    sql = "UPDATE " + tab + " SET flag=0 WHERE flag!=2 AND "
         +keyField+" IN (SELECT "+keyField+" FROM "+checkTab+" WHERE flag=2)";
    a = f_db.ExecSql(sql);
    // текущее время компьютера
    str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
    // установим тек. время у новых записей (flag=2), и установка флага в 1
    // заполним 2 в флаг2, т.е. флаг 2 у только, что добавленных записей
    // FLAG2
    sql = "UPDATE "+tab+" SET dat='"+str+"', flag=1, flag2=2 WHERE flag=2";
    a = f_db.ExecSql(sql);
    //--------------------------------------------------
    System.out.println(tab + "] записано в  лог: " + a);
    //--------------------------------------------------
    // удалить старые записи лога, у которых flag=0
    sql = "DELETE FROM "+tab+" WHERE flag=0 AND " +
        "(strftime('%s','now','localtime')-strftime('%s', dat)) > 3600*" + ttl;
    a = f_db.ExecSql(sql);
    //--------------------------------------------------
    System.out.println(tab + "] удалено  старых: " + a);
    //--------------------------------------------------
  }

  /**
   * Аналог CONCAT_WS от MySQL: соедияет значения полей через подчеркивание для Sqlite
   * @param fldlst список полей через запятую
   * @return строка выражения
   */
  private String strconcat(String fldlst) {
    String[] ss = fldlst.split(",");
    StringBuilder out = new StringBuilder();
    String un = "";
    for (String s : ss) {
      out.append(un);
      out.append(s);
      un = "||'_'||";
    }
    return "(" + out + ")";
  }

  /**
   * сделать карту число - строка
   */
  private Map<Integer,String> makeMapIntStr()
  {
    ArrayList<String[]> arr  = f_db.DlookupArray("SELECT DISTINCT email,nodes FROM agenda");  // список всех нод
    // наберём множество соответствий "нода" - массив email
    Map<Integer,Set<String>> map = new HashMap<>();
    // запись из БД
    for(String[] rst: arr) {
      Set<String> emails = R.setOfStr(rst[0]);  // набор эл. адресов
      Set<Integer> nodes = R.setOfInt(rst[1]);  // набор номеров нод
      // электронный адрес
      for(String eml : emails) { // электронный адрес
        // номер ноды
        for(Integer i : nodes) {
          Set<String> ms;   // множество (набор) строк
          ms = map.get(i);  // есть ли уже множество строк?
          if(ms == null)
            ms = new HashSet<>(); // нет - создадим пустое множество
          ms.add(eml);    // дополним набор строкой (эл.адрес)
          map.put(i, ms); // заполняем карту для i множеством строк (эл.адреса)
        }
      }
    }
    // теперь у нас есть набор номеров и соответствующих им множества адресов
    // преобразуем его в карту номер - строка с эл. адресами
    Map<Integer,String> mis = new HashMap<>();
    // пройдемся по ключам
    for(Integer i: map.keySet()) {
      Set<String> as = map.get(i);
      String emls = R.concateStr(as); // строка с эл. адресами
      mis.put(i, emls); // будем добавлять в карту для последующего использования
    }
    return mis;
  }

} // end class
