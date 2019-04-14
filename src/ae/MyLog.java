/*
 * Copyright (c) 2018. Aleksey Eremin
 * 02.04.18 23:34
 */
/*
 * Пишем лог изменений в Tasks
 * Пишем таблицу задач проверки Tasks и накапливание этой информации в течении LogRecordTTL дней (по-умолчанию 180)
 *
 */

/*

 */
package ae;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

public class MyLog
{
  Database  db;

  MyLog(Database db)
  {
    this.db = db;
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
    a = db.ExecSql(sql);
    // запишем лог задач
    String fieldsCompare = "id_task,status";    // поля сравнения
    String fieldsCopy = "id_task,status,flag";  // список полей для копирования
    writeChangeLog("TasksLog", fieldsCompare, fieldsCopy, "Tasks", "id_task", R.LogRecordTTL);
    //
    // если есть новая запись задачи, у которой нет флага 0,
    // т.е только что, добавленная запись .
    sql = "UPDATE TasksLog SET flag2=0 WHERE flag2!=0 AND " +
        "id_task NOT IN (SELECT id_task FROM TasksLog WHERE flag=0)";
    a = db.ExecSql(sql);
    // ------------------------------------------------------------
    // список завершившихся задач
    System.out.println("---------------------------------");
    sql = "SELECT l.id_task,agent_name,ts_start,ts_stop,result,l.status " +
          "FROM TasksLog as l LEFT JOIN Tasks ON l.id_task=Tasks.id_task " +
          "WHERE l.flag2=2 AND l.status!='RUNNING'";
    ArrayList<String[]> arr = db.DlookupArray(sql);
    for(String[] r: arr) {
      //r[0] номер завершившаяся задачи
      System.out.print("Задача " + r[0] + " " + r[5] + "  ");
      sendMail(r[0],r[1],r[2],r[3],r[4],r[5]);
    }
    System.out.println("---------------------------------");
  }

  private void sendMail(String id_task,String agent_name, String ts_start, String ts_stop,
                        String result, String status)
  {
     String msg = "" +
         "Агент: " + agent_name + ".\r\n" +
         "Задача: " + id_task + ".\r\n" +
         "Старт: " + ts_start + ".\r\n" +
         "Завершено: " + ts_stop + ".\r\n" +
         "Результат: \"" + result + "\".\r\n" +
         "Статус: \"" + status + "\".\r\n" +
         "\r\n" + R.MsgSignature +
         "\r\n";
     String b;
     MailSend ms = new MailSend();
     b = ms.mailSend(R.EmailTo, "Завершение задачи проверки " + agent_name, msg, null);
     if(b != null) {
       System.out.println("Письмо отправлено: " + R.EmailTo);
     } else {
       System.out.println("Ошибка отправки почты.");
     }
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
   * @param ttl             время жизни записи в таблице лога, дни
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
    a = db.ExecSql(sql);
    //--------------------------------------------------
    System.out.println(tab + "] удалено: " + a);
    //--------------------------------------------------
    // подготовим флаги в Lens
    a = db.ExecSql("UPDATE " + checkTab + " SET flag=0 WHERE flag!=0");
    // найдем новые и измененные агенты и пометим их флагом 2
    // сливаем содержимое полей, чтобы сравнивать, поля с NULL в слиянии пропадают
    concat = strconcat(fieldsCompare) ;  //"CONCAT_WS('_'," + fieldsCompare + ")"; // слияние значений полей
    // пометим флагом 2 изменившиеся агенты в табл. Lens
    sql = "UPDATE "+checkTab+" SET flag=2 WHERE "+concat+" NOT IN " +
        "(SELECT "+concat+" FROM "+tab+" WHERE flag=1)";
    a = db.ExecSql(sql);
    // скопируем все агенты у которых флаг 2
    sql = "INSERT INTO "+tab+"("+fieldsCopy+") SELECT "+fieldsCopy+" FROM "+checkTab+" WHERE flag=2";
    a = db.ExecSql(sql);
    // переведем записи с флагом не 2 (т.е. не новые) в 0, если у агента есть еще запись с флагом 2
    // выбираем ид. по таблице Lens, т.к. MySql не дает исправлять ту же таблицу
    sql = "UPDATE " + tab + " SET flag=0 WHERE flag!=2 AND "
         +keyField+" IN (SELECT "+keyField+" FROM "+checkTab+" WHERE flag=2)";
    a = db.ExecSql(sql);
    // текущее время компьютера
    str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
    // установим тек. время у новых записей (flag=2), и установка флага в 1
    // заполним 2 в флаг2, т.е. флаг 2 у только, что добавленных записей
    // FLAG2
    sql = "UPDATE "+tab+" SET dat='"+str+"', flag=1, flag2=2 WHERE flag=2";
    a = db.ExecSql(sql);
    //--------------------------------------------------
    System.out.println(tab + "] записано в  лог: " + a);
    //--------------------------------------------------
    // удалить старые записи лога, у которых flag=0
    sql = "DELETE FROM "+tab+" WHERE flag=0 AND " +
        "(strftime('%s','now','localtime')-strftime('%s', dat)) > 3600*" + ttl;
    a = db.ExecSql(sql);
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

} // end class


