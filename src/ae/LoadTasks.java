package ae;

/*
  Загрузка задач в таблицу Tasks
  -- auto-generated definition
create table Tasks
(
  id_task        INT  primary key,
  id_task_meta   INT,
  name_task_meta VARCHAR(255),
  node_id        INT,
  agent_name     VARCHAR(255),
  result         VARCHAR(255),
  ts_create      DATETIME,
  ts_start       DATETIME,
  ts_stop        DATETIME,
  status         VARCHAR(255),
  pass           INT,
  fail           INT,
  flag           INT       default 0,
  wdat           TIMESTAMP default (DATETIME('now', 'localtime'))
);

 */

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

class LoadTasks extends LoadData
{
  LoadTasks(Database db)
  {
    super(db);
  }

  int load()
  {
    int     cnt = 0;
    int     a, c;
    String  sql;
    // найдем и пометим устаревшие задачи TasksTTL часы
    sql = "UPDATE Tasks Set flag = 1 WHERE " +
        "(strftime('%s','now','localtime')-strftime('%s', ts_create)) > 3600*" + R.TasksTTL;
    a = f_db.ExecSql(sql);
    // найдем и пометим задачи со статусом RUNNING
    sql = "UPDATE Tasks Set flag = 1 WHERE status='RUNNING'";
    a = f_db.ExecSql(sql);
    // System.out.println("Устаревших и выполняющихся задач: " + (a+b));
    // удалить из архива агенты с флагом 1
    sql = "DELETE FROM Tasks WHERE flag=1";
    a = f_db.ExecSql(sql);
    System.out.println("Удалено старых и выполняющихся задач: " + a);
    // начало запрашиваемого интервала
    final LocalDateTime dt1 = LocalDateTime.now().minusHours(R.HoursTasksBack);
    // выбираем по строке мета-задачи
    String[] metastrs;
    metastrs = R.MetaTasks.replace(',',';').split(";");
    for(String metas: metastrs) {
      String s_meta = metas.trim();
      if(s_meta.length()>1) {
        // берем по частям
        System.out.println(s_meta + ")");
        LocalDateTime dt = dt1;
        int d = R.DeltaLoad;  // интервал времени
        for (int i = R.HoursTasksBack; i > 0; i -= d) {
          R.sleep(900);
          System.out.print(dt.toString() + " ");
          if(d > i) d = i;
          c = loadPart(s_meta, R.Nodes, dt, d);  // загрузить задачи под запланированную задачу
          cnt += c;
          dt = dt.plusHours(d);   // сдвинемся вперед на di часов
        }
      }
    }

    /*
    // выбираем по номеру мета-задачи
    metastrs = R.MetaTasksID.replace(',',';').split(";");
    for(String metas: metastrs) {
      try {
        int i_meta = Integer.parseInt(metas.trim());
        if (i_meta > 0) {
          // берем по частям
          LocalDateTime dt = dt1;
          System.out.println(i_meta + ">");
          for (i = R.HoursTasksBack; i >= d; i -= d) {
            R.sleep(1200);
            System.out.print(dt.toString() + " ");
            c = this.readTasks(i_meta, null, dt, d);  // загрузить задачи под запланированную задачу
            cnt += c;
            dt = dt.plusHours(d);   // сдвинемся вперед на d часов
          }
        }
      } catch (NumberFormatException e) {
        System.out.println("?-WARNING-номер мета-задачи не число: \"" + metas + "\"");
      }
      */

    return cnt;
  }

  private int  loadPart(String meta_task, String node_spis, LocalDateTime tstart, int interval)
  {
    String sss = "id_task, id_task_meta, name_task_meta, " +
        "lens.id:node_id, lens.name:agent_name, " +
        "result.failReason:result, ts_create, ts_start, ts_stop, status, pass, fail";
    String url = "https://www.rfc-revizor.ru/lens/tasks/checktasks/table";

    LocalDateTime tstop = tstart.plusMinutes(60*interval-1);         // сдвинемся вперед
    //
    DateTimeFormatter dtpat = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");
    String st1 = tstart.format(dtpat);
    String st2 = tstop.format(dtpat);

    Map<String,String> args = new HashMap<>();  // аргументы
    args.put("filter_ts_create_from", st1);
    args.put("filter_ts_create_to",   st2);
    // args.put("filter_id_task_meta", im.toString()); // числовой параметр мета-задачи
    args.put("filter_name", meta_task);   // строковый параметр мета-задачи
    if(node_spis != null)
      args.put("filter_lens_list", node_spis);   // номер ноды
    //
    Json2Sql j2s = new Json2Sql(sss, "Tasks");
    return super.load(url, args, "results.data", j2s);
  }

//  /**
//   * Возращает время вставки последнего элемента в таблицу
//   * @return время вставки UNIX epoch (сек)
//   */
//  public long  getLastItemTime()
//  {
//    String sql = "select strftime('%s', max(wdat), 'UTC') FROM Tasks";
//    String dats = f_db.Dlookup(sql);
//    long l;
//    try {
//      l = Long.parseLong(dats);
//    } catch(NumberFormatException e) {
//      l = 0;
//    }
//    return l;
//  }

} // end of class

