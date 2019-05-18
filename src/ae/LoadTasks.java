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
import java.util.*;

class LoadTasks extends LoadData
{
  LoadTasks(Database db)
  {
    super(db);
  }

  /**
   * Загрузка задач
   * @return кол-во
   */
  int load()
  {
    int     cnt = 0;
    int     a, c;
    String  sql;
    // найдем и пометим устаревшие задачи TasksTTL часы
    sql = "UPDATE Tasks Set flag = 1 WHERE " +
        "(strftime('%s','now','localtime')-strftime('%s', ts_create)) > 3600*" + R.TasksTTL;
    f_db.ExecSql(sql);
    // найдем и пометим задачи со статусом RUNNING
    sql = "UPDATE Tasks Set flag = 1 WHERE status='RUNNING'";
    f_db.ExecSql(sql);
    // удалить из архива агенты с флагом 1
    sql = "DELETE FROM Tasks WHERE flag=1";
    a = f_db.ExecSql(sql);
    System.out.println("Удалено старых и выполняющихся задач: " + a);
    //
    String spisnodes = getSpisNodes();  // список в квадратных скобках
    if (spisnodes == null) {
      System.out.println("Не указаны ноды");
      return 0;
    }
    //
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
          c = loadPart(s_meta, spisnodes, dt, d);  // загрузить задачи под запланированную задачу
          cnt += c;
          dt = dt.plusHours(d);   // сдвинемся вперед на di часов
        }
      }
    }
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
    //
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

  /**
   * Получить строку со списком требуемых нод
   * @return строка нод в квадратных скобках или null если нет заданных нодов
   */
  private String getSpisNodes()
  {
    HashSet<Integer>    set = new HashSet<>();  // множество чисел
    ArrayList<String[]> arr = f_db.DlookupArray("SELECT nodes FROM agenda");  // список всех нод
    // наполним множество
    for(String[] rst: arr) {
      Set<Integer> si = R.strInt2set(rst[0]); // преобразовать строку с чисалми в набор чисел
      set.addAll(si);
    }
    // проверим, есть ли чего проверять?
    if(set.size() < 1)
      return null;
    //
    // добавим в массив для последующей сортировки
    ArrayList<Integer> iset = new ArrayList<>(set);
    Collections.sort(iset); // сортировать
    //
    // переведем множество в строковый список
    StringBuilder snodes = new StringBuilder();  // список отслеживаемых нод
    String sep = "";
    for(Integer i: iset) {
      snodes.append(sep).append(i); // добавим список нод
      sep = ",";
    }
    return "[" + snodes + "]";
  }

} // end of class

