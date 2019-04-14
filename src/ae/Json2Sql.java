package ae;

import org.json.JSONException;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;

/*
 Превращает JSON объект в строку для вставки в SQL
 Описание преобразования.
 Преобразование происходит на основе строки шаблона для каждого параметра, разделенных запятыми
 <название поля JSON>[:<название поля в БД>][:<тип>]
 <название поля JSON> - название поля JSON, например id_node_uniq - это имя элемента в JSON;
                        может быть вида node_info.name, значит в объекте JSON node_info берем
                        подобъект name;
                        может быть вида descr/installation_addr это означает, что есть объект
                        строка descr, в которой содержится строковое представление объекта JSON, а в нем
                        элемент installation_addr
 <название поля в БД> - название поля в БД (SqLite) если оно отличается по написанию от <название поля JSON>
 <тип>                - тип значения s Строка (по-умолчанию), i - целое число,
                        u - число секунд эпохи UNIX (преобразуется в строку вида 2018-07-02 18:00:12)

 */

class Json2Sql {
  // строка значение, разделенных запятыми в виде
  // в виде: "<название поля JSON>[:<название поля в БД>][:<тип>]"
  // <тип> s (строка по умолчанию), i (целое), u (время в формате UNIX).
  private String    tableName;  // имя таблицы куда вставляют строки
  private String[]  tipf;       // тип s-строка i-число u-число секунд UNIX
  private String[]  jsof;       // имя поля JSON
  private String[]  sqlf;       // имя поля SQL
  private int       numFld;     // кол-во полей

  Json2Sql(String mapArray, String tableName)
  {
    String[] map = mapArray.split(",", 256);
    this.tableName = tableName;
    numFld = 0;
    try {
      int nf =  map.length;
      tipf = new String[nf];
      jsof = new String[nf];
      sqlf = new String[nf];
      for(int i=0; i < nf; i++) {
        String str;
        str = map[i].trim();
        String[] mm = str.split(":", 3);
        switch (mm.length) {
          case 1:
            jsof[i] = mm[0];
            sqlf[i] = mm[0];
            tipf[i] = "s";
            break;

          case 2:
            jsof[i] = mm[0];
            sqlf[i] = mm[1];
            tipf[i] = "s";
            break;

          case 3:
            jsof[i] = mm[0];
            sqlf[i] = mm[1];
            tipf[i] = mm[2];
            break;
        }
      }
      numFld = nf;
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  /**
   * Чтение объекта JSON по заданному шаблону
   * @param json  JSON-объект
   * @return  строка SQL запроса INSERT
   */
  String  read(JSONObject json)
  {
    StringBuilder  nams = new StringBuilder(256);
    StringBuilder  vals = new StringBuilder(1024);
    String  sep = ""; // разделитель
    for(int i = 0; i < numFld; i++) {
      String val = convert(json, tipf[i], jsof[i], sqlf[i]);
      nams.append(sep).append(sqlf[i]);
      vals.append(sep).append(val);
      sep = ",";
    }
    String sql;
    sql = "INSERT INTO " + tableName + " (" + nams + ") VALUES(" + vals + ")";
    return sql;
  }

  /**
   * Считывает значение из JSON объекта согласно типа, поля JSON, поле SQL
   * @param json      объект JSON
   * @param tip       тип поля
   * @param jsonfield название поля в JSON
   * @param sqlfield  название поля в SQL
   * @return строка значения парметра, для вставки в SQL (строчный параметр обрамляется в апострофы)
   */
  String convert(JSONObject json, String tip, String jsonfield, String sqlfield)
  {
    String  sv;
    int     iv;
    String  val = "NULL";
    switch (tip) {
      case "s":
        sv = jStr(json, jsonfield);
        val = s2s(sv);
        break;

      case "i":
        iv = jInt(json, jsonfield);
        val = Integer.toString(iv);
        break;

      case "u":
        iv = jInt(json, jsonfield);
        sv = unix2datetimestr(iv);
        val = s2s(sv);
    }
    return val;
  }

  /**
   * заменяет апостроф на обратный апостров, чтобы можно было вставить в БД
   * и обрамляет строку апострофами, если не null
   * @param str     исходная строка
   * @return        строка с замененными апострофами
   */
  static String s2s(String str)
  {
    if(null == str) {
      return "null";
    }
    int l = str.length();
    if(l < 1) {
      return "null";
    }
    String s;
    s = str.substring(0,l);
    s = s.replace("'", "`");
    return "'" + s + "'";
  }

  /*
  Получим JSON объект и имя его параметра на основе входных данных
   */
  private class pairval {
    JSONObject  jo;
    String      nam;
  }

  /**
   * Получить JSON-объект и имя его парметра (возможно, что вложенного),
   * чтобы получить из него значение параметра
   * @param jobj  JSON объект
   * @param par   имя параметра в узле (или через точку подузла),
   *              если отделено "/" то первый параметр содержит строку с описанием JSON
   * @return  внутренний класс
   */
  private pairval getPairval(JSONObject jobj, String par)
  {
    if(jobj == null) return null;
    pairval pv = null;
    try {
      String[] subs = par.split("/",2);
      // указана строка-параметр, содержащая JSON-объект?
      if(subs.length > 1) {
        String str = jobj.getString(subs[0]);
        jobj = new JSONObject(str);
        par = subs[1];
      }
      String[]  nams = par.split("\\.",8);
      Object ob = jobj;
      int n = nams.length - 1;
      for(int i=0; i < n; i++) {
        ob = ((JSONObject) ob).get(nams[0]);
      }
      pv = new pairval();
      pv.jo  = (JSONObject) ob;
      pv.nam = nams[n];
    } catch (Exception e) {
      // System.out.println(par + " - не найден - " + e.getMessage());
    }
    return pv;
  }

  /**
   * возвращает строковое значение параметра par указанного объекта JSON
   * @param jobj  JSON объект
   * @param par   имя параметра в узле (или через точку подузле) если отделено "/" то первый параметр содержит строку с описанием JSON
   * @return  строковое значение или null
   */
  private String jStr(JSONObject jobj, String par)
  {
    String val = null;
    pairval pv = getPairval(jobj, par);
    if(pv != null) {
      try {
        val = pv.jo.getString(pv.nam);
      } catch (JSONException e) {
        // System.out.println(e.getMessage());
      }
    }
    return val;
  }

  /**
   * возвращает числовое значение параметра par указанного объекта JSON
   * @param jobj  JSON объект
   * @param par   имя параметра в узле если отделено "/" то первый параметр содержит строку c описанием JSON
   * @return  числовое значение
   */
  private int jInt(JSONObject jobj, String par)
  {
    int val = 0;
    pairval pv = getPairval(jobj, par);
    if(pv != null) {
      try {
        val = pv.jo.getInt(pv.nam);
      } catch (JSONException e) {
        // System.out.println(e.getMessage());
      }
    }
    return val;
  }

  /**
   * преобразовать секунды UNIX эпохи в строку даты
   * @param unix  секунды эпохи UNIX
   * @return дата и время в формате SQL (ГГГГ-ММ-ДД ЧЧ:ММ:СС)
   */
  private String unix2datetimestr(int unix)
  {
    Date date = new Date(unix*1000L);
    // format of the date
    SimpleDateFormat jdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //jdf.setTimeZone(TimeZone.getTimeZone("GMT-4"));
    return jdf.format(date);
  }

} // END OF CLASS
