package ae;

/*
 Загрузка данных АС Ревизор из JSON в базу данных SQL
 */

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;

/**
 * Базовый класс загрузки данных
 */
class LoadData
{
  Database f_db;
  LoadData(Database db)
  {
    this.f_db = db;
  }

  /**
   * Обобщенная загрузка
   * @return  кол-во загруженных строк данных
   */
  int load()
  {
    return 0;
  }

  /**
   * Загружает данные JSON в БД
   * @param url URL страницы данных
   * @param par название объекта в JSON
   * @param j2s преобразователь поля JSON в строку SQL
   * @return  кол-во загруженных строк
   */
  int load(String url, String par, Json2Sql j2s)
  {
    return load(url, null, par, j2s);
  }

  /**
   * Загружает данные JSON в БД
   * @param url   URL страницы данных
   * @param args  аргументы http запроса передаваемые как POST
   * @param par   название объекта в JSON
   * @param j2s   преобразователь поля JSON в строку SQL
   * @return  кол-во загруженных строк
   */
  int load(String url, Map<String,String> args, String par, Json2Sql j2s)
  {
    JSONArray jsonArray = loadJsonArray(url, args, par);
    if(jsonArray == null) {
      return 0;
    }
    int Na = jsonArray.length();
    if(Na < 1) {
      System.out.println("?-Warning-нет данных в " + par);
      return 0;
    }
    int cnt = 0;
    for(int i=0; i < Na; i++) {
      JSONObject jobj = (JSONObject) jsonArray.get(i);
      String sql = j2s.read(jobj);
      int a = f_db.ExecSql(sql);
      if(a>0) {
        System.out.print('.');
        cnt++;
      }
    }
    System.out.println(" ");
    return cnt;
  }

  /**
   * Загружает текст из WEB в структуру данных массив JSON
   * @param url   URL страницы WEB
   * @param args  аргументы, передаваемые POST
   * @param par   название объекта в JSON, через точку можно указать вложенный объект
   * @return  структура данных массив JSON
   */
  private JSONArray  loadJsonArray(String url, Map<String,String> args, String par)
  {
    ContentHttp conth = new ContentHttp();
    String txt = conth.getContent(url, args); // загрузим
    if (txt == null) {
      System.out.println("Не могу загрузить страницу - " + url);
      return null;
    }
    txt = getJsonTxt(txt);
    //
    JSONArray jsonArray = null;
    try {
      Object jo = new JSONObject(txt);
      String[] apar = par.split("\\.", 12);
      int n = apar.length;
      for(int i = 0; i < n; i++) {
        jo = ((JSONObject) jo).get(apar[i]);
      }
      jsonArray = (JSONArray) jo;
    } catch (Exception e) {
      System.out.println("?-Error-получен неверный ответ JSON");
    }
    return jsonArray;
  }


  /**
   * Вырезать текст, содержащий JSON из строки
   * @param txt входной текст
   * @return  текст только JSON
   */
  private String getJsonTxt(String txt)
  {
    String s = null;
    int i, j;
    i = txt.indexOf("{");
    if(i >= 0) {
      j = txt.lastIndexOf('}') ; //, i+1);
      if(j > 0) {
        s = txt.substring(i, j+1);
      }
    }
    return s;
  }

}

