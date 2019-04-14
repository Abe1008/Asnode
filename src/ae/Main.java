/*
 * Copyright (c) 2017. Aleksey Eremin
 * 11.10.17 15:20
 */

package ae;

/*
 Отслеживание задачи ноды
 Считывает данные о задачах из АС Ревизор
 И пишет письмо, когда очередная задача завершается.
 */

import java.util.Date;

import static ae.R.WorkDB;
import static ae.R.loadDefault;

public class Main {

  public static void main(String[] args)
  {
    long dt;
    // write your code here
    System.out.println("Отслеживаем задачи ноды из АС Ревизор. " + R.Ver );
    //
    // первый аргумент: имя файла БД
    // второй аргумент: [nnn[,mmm]] номер(а) ноды
    if(args.length > 0) {
      // задан файл рабочей базы данных
      R.WorkDB = args[0];
      if(args.length >1) {
        R.Nodes = args[1]; // номера нод
      }
    }
    //
    System.out.println("База данных: " + WorkDB);
    Date dat = new Date();
    System.out.println(dat.toString());
    long t1 = dat.getTime();       // время старт программы в мсек UNIX epoch
    //
    // обработка алгоритма программы
    // прочитать из БД значения часов выдержки
    loadDefault();
    //
    // загрузим новую порцию данных из портала
    // агенты
    //LoadData la = new LoadLens(R.db);
    // a = la.load();          //  загрузить данные по агентам
    //
    LoadTasks lt = new LoadTasks(R.db);
    lt.load();   // загрузить данные о задачах проверки

    MyLog myLog = new MyLog(R.db);
    myLog.work();

    R.closeDb();
    //
    dat = new Date();
    System.out.println(dat.toString());
    long t2 = dat.getTime();  // время окончания программы в мсек UNIX epoch
    dt = (t2 - t1)/1000L;     // разница в секундах
    System.out.println("Время выполнения задачи: " + dt + " c");
  }

}  // end of class
