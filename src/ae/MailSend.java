/*
 * Copyright (c) 2017. Aleksey Eremin
 * 08.02.17 17:05
 * 12.04.2019
 */

package ae;

import javax.mail.*;
import javax.mail.internet.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

/**
 * Created by ae on 08.02.2017.
 * Modify 12.04.2019
 * Отказался от Apache mail
 * Отправка почты с помощью класса mailx
 * https://javaee.github.io/javamail/
 */

public class MailSend {
  private final String mail_from        = R.SmtpSender;
  private final String smtp_server_adr  = R.SmtpServer;
  private final int    smtp_server_port = R.SmtpServerPort;
  private final String smtp_server_user = R.SmtpServerUser; // имя пользовтаеля для регистрации на почтовом сервере
  private final String smtp_server_pwd  = R.SmtpServerPwd;   // пароль для почтового сервера
  private final String addr_cc          = R.MailCC;  // адрес копии

  /**
   * Отправка почтового сообщения на адрес SmtpMailTo и если указан SmtpMailСС
   * @param adrEmail              адрес получателя
   * @param subject               тема сообщения
   * @param message               сообщение
   * @param fileNameAttachment    имя файла вложения (может быть null)
   */
  public String mailSend(String adrEmail, String subject, String message, String fileNameAttachment)
  {
    LocalDateTime dt = LocalDateTime.now();
    String sDat = dt.format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm"));

    message = message + "\r\n" + sDat+ "\r\n"; // + Дата

    Locale.setDefault(new Locale("ru", "RU"));
    //
    Properties prop = System.getProperties();
    prop.put("mail.smtp.host", smtp_server_adr);
    prop.put("mail.smtp.port", smtp_server_port);
    Authenticator authenticator;
    if(smtp_server_user != null) {
      // наличие пароля предполагает SSL протокол отправки smtp
      prop.put("mail.smtp.auth", "true");
      prop.put("mail.smtp.socketFactory.port", smtp_server_port);
      prop.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
      authenticator = new Authenticator() {
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(smtp_server_user, smtp_server_pwd);
        }
      };
    } else {
      authenticator = null; // нет аутентификации
    }
    // делаем сессию для передачи сообщения
    Session session = Session.getInstance(prop, authenticator);
    session.setDebug(R.MAILDEBUG); // true или false
    try {
      //
      MimeMessage msg = new MimeMessage(session);
      msg.setFrom(new InternetAddress(mail_from));
      // разбор строки адреса получателя на несколько адресов, разделенных ;
      if(adrEmail != null) {
        InternetAddress[] address = str2iadr(adrEmail);
        msg.setRecipients(Message.RecipientType.TO, address);
      }
      if(addr_cc != null) {
        InternetAddress[] address = str2iadr(addr_cc);
        msg.setRecipients(Message.RecipientType.CC, address);
      }
      msg.setSubject(subject, StandardCharsets.UTF_8.name());
      //msg.setText(txtmsg);
      msg.setSentDate(new Date());  // дата отправки
      // @see https://www.journaldev.com/2532/javamail-example-send-mail-in-java-smtp#javamail-example-8211-send-mail-in-java-with-attachment
      // Create a multipart message, возможно будет вложение
      Multipart multipart = new MimeMultipart();
      // создадим 1 часть с текстом
      BodyPart messageBodyPart = new MimeBodyPart();
      // Fill the message
      messageBodyPart.setText(message);
      // Set text message part, добавим часть в составное тело сообщения
      multipart.addBodyPart(messageBodyPart);
      // если задано имя файла вложения, то добавим еще одну часть к письму
      if(fileNameAttachment != null) {
        // Second part is attachment
        MimeBodyPart fileBodyPart = new MimeBodyPart();
        // этого достаточно (по примеру javamail-samples\sendfile.java)
        fileBodyPart.attachFile(fileNameAttachment);
        multipart.addBodyPart(fileBodyPart);
      }
      // Send the complete message parts
      msg.setContent(multipart);
      // отправка сообщения
      Transport.send(msg);
    } catch (Exception e) {
      //e.printStackTrace();
      System.err.println("?-Error-mailSend(): " + e.getMessage());
      return null;
    }
    return "Message sent";
  }

  /**
   * Преобразует строку с адресом (адресами, разделенными запятой или точка-запятой)
   * в массив InternetAdress
   * @param strAdr строка с адресом или адресами
   * @return массив InternetAdress
   */
  private InternetAddress[] str2iadr(String strAdr)
  {
    if(null == strAdr) return null;
    String[] aastr = strAdr.replace(',', ';').split(";");
    ArrayList<InternetAddress> arr = new ArrayList<>();
    for (String s: aastr) {
      try {
        arr.add(new InternetAddress(s));
      } catch (AddressException e) {
        System.out.println("Ошибка преобразования e-mail адреса " + s + ": " + e.getMessage());
      }
    }
    // преобразовать в массив
    // https://stackoverflow.com/questions/7969023/from-arraylist-to-array
    // https://shipilev.net/blog/2016/arrays-wisdom-ancients/
    return arr.toArray(new InternetAddress[0]);
  }

} // end of class

