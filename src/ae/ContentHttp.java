/*
 * Copyright (c) 2018. Aleksey Eremin
 * 12.03.18 22:56
 */
/*
 Получить содерижимое страницы сервера АС Ревизор
 с заданным URL и/или с аргументами POST, с авторизацией
 */
package ae;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.*;

public class ContentHttp {
  private String  baseUrl;    // "https://www.rfc-revizor.ru"; // базовый URL
  private String  userName;   // учетное имя на сайте
  private String  userPass;   // пароль на сайте
  // private int     maxBodysize = 10*1024*1024;
  private int     timeOut;      // 30000 таймаут соединений (мс)
  private String  proxyServ;    // *** "10.52.2.155"
  private int     proxyPort;    // *** 3128;
  private String  proxyUser;
  private String  proxyPass;
  private final static String OkMatches = "HTTP.+200\\D+OK";  // образец правильного ответа от сервера (строка статуса)

  public ContentHttp()
  {
    baseUrl   = R.siteRevizor;
    proxyServ = R.ProxyServer;
    proxyPort = R.ProxyPort;
    proxyUser = R.ProxyUser;
    proxyPass = R.ProxyPass;
    userName  = R.SiteUsr;
    userPass  = R.SitePwd;
    timeOut   = R.TimeOut;
    if (proxyServ != null && proxyServ.length() < 8) {
      proxyServ = null;
    }
  }

  /**
   * Прочитать web-страницу по протоколу HTTP по указателю страницы
   * @param urlQuest  URL страницы
   * @return  содержимое страницы
   */
  public String getContent(String urlQuest)
  {
    return getContent(urlQuest, null);
  }

  /**
   * Прочитать web-страницу по протоколу HTTP с аргументами POST
   * с сервера АС Ревизор, с авторизацией на сервере.
   * @param urlQuest  URL страницы
   * @param postArgs  карта аргументов (название, значение)
   * @return  содержимое страницы
   */
  public String getContent(String urlQuest, Map<String,String> postArgs)
  {
    final String user_agent = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36";
    String output = null;
     try {
      // таймаут
      BasicCookieStore cookieStore = new BasicCookieStore();
      RequestConfig reqconfig = RequestConfig.custom()
          .setConnectTimeout(timeOut)
          .setConnectionRequestTimeout(timeOut)
          .setSocketTimeout(timeOut)
          .build();

      // proxy сервер
      CredentialsProvider credsProvider = null;
      HttpHost proxy = null;
      if (proxyServ != null) {
        credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
            new AuthScope(proxyServ, proxyPort),
            new UsernamePasswordCredentials(proxyUser, proxyPass));
        proxy = new HttpHost(proxyServ, proxyPort);
      }

      ////////////////////////////////
      // проигнорировать сертификат сайта
      // http://literatejava.com/networks/ignore-ssl-certificate-errors-apache-httpclient-4-4/
      // setup a Trust Strategy that allows all certificates.
      SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, (arg0,arg1) -> true).build();

      CloseableHttpClient httpclient;
      httpclient = HttpClients.custom()
          .setUserAgent(user_agent)           // название "браузера"
          .setDefaultRequestConfig(reqconfig) // уст. таймаут
          .setDefaultCookieStore(cookieStore) // работаем с куками
          .setDefaultCredentialsProvider(credsProvider) // авторизация на прокси
          .setProxy(proxy)
          .setSSLContext(sslContext)
          .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)  // http://qaru.site/questions/44866/how-to-ignore-ssl-certificate-errors-in-apache-httpclient-40
          .build();
      try {
        CloseableHttpResponse response0;
        HttpGet httpget = new HttpGet(baseUrl);
        response0 = httpclient.execute(httpget);
        try {
          HttpEntity entity = response0.getEntity();
          EntityUtils.consume(entity);
          //System.out.println("Login to: " + response0.getStatusLine());
          // System.out.println("Initial set of cookies:");
          // List<Cookie> cookies = cookieStore.getCookies();
          // if (cookies.isEmpty()) { System.out.println("None"); }
          // else { for (int i = 0; i < cookies.size(); i++) { System.out.println("- " + cookies.get(i).toString()); } }
        } finally {
          response0.close();
        }

        CloseableHttpResponse response1;
        // передаем регистрационные данные в АС Ревизор
        HttpUriRequest login = RequestBuilder.post()
            .setUri(new URI(baseUrl))
            .addParameter("OAMAuthorizationUserName", userName)
            .addParameter("OAMAuthorizationUserPassword", userPass)
            .build();
        response1 = httpclient.execute(login);
        try {
          HttpEntity entity = response1.getEntity();
          EntityUtils.consume(entity);
          String ss = String.valueOf(response1.getStatusLine());
          if (!ss.matches(OkMatches)) {
            System.out.println("?-Error-нет ответа от сервера");
            return null;
          }
        } finally {
          response1.close();
        }
        // String str1 = EntityUtils.toString(response1.getEntity());
        // System.out.println(str1);

        List<Cookie> cookies = cookieStore.getCookies();
        if (cookies.isEmpty()) {
          System.out.println("?-Error-авторизация не прошла"); // авторизация не прошла!
          return null;
        }
        // System.out.println("Post logon cookies");
        // for (int i = 0; i < cookies.size(); i++) System.out.println("- " + cookies.get(i).toString());
        //
        Thread.sleep(200);  // задержимся на 200 мс, перед следующим запросом данных
        // формируем запрос к АС Ревизор
        CloseableHttpResponse response2;
        if (postArgs == null) {
          // нет POST параметров - GET
          HttpUriRequest zapros = RequestBuilder.get(new URI(urlQuest)).build();
          response2 = httpclient.execute(zapros);
        } else {
          // есть POST параметры
          // http://hc.apache.org/httpcomponents-client-ga/quickstart.html
          HttpPost htpo = new HttpPost(urlQuest);
          List<NameValuePair> nvps = new ArrayList<>();
          for (Map.Entry<String, String> kv : postArgs.entrySet()) {
            nvps.add(new BasicNameValuePair(kv.getKey(), kv.getValue()));
          }
          UrlEncodedFormEntity uee = new UrlEncodedFormEntity(nvps,"UTF-8");
          htpo.setEntity(uee);
          response2 = httpclient.execute(htpo);
        }
        try {
          String ss = String.valueOf(response2.getStatusLine());
          if (!ss.matches(OkMatches)) {
            System.out.println("?-Error-нет данных от сервера");
            return null;
          }
          // долгожданный ответ
          output = EntityUtils.toString(response2.getEntity());
        } finally {
          response2.close();
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
      } finally {
        httpclient.close();
      }
      //
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return output;
  }

} // end of class

