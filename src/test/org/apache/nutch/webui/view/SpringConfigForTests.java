package org.apache.nutch.webui.view;

import java.sql.SQLException;

import javax.annotation.Resource;

import org.apache.nutch.webui.NutchUiApplication;
import org.apache.wicket.util.tester.WicketTester;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.j256.ormlite.db.H2DatabaseType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;

@Configuration
@ComponentScan("org.apache.nutch.webui")
public class SpringConfigForTests {

  @Resource
  private NutchUiApplication application;

  @Bean
  public WicketTester getTester() {
    WicketTester wicketTester = new WicketTester(application);
    application.getMarkupSettings().setStripWicketTags(false);
    return wicketTester;
  }

  @Bean
  public JdbcConnectionSource getConnectionSource() throws SQLException {
    JdbcConnectionSource source = new JdbcConnectionSource("jdbc:h2:mem:",
        new H2DatabaseType());
    source.initialize();
    return source;
  }
}