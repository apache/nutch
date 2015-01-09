package org.apache.nutch.webui.pages.settings;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.nutch.webui.model.NutchConfig;
import org.apache.nutch.webui.pages.AbstractBasePage;
import org.apache.nutch.webui.pages.components.CpmIteratorAdapter;
import org.apache.nutch.webui.service.NutchService;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.markup.repeater.RefreshingView;
import org.apache.wicket.model.IModel;
import org.apache.wicket.spring.injection.annot.SpringBean;

public class SettingsPage extends AbstractBasePage<Void> {
  @SpringBean
  private NutchService nutchService;

  private WebMarkupContainer settingsTable;

  public SettingsPage() {
    settingsTable = new WebMarkupContainer("settingsTable");
    settingsTable.setOutputMarkupId(true);
    RefreshingView<NutchConfig> nutchConfig = new RefreshingView<NutchConfig>(
        "settings") {

      @Override
      protected Iterator<IModel<NutchConfig>> getItemModels() {
        return new CpmIteratorAdapter<NutchConfig>(
            convertNutchConfig(nutchService.getNutchConfig(getCurrentInstance()
                .getId())));
      }

      @Override
      protected void populateItem(Item<NutchConfig> item) {
        item.add(new Label("name"));
        item.add(new TextField<String>("value"));
      }
    };
    settingsTable.add(nutchConfig);
    add(settingsTable);
  }

  private List<NutchConfig> convertNutchConfig(Map<String, String> map) {
    List<NutchConfig> listNutchConfigs = new LinkedList<NutchConfig>();
    for (String key : map.keySet()) {
      NutchConfig conf = new NutchConfig();
      conf.setName(key);
      conf.setValue(map.get(key));
      listNutchConfigs.add(conf);
    }
    return listNutchConfigs;
  }
}
