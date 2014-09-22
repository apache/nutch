package org.apache.nutch.webui.pages.instances;

import org.apache.nutch.webui.model.NutchInstance;
import org.apache.nutch.webui.service.NutchInstanceService;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.markup.html.form.AjaxSubmitLink;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.PasswordTextField;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.spring.injection.annot.SpringBean;

import de.agilecoders.wicket.core.markup.html.bootstrap.common.NotificationPanel;
import de.agilecoders.wicket.core.markup.html.bootstrap.dialog.Modal;
import de.agilecoders.wicket.core.markup.html.bootstrap.form.BootstrapForm;

public class InstancePanel extends Modal {

  private BootstrapForm<NutchInstance> form;

  private NotificationPanel notificationPanel;

  @SpringBean
  private NutchInstanceService instanceService;

  public InstancePanel(String markupId) {
    super(markupId);
    header(Model.of("Instance"));

    notificationPanel = new NotificationPanel("notificationPanel");
    notificationPanel.setOutputMarkupId(true);
    add(notificationPanel);

    form = new BootstrapForm<NutchInstance>("instanceForm");
    form.add(new TextField<String>("name").setRequired(true));
    form.add(new TextField<String>("host").setRequired(true));
    form.add(new TextField<Integer>("port").setRequired(true));
    form.add(new TextField<String>("username"));
    form.add(new PasswordTextField("password").setResetPassword(false).setRequired(false));

    addButton(new AjaxSubmitLink("button", form) {
      @Override
      protected void onSubmit(AjaxRequestTarget target, Form<?> ajaxForm) {
        instanceService.saveInstance(form.getModelObject());
        target.add(this.getPage());

      }

      protected void onError(AjaxRequestTarget target, Form<?> form) {
        target.add(notificationPanel);
      };
    }.setBody(Model.of("Save")));
    add(form);
  }

  public void setModel(IModel<NutchInstance> model) {
    form.setModel(model);
  }

}
