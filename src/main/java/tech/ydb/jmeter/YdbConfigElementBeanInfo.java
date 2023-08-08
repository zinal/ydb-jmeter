package tech.ydb.jmeter;

import java.util.Set;
import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jmeter.testbeans.gui.TypeEditor;

public class YdbConfigElementBeanInfo extends BeanInfoSupport {
    public YdbConfigElementBeanInfo() {
        super(YdbConfigElement.class);

        createPropertyGroup("varName", new String[] { "dataSource" });

        createPropertyGroup("database", new String[] { "endpoint", "database", 
            "tlsCertFile", "poolMax", "retriesMax" });

        createPropertyGroup("auth", new String[] { "authMode", "username", "password", "saKeyFile" });

        PropertyDescriptor p = property("dataSource");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("endpoint");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p = property("database");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("tlsCertFile", TypeEditor.FileEditor);
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("poolMax");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "-1");

        p = property("retriesMax");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "-1");

        p = property("authMode");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, YdbConfigElement.AuthMode.STATIC.name());
        p.setValue(TAGS,new String[]{
                YdbConfigElement.AuthMode.STATIC.name(),
                YdbConfigElement.AuthMode.SAKEY.name(),
                YdbConfigElement.AuthMode.NONE.name(),
                YdbConfigElement.AuthMode.ENV.name(),
                YdbConfigElement.AuthMode.METADATA.name(),
                });

        p = property("saKeyFile", TypeEditor.FileEditor);
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("username");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p = property("password", TypeEditor.PasswordEditor);
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
    }
}
