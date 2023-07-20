package tech.ydb.jmeter;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testbeans.gui.TypeEditor;

public abstract class AbstractYdbBeanInfoSupport extends BeanInfoSupport {

    /**
     * @param beanClass class to create bean info for
     */
    protected AbstractYdbBeanInfoSupport(Class<? extends TestBean> beanClass) {
        super(beanClass);

        createPropertyGroup("varName",
                new String[]{"dataSource" });

        createPropertyGroup("sql",
                new String[] {
                "queryType",
                "query",
                "queryArguments",
                "queryArgumentsTypes",
                "variableNames",
                "queryTimeout",
                "resultSetMaxRows",
                "resultSetHandler"
                });

        PropertyDescriptor p = property("dataSource");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("queryArguments");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("queryArgumentsTypes");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("variableNames");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("resultSetHandler");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, AbstractYdbTestElement.RS_STORE_AS_STRING);
        p.setValue(NOT_OTHER, Boolean.TRUE);
        p.setValue(TAGS,new String[]{
                AbstractYdbTestElement.RS_STORE_AS_STRING,
                AbstractYdbTestElement.RS_COUNT_RECORDS
                });

        p = property("resultVariable");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("queryTimeout");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("resultSetMaxRows");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("queryType");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, AbstractYdbTestElement.DATAQUERY);
        p.setValue(NOT_OTHER,Boolean.TRUE);
        p.setValue(TAGS,new String[]{
                AbstractYdbTestElement.DATAQUERY,
                AbstractYdbTestElement.SCANQUERY,
                AbstractYdbTestElement.SCHEMEQUERY,
                });

        p = property("txType");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, AbstractYdbTestElement.SERIALIZABLERW);
        p.setValue(NOT_OTHER,Boolean.TRUE);
        p.setValue(TAGS,new String[]{
                AbstractYdbTestElement.SERIALIZABLERW,
                AbstractYdbTestElement.ONLINERO,
                AbstractYdbTestElement.STALERO,
                AbstractYdbTestElement.SNAPSHOTRO,
                });

        p = property("query", TypeEditor.TextAreaEditor);
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p.setValue(TEXT_LANGUAGE, "sql"); 
    }
}
