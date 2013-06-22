package siena.core.base;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import siena.ClassInfo;

public class ExtClassInfo
{
    public ClassInfo mSienaInfo;
    public Map<String, Field> mColumnNameToFieldMap = new HashMap<String, Field>();
    public Map<Field, String> mFieldToColumnNameMap = new HashMap<Field, String>();

    public ExtClassInfo(Class<?> pClass)
    {
        mSienaInfo = ClassInfo.getClassInfo(pClass);

        /* Calculate the field to column names */

        for (Field field : mSienaInfo.allFields)
        {
            String[] columnNames = ClassInfo.getColumnNames(field);
            for (String columnName : columnNames)
            {
                mColumnNameToFieldMap.put(columnName, field);
                mFieldToColumnNameMap.put(field, columnName);
            }
        }
    }
}
