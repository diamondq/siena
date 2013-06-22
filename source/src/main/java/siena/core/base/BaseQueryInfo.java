package siena.core.base;

import siena.Query;

public class BaseQueryInfo<EXTCLASSINFO extends ExtClassInfo>
{
    public boolean mKeysOnly;
    public Query<?> mQuery;
    public EXTCLASSINFO mCCInfo;
}
