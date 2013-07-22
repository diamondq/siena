package siena.mapdb;

import siena.BaseQueryData;
import siena.core.base.BaseQueryInfo;

public class MapDBQueryInfo<T> extends BaseQueryInfo<T, MapDBClassInfo>
{
	public MapDBQueryInfo(MapDBClassInfo pCLInfo, BaseQueryData<T> pQuery, boolean pKeysOnly)
	{
		super(pCLInfo, pQuery, pKeysOnly);
	}

}
