package siena.core.base;

import siena.core.options.QueryOption;

public class QueryOptionPaginateValue extends QueryOption {

	public static final int ID = 0x5001;

	public int value = 0;

	public boolean isAtEnd = false;

	public QueryOptionPaginateValue(int option, State active, int value) {
		super(option, active, value);
	}

	@Override
	public QueryOption clone() {
		return new QueryOptionPaginateValue(type, state, value);
	}

}
