package siena.core.base;

public class BaseOptions {

	public int limit;
	public int offset;
	public QueryOptionPaginateValue pageOpt;

	public BaseOptions(int pLimit, int pOffset,
			QueryOptionPaginateValue pPageOpt) {
		limit = pLimit;
		offset = pOffset;
		pageOpt = pPageOpt;
	}

}
