package main;

public enum SearchDirection {
	OUT_GOING, BOTH;

	public static final SearchDirection resolve(String name) {
		for (SearchDirection propertyDataType : SearchDirection.values()) {
			if (propertyDataType.name().equals(name)) {
				return propertyDataType;
			}
		}
		return OUT_GOING;
	}
}
