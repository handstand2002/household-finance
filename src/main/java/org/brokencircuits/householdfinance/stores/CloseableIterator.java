package org.brokencircuits.householdfinance.stores;

import java.util.Iterator;

public interface CloseableIterator<T> extends AutoCloseable, Iterator<T> {

}
