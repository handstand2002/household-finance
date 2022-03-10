package org.brokencircuits.householdfinance.util;

import lombok.Value;

@Value(staticConstructor = "of")
public final class Pair<K,V> {
  K key;
  V value;

  public Pair(K key, V value) {
    this.key = key;
    this.value = value;
  }

  K getLeft() {
    return key;
  }

  V getRight() {
    return value;
  }
}
