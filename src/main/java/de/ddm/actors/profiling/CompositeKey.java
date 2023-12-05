package de.ddm.actors.profiling;

import java.util.Objects;

public class CompositeKey {
    private final String key1;
    private final String key2;


    public CompositeKey(String key1, String key2) {
        this.key1 = key1;
        this.key2 = key2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeKey that = (CompositeKey) o;
        return Objects.equals(key1, that.key1) && Objects.equals(key2, that.key2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key1, key2);
    }

    public String getSubKey1() {
    	return this.key1;
    }

    public String getSubKey2() {
    	return this.key2;
    }
}

