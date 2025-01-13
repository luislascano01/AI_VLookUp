package com.sabadell.ai_vlookup;

import java.io.Serializable;
import java.util.Objects;


public class ReturnEntry implements Serializable {
	private static final long serialVersionUID = 1L;
        private final Integer index;
        private Double weight;

        public ReturnEntry(Integer index, Double weight) {
            this.index = index;
            this.weight = weight;
        }

        public Integer getIndex() {
            return index;
        }

        public Double getWeight() {
            return weight;
        }

        public void setWeight(Double weight) {
            this.weight = weight;
        }

        /**
         * Override equals and hashCode so removal by object works based on 'index'.
         * If you need multiple entries with the same 'index' but different weights,
         * you'll have to handle that logic differently (e.g., store them in a list).
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ReturnEntry other = (ReturnEntry) obj;
            return Objects.equals(this.index, other.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }
    }