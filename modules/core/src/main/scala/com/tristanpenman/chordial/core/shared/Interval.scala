package com.tristanpenman.chordial.core.shared

class Interval(begin: Long, end: Long) {
  def contains(id: Long): Boolean =
    if (begin <= end) {
      id >= begin && id < end // Interval does not wrap around
    } else {
      id >= begin || id < end // Interval is empty (begin == end) or wraps back to Long.MinValue (begin > end)
    }
}

object Interval {
  def apply(begin: Long, end: Long): Interval = new Interval(begin, end)
}
