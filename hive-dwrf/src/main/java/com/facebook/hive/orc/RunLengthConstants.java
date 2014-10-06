package com.facebook.hive.orc;

public class RunLengthConstants {

  /* The minimum number of repetitions seen before the writer switches to run length encoding */
  static final int MIN_REPEAT_SIZE = 3;

  /* Max literals in a 'run' when run length is NOT used */
  static final int MAX_LITERAL_SIZE = 128;

  /* Max repetitions of a literal allowed in a 'run' which is run length encoded */
  static final int MAX_REPEAT_SIZE = 127 + MIN_REPEAT_SIZE;
}
