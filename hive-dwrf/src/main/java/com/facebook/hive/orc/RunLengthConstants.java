package com.facebook.hive.orc;

public class RunLengthConstants {
  static final int MIN_REPEAT_SIZE = 3;
  static final int MAX_LITERAL_SIZE = 128;
  static final int MAX_REPEAT_SIZE = 127 + MIN_REPEAT_SIZE;

  static final int END_OF_BUFFER_BYTE = -1;
}
