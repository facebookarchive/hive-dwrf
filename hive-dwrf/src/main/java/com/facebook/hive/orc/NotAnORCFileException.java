package com.facebook.hive.orc;

public class NotAnORCFileException extends RuntimeException {
  /**
   * Default constructor
   */
  public NotAnORCFileException() {
  }

  /**
   * @param message The detailed message
   */
  public NotAnORCFileException(String message) {
    super(message);
  }

  /**
   *
   * @param cause The cause of this exception
   */
  public NotAnORCFileException(Throwable cause) {
    super(cause);
  }

  /**
   *
   * @param message The detailed message
   * @param cause   The cause of this exception
   */
  public NotAnORCFileException(String message, Throwable cause) {
    super(message, cause);
  }
}
