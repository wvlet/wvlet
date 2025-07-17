package wvlet.lang.api

/**
  * Test utility class for demonstration purposes
  */
object TestUtility:

  /**
    * Simple string formatting utility
    */
  def formatMessage(prefix: String, message: String): String = s"${prefix}: ${message}"

  /**
    * Check if a string is palindrome
    */
  def isPalindrome(str: String): Boolean =
    val cleaned = str.toLowerCase.replaceAll("[^a-z0-9]", "")
    cleaned == cleaned.reverse

  /**
    * Calculate factorial
    */
  def factorial(n: Int): Long =
    if n <= 0 then
      1
    else
      (1 to n).map(_.toLong).product
