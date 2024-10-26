package wvlet.lang.api.v1.query

/**
  * Specify how to select queries to run
  */
enum QuerySelection:
  case Single, // Run a single query containing the cursor
    Subquery,  // Run the subquery upto the specified line
    Describe,  // Describe the subquery upto the specified line
    AllBefore, // Run all queries up to the specified line
    All        // Run all queries in the compilation unit
