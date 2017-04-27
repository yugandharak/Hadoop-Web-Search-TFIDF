Yugandhara Kulkarni
[800965912]
Username - ykulkarn

Assignment - 2

Submission consists of a compressed folder - ykulkarn.zip

The folder has following files:

1. DocWordCount.java
   The program needs two argumets - Input and Output file paths.
   It reads the data in input files and stores output in the output file in the form of : WORD#####FILE_NAME	COUNT   

2. TermFrequency.java
   The program needs two argumets - Input and Output file paths.
   It reads the data in the input files and stores output in the output file in the form of : WORD#####FILE_NAME	LOG_VALUE
  
3. TFIDF.java
   The program needs three argumets - Input , Intermediate and Output file paths.
   The intermediate path stores the file with output from TermFrequency.
   The chaining requires three file paths as, output of TermFrequency is used as an input for TFIDF
   The final output is stroed in an output file in the location specified in third argument in the form of : WORD#####FILE_NAME	TDIDF_VALUE

4. Search.java
   The program needs two argumets - Input and output file paths.
   The input file for the program should be an output of the TFIDF.java program, as chaining has not been implmented.
   The program takes String as a user input and performs search operation for this input and stores output in the form of : FILENAME	TDIDF_VALUE     

5. DocWordCount.out
   The file is an output file of the program DocWordCount.java
   
6. TermFrequency.out
   The file is an output file of the program TermFrequency.java
   
7. TFIDF.out
   The file is an output file of the program TFIDF.java
  
8. query1.out
   the file is an output of the program Search.java for the user input : computer science
   
9. query2.out
   The file is an output of the program Search.java for the user input : data analysis
   
Bonus point assignment:
Rank.java
   The program needs two argumets - Input and output file paths.
   The input file for the program should be an output of the Search.java program, as chaining has not been implmented.
   The program sorts the input file in the order of term frequency and generates the output.