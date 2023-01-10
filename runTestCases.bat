DEL /Q .\testCasesOutput
mkdir .\testCasesOutput
call mvn clean install
java -cp ./target/classes com.mapreduce.tasks.wordcount.WordCount
java -cp ./target/classes com.mapreduce.tasks.wordlength.WordLength
java -cp ./target/classes com.mapreduce.tasks.movieprofile.MovieProfile
python outputComparison.py
pause