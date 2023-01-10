# if [[ "$OSTYPE" == "linux-gnu"* ]]; then
#   find -name "*.java" > sources.txt
#   javac @sources.txt
# else
#   javac src/**/*.java
# fi
# mvn -f ./pom.xml clean install -U
# mvn clean
pwd
rm -rf testCasesOutput
mkdir testCasesOutput
mvn clean install
java -cp ./target/classes com.mapreduce.tasks.wordcount.WordCount
java -cp ./target/classes com.mapreduce.tasks.wordlength.WordLength
java -cp ./target/classes com.mapreduce.tasks.movieprofile.MovieProfile
python3 outputComparison.py