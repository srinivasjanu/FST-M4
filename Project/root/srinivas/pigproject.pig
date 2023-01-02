-- PIG: Activity1.
-- Loading the inputs from HDFS
inputFile = LOAD 'hdfs:///user/srinivas/inputs' USING PigStorage('\t') AS (name:chararray, line:chararray);

-- Filter out the first 2 lines from inputFile
ranked = RANK inputFile;
rankInput = FILTER ranked BY (rank_input > 2);

-- Group inputs to all lines by one character
grpd = GROUP rankedInput By name;

-- Count the number of lines
lineCount = FOREACH grpd GENERATE $0AS name, COUNT($1) as noOfLines;
result = ORDER LineCount BY noOfLines;

-- Delete output folder from HDFS
rmf hdfs:///user/srinivas/PigProjectOutput;

-- Save result in HDFS
STORE lineCount INTO 'hdfs:///user/srinivas/PigProjectOutput';
