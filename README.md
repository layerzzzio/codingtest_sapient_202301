# Publicis Sapient Technical Assessment

## Big Data Assignments:

### QUESTION -1 (General Coding)

**Description:** From the sample data given below, remove duplicates on the combination of Name and Age and print results.
- Please do not use high-level API/Framework like pandas/spark-sql, etc.
- Solve this problem by using simple data structures given in a programming language.
- Please try to optimize the solution for efficiency in terms of space and time.

**Given Dataset:**

<img width="234" alt="image" src="https://github.com/layerzzzio/codingtest_sapient_202301/assets/98493964/e2bbc0a9-0786-46e9-9139-826cefd44ea9">

### QUESTION - 2

**Description:** Given a time series data which is a clickstream of user activity is stored in any flat files, the task is to enrich the data with a session id.

**Session Definition:**
- Session expires after inactivity of 30 mins, because of inactivity no clickstream record will be generated.
- Session remains active for a total duration of 2 hours

**Steps:**
1. Load Data in any flat file format.
2. Read the data and use spark batch (pyspark/scala) to do the computation.
3. Save the results in parquet with enriched data.

> Note: Please do not use direct spark-sql.

**Given Dataset:**

<img width="645" alt="image" src="https://github.com/layerzzzio/codingtest_sapient_202301/assets/98493964/62c0c01c-ef7c-4214-a8b6-9edeb446aebe">

### QUESTION 3

**Description:** In addition to the problem statement given in question 2 assume the below scenario as well and design a schema based on it:
- Get the Number of sessions generated in a day.
- Total time spent by a user in a day.
- Total time spent by a user over a month.

**Guidelines and Instructions:**
- Design the table in any flat file format.
- Write the script to create the file.
- Load data into the file.
- Write all the queries in spark-sql.
- Think in the direction of using partitioning, bucketing, etc.
