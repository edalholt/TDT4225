# Assignment 2 - MySQL

Group: 36 \
Students: Eivind Dalholt, Vegard Henriksen and Synne Ødegaard

## Introduction
Briefly explain the task and the problems you have solved. How did you work as a group? If you used Git, a link to the repository would be nice.

The exercise was based on the Geodata GPS Trajectory dataset. First, the dataset had to be cleaned and inserted into a MySQL database, created by the group. Then, SQL queries and Python code were used to manipulate the data in order to answer questions. The answers can be found under Results.

The group met in person to work on the task. X meetings were held. In task 1, different group members worked on different parts of the task. This was done because it seemed efficient, as the task consisted of two distinct tasks: creating the database and data cleaning. In  task 2, the questions were divided among the group members. Each member answered four questions. All group members participated in writing the report.

Git repository: xxx

## Results
Add your results from the tasks, both as text and screenshots. Short sentences are sufficient.

### Task 1: 
showing top 10 rows from all of your tables is sufficient

### Task 2:
(Add pictures of code and results, describe briefly)

Question 1:

Question 2:

Question 3:
Count how many times each user_id appears in the Activity table. The user_id that appears the most must belong to the user with the most activities. Retrieve the 15 users with the most activities.

Question 4:

Question 5:

Question 6:

Question 7:

Question 8:

Question 9:

Question 10:

Question 11:

Question 12:
Used rank function described here: https://www.sqlshack.com/overview-of-sql-rank-functions/ to rank every transportation mode on how often they are used for every user. Then, the top 1 most frequently used transportation mode is chosen for every user. Users who don’t register transportation modes are not included.

## Discussion
Discuss your solutions. Did you do anything differently than how it was explained in the assignment sheet, in that case why and how did that work? Were there any pain points or problems? What did you learn from this assignment?

### Solution to task 1:
A Python script was made to create the database, as suggested in the assignment sheet. The data was first cleaned in accordance with the task, and then processed into an appropriate format. The data was added to a JSON file. Then, data was read from the JSON file into three lists: one for users, one for activities and one for trackpoints. The lists for users and activities were each inserted into the database in one bulk. The list for trackpoints, however, was divided into chunks before being inserted, as the list was too large to be inserted in a single bulk.

### Solution to task 2:
-

Did differently:
- Activity ID is string instead of an integer (which was suggested in the assignment sheet). This is because trackpoints need a foreign key to activity. When inserting trackpoints into the database, it is necessary to know the ID of the activity the trackpoint belongs to. This is not possible if the ID is auto generated. In order to always be able to infer the activity ID, the group opted for a solution where the activity ID is the combination of the start time and the user's ID. The user ID is a string; therefore, the activity ID has to be a string as well.
- Float is used instead of double, as Python does not have double as an in-built data type.


## Feedback
Optional - give us feedback on the task if you have any. The assignment is new this semester and we would love to improve if there were any problems.
