# Assignment 2 - MySQL

Group: 36 \
Students: Eivind Dalholt, Vegard Henriksen and Synne Ødegaard

## Introduction
Briefly explain the task and the problems you have solved. How did you work as a group? If you used Git, a link to the repository would be nice.

The exercise was based on the Geodata GPS Trajectory dataset. First, the dataset had to be cleaned and inserted into a MySQL database, created by the group. Then, SQL queries and Python code were used to manipulate the data in order to answer questions. The answers can be found under Results.

The group met in person to work on the task. X meetings were held. In task 1, different group members worked on different parts of the task. This was done because it seemed efficient, as the task consisted of two distinct tasks: creating the database and data cleaning.

Git repository: xxx

## Results
Add your results from the tasks, both as text and screenshots. Short sentences are sufficient.

## Discussion
Discuss your solutions. Did you do anything differently than how it was explained in the assignment sheet, in that case why and how did that work? Were there any pain points or problems? What did you learn from this assignment?

Did differently:
- Activity ID is string instead of integer. This is because trackpoints need a foreign key to activity. We won't know what the key is supposed to be unless we have a system in which the activity id can be inferred. In our system, the activity id is the start time and user id combined. This way, we know that the id is unique and that it can be inferred. Because user id is a string, the activity id is a string too.

## Feedback
Optional - give us feedback on the task if you have any. The assignment is new this semester and we would love to improve if there were any problems.
