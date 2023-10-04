# Assignment 2 - MySQL

Group: 36 \
Students: Eivind Dalholt, Vegard Henriksen and Synne Ødegaard

## Introduction
Briefly explain the task and the problems you have solved. How did you work as a group? If you used Git, a link to the repository would be nice.

The exercise was based on the Geodata GPS Trajectory dataset. First, the dataset had to be cleaned and inserted into a MySQL database, created by the group. Then, SQL queries and Python code were used to manipulate the data in order to answer questions. The answers can be found under Results.

The group met in person to work on the task. Four meetings were held. In task 1, different group members worked on different parts of the task. This was done because it seemed efficient, as the task consisted of two distinct tasks: creating the database and data cleaning. In  task 2, the questions were divided among the group members. Each member answered four questions. All group members participated in writing the report.

Git repository: xxx

## Results
Add your results from the tasks, both as text and screenshots. Short sentences are sufficient.

### Task 1: 
First ten rows from User: \
![image](https://github.com/edalholt/TDT4225/assets/69513928/6cf42e05-1ec1-4dfe-90a2-72e374beff90)

First ten rows from Activity:
![image](https://github.com/edalholt/TDT4225/assets/69513928/b671823f-53b7-4d0c-9eaa-13ac3662632a)

First ten rows from TrackPoint:
![image](https://github.com/edalholt/TDT4225/assets/69513928/03e101d4-e1af-47a8-9bce-6e8c11bfe361)

### Task 2:
(Add pictures and results, describe briefly)

Question 1: 

Question 2: 

Question 3: \
![image](https://github.com/edalholt/TDT4225/assets/69513928/57031213-8281-4c20-ad91-8ac1d17c3363)

Counted how many times each user_id appears in the Activity table. The user_id that appears the most must belong to the user with the most activities. Retrieved the 15 users with the most activities.

The results show the top 15 users with the most registered activities and how many activities they have registered.

Question 4: 

Question 5: 

Question 6: \
![image](https://github.com/edalholt/TDT4225/assets/69513928/47481399-661b-4e91-be8b-2a7ee576fc15)

Grouped user_id, start_date_time and end_date_time. Counted how many elements are in each group. If a group has more than one element, it means that the same activity has been registered multiple times. The query gave zero results, meaning that no activities were registered multiple times. 

Question 7: 

Question 8: 

Question 9: \
![image](https://github.com/edalholt/TDT4225/assets/69513928/e7915917-c6a0-402d-acae-632332b4b448)

Retrieved all altitudes, activity id’s and user id’s from Activity and TrackPoint. In a dictionary, stored every user’s gain of altitude meters. Iterated over every row from the result of the query. If a TrackPoint’s altitude was higher than the previous TrackPoint’s, and they belonged to the same activity, it meant that the user has gained altitude. Added this to the user’s sum of gained altitude meters. When this was done for all TrackPoints, found the 15 users who had gained the most altitude meters.

The results show the 15 users who have gained the most altitude meters and how many altitude meters they have gained.

Question 10: 

Question 11: 

Question 12: \
![image](https://github.com/edalholt/TDT4225/assets/69513928/ece024dd-ad66-42bb-bc21-709574a06581)
![image](https://github.com/edalholt/TDT4225/assets/69513928/4242397f-948f-4a3d-bcfb-f95b96515f28)



Used rank function described here: https://www.sqlshack.com/overview-of-sql-rank-functions/ to rank every transportation mode on how often they are used for every user. Then, the top 1 most frequently used transportation mode is chosen for every user. Users who don’t register transportation modes are not included.

The results show all users who have registered a transportation mode and their most used transportation mode, sorted on user_id.

## Discussion
In task 1, Python script was made to create the database, as suggested in the assignment sheet. The data was first cleaned in accordance with the task, and then processed into an appropriate format. The data was added to a JSON file. Then, data was read from the JSON file into three lists: one for users, one for activities and one for trackpoints. The lists for users and activities were each inserted into the database in one bulk. The list for trackpoints, however, was divided into chunks before being inserted, as the list was too large to be inserted in a single bulk.

In task 2, the questions were divided among the group members. The questions were divided in such a way that all group members got to try both easy and more difficult questions. For some questions, it was sufficient to only write a query, while other questions required both a query and Python code. Writing queries and code for large data volumes was not something any of the group members had done before, and so it was more time-consuming than what was expected. It was, however, quite educational, and it was interesting to see how important it can be to consider runtime when writing code.

The group did some things differently than what was explained in the assignment description. Activity ID is now a string instead of an integer, which was the data type suggested in the assignment sheet. This is because trackpoints need a foreign key to activity. When inserting trackpoints into the database, it is necessary to know the ID of the activity the trackpoint belongs to. This is not possible if the ID is auto generated. In order to always be able to infer the activity ID, the group opted for a solution where the activity ID is the combination of the start time and the user's ID. The user ID is a string; therefore, the activity ID has to be a string as well. This worked well, and the group did not experience any issues related to the change. A second thing that was done differently is that float was used instead of double, as Python does not have double as an in-built data type.

Some of the things the group learned:
- Batching is convenient when inserting lots of data. Batching greatly improved our data insert times.
- How to use Pandas. Pandas proved to be convenient when iterating over the results of a query.
- The division of questions resulted in all members getting to learn how to write queries and code for very large data volumes.


## Feedback
Optional - give us feedback on the task if you have any. The assignment is new this semester and we would love to improve if there were any problems.
