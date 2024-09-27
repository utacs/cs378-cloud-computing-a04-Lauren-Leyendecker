# Please add your team members' names here. 

## Team members' names 

1. Student Name: Bersam Basagaoglu

   Student UT EID: BB42642

2. Student Name: Shreya Kappala

   Student UT EID: SK55965
3. Student Name: Lauren Leyendecker

   Student UT EID: LSL738

 ...

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
    


# Add your Project REPORT HERE 

Screenshot of our cluster
![image](https://github.com/user-attachments/assets/7f66c0d4-cf3f-4952-8eb9-08cda02cb2aa)

Screenshot of our three tasks/Hadoop jobs
![image](https://github.com/user-attachments/assets/d6c8644b-51c5-47a2-8284-a7b0ff6b9a4d)

Task 1 Output
![image](https://github.com/user-attachments/assets/27d24601-e637-4e82-829e-4acc3825631c)

Task 1 Yarn History
![image](https://github.com/user-attachments/assets/1b9ed784-439a-4f64-82f7-606fe46e2f7e)

Task 2 Output
![image](https://github.com/user-attachments/assets/315efff4-8bad-4e71-8a9b-8704f8d36d43)

Task 2 Yarn History
![image](https://github.com/user-attachments/assets/8b1a4ce6-4d01-43a1-b813-64e9e5fecd95)

Task 3 Output
![image](https://github.com/user-attachments/assets/52fa4045-2bd1-428d-8c91-71894e6392b8)

Task 3 Yarn History
![image](https://github.com/user-attachments/assets/a069f14e-ca83-48a7-8d3f-d774dcb6d440)



# Project Template

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```



## Run your application
Inside your shell with Hadoop

Running as Java Application:

```java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar SOME-Text-Fiel.txt  output``` 

Or has hadoop application

```hadoop jar your-hadoop-application.jar edu.cs.utexas.HadoopEx.WordCount arg0 arg1 ... ```



## Create a single JAR File from eclipse



Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"
