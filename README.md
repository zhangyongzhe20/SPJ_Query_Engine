# SPJ_Query_Engine: On Query Processing
 
 ## Goals
For this project, to experience on query processing working in a “real” system, specifically focusing on a simple SPJ (Select-Project-Join) query engine.  
  Different query execution trees have different performance results, which will provide some motivation for query optimization.  (Note that the differences in your simple query processing system will probably be minor, because you will be using “toy” data sets and an execution system that ignores many of the complex aspects of a real system.)


## Specifications

1. Implement the Block Nested Loops join

2. Implement ONE of the followings:

    1. Sort Merge join (which means you must also implement an external sort-merge algorithm)

    2. Indexed Nested Loops Join (you need to build an index)

    3. Hash Join

    4. Any other advanced join algorithms. 

3. Implement DISTINCT (the parser already supports this)

4. Implement Orderby. This should include ASC (in ascending order) or DESC (in descending order). The parser has to be changed to support this.

5. Implement ONE of the followings (the parser already supports these):

    1. Groupby

    2. Aggregate functions (MIN, MAX, COUNT, AVG)

6. Identify any bugs and/or limitations with the package provided to you. Address/Fix at least one of these.

To make changes to the parser, click [here](https://www.comp.nus.edu.sg/~tankl/cs3223/project/tutorial.htm) for a step-by-step guide.


## Experiments
You should create five tables for you to experiment with:

1. Flights(flno: integer, from: string, to: string, distance: integer, departs: time, arrives: time)
2. Aircrafts(aid: integer, aname: string, cruisingrange: integer)
3. Schedule(flno:integer, aid: integer)
4. Certified(eid: integer, aid: integer)
5. Employees(eid: integer, ename: string, salary: integer)

where the underscored attributes are the keys of the respective tables. Note that Employees relation describes pilots and other kinds of employees as well; every pilot is certified for some aircraft, and only pilots are certified to fly. The Schedule table shows the assignment of aircraft to flights. You will find a program (ConvertTxtToTBL) that converts an input text file to an output table in the package you downloaded. You should create your own data files. Since we are dealing with a large main memory system, your tables must contain sufficiently large number of records (more than 10000) to be able to see the difference in performance between the various algorithms.

1. Experiment 1, run the following joins of two relations:

Employees and Certified (via eid)
Flights and Schedule (via flno)
Schedule and Aircrafts (via aid)
Run each join using the two possible plans, under the two join algorithms. Record the time to perform each join under each join algorithm.

2. Experiment 2, suppose we would like to know the list of pilots who have been scheduled for a flight.  Should try out three different execution plans for this query (by restricting the join methods supported by the optimizer) and execute them with different join algorithms.

Experienced on query processing working in a “real” system, specifically focusing on a simple (Select-Project-Join) query engine. Implement different join algorithms such as block-nested loop (BNL) and sort-merge join. Compare different algorithms and different plans to demonstrate the query execution plan orderings can make a significant difference in a real-world system with large datasets.

## [Experimental Results](https://docs.google.com/document/d/1Djbe65YHOsSD56cEunUBW6Ow9LvOyD9BA6tzrDAW6bQ/edit?usp=sharing)
