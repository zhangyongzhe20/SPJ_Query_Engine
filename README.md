# cs3223-project

Follows the iterator model described in lecture.

Summary of changes made:

Stephen:
1. Implemented BlockNestedLoopsJoin in BlockNestedJoin.java
   - Introduced a Block class in Block.java to keep BlockNestedJoin's implementation as similar to given NestedJoin code
    - Added BNLJ formula to PlanCost.java
    - Modified RandomOptimizer.java to use BNLJ
2. Modified parser to support orderby
3. Implemented external sort in Sort.java. Orderby uses Sort directly.
   - Added Sort cost formula to PlanCost.java
    - Modified RandomOptimizer.java to allow executor to reach and assign joins to Join nodes below Sort node
    - Integrated orderby into RandomInitialPlan.java
4. Integrated Distinct into RandomInitialPlan.java

Jaedon:
1. Implemented Distinct
2. Implemented GroupBy
3. Implemented Time

Zhang:
1. Implemented SortMergeJoin