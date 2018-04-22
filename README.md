# RelationalQueryOptimizer

### Estimator.java

Purpose: estimate the cost of the query plans.

Estimator implements the _**PlanVisitor**_ interface and performs
a depth-first traversal of the query plan. 

On each operator, the Estimator should create an instance of _**Relation**_ 
(bearing appropriate Attribute instances and tuple counts) and attach to the operator as its output.

Some operators may require you to revise the value counts for the attributes on the newly created output
relations (for example, a select of the form attr=val will change the number of distinct values for that
attribute to 1)

Note also that an attribute on a relation may not have more distinct values than there are tuples in the relation.

Page 5 of this coursework specification lists the formulae that you should use to calculate the sizes of the
output relations, and to revise the attribute value counts. 

The supplied distribution of SJDB includes a skeleton for Estimator, 
including an implementation of the visit(Scan) method.

### Optimiser.java

Goal: take a canonical query plan as input, and produce an optimised query plan as output. 

The optimised plan should not share any operators with the canonical query plan; 
all operators should be created afresh.

In order to demonstrate your optimiser, you should be able to show your cost estimation and query
optimisation classes in action on a variety of inputs. 

The SJDB zip file contains a sample catalogue and queries. 

In addition, the SJDB class (see page 3) contains a main() method with sample code for reading a
serialised catalogue from file and a query from stdin.