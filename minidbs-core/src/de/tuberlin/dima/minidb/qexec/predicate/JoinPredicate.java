package de.tuberlin.dima.minidb.qexec.predicate;


import org.apache.hadoop.io.Writable;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;


/**
 * Interface describing a join predicate. The predicate checks if two tuples should be joined.
 * 
 * The predicate itself is not necessarily a comparison between two columns from two tuples,
 * but may for example be a conjunction or disjunction of other join predicates.
 * The join predicate can hence be understood as a tree where the inner nodes are conjunctions
 * or disjunctions and the leafs are direct comparisons of tuples. A call to
 * <code>evaluate(...)</code> determines if the subtree below that predicate is true.
 * 
 * @author Helko Glathe, Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface JoinPredicate extends Writable
{
	/**
	 * Evaluates this predicate on the given tuple.
	 * 
	 * @param leftHandSide The left hand side tuple.
	 * @param rightHandSide The right hand side tuple.
	 *  
	 * @return True, if the two tuple should be joined, false otherwise. 
	 * @throws QueryExecutionException If the predicate could not be evaluated on
	 *                                 the given tuples.
	 */
	public boolean evaluate(DataTuple leftHandSide, DataTuple rightHandSide)
	throws QueryExecutionException;
	
	/**
	 * Returns if this is an equi join predicate.
	 * 
	 * @return
	 */
	public boolean isEquiJoin();
}
