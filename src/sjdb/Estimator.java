package sjdb;

import org.w3c.dom.Attr;

import java.util.*;
import java.util.stream.Collectors;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();

		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();

		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
	    Operator input = op.getInputs().get(0); // assume there is only one input

        List<String> projectedAttrNames = new ArrayList<>();

        for(Attribute attr: op.getAttributes()) {
            projectedAttrNames.add(attr.getName());
        }

        Relation output = new Relation(input.getOutput().getTupleCount());

        for(String projectedAttrName: projectedAttrNames) {

            for(Attribute inputAttr: input.getOutput().getAttributes()) {

                if(inputAttr.getName().equals(projectedAttrName)) {

                    output.addAttribute(new Attribute(inputAttr));

                }
            }
        }

        op.setOutput(output);
	}

	public void visit(Select op) {
        Predicate pred = op.getPredicate();
        Relation output;
        if(pred.equalsValue()) {
            output = visitSelectByVal(op);  // attr = val
        } else {
            output = visitSelectByAttr(op);  // attr = attr
        }
        op.setOutput(output);
    }


    private Relation visitSelectByAttr(Select op) {
        String leftAttrName = op.getPredicate().getLeftAttribute().getName();
        String rightAttrName = op.getPredicate().getRightAttribute().getName();

        int maxVal = Integer.MIN_VALUE;
        int minVal = Integer.MAX_VALUE;

        // Assume there will only be single input,
        // the single input can only be a Product or Scan
        Operator input = op.getInputs().get(0);

        HashMap<String, Attribute> newAttrs = new HashMap<>();

        for(Attribute attr: input.getOutput().getAttributes()) {

            if(leftAttrName.equals(attr.getName())) {
                if(attr.getValueCount() > maxVal) maxVal = attr.getValueCount();
                if(attr.getValueCount() < minVal) minVal = attr.getValueCount();

            } else if(rightAttrName.equals(attr.getName())) {
                if(attr.getValueCount() > maxVal) maxVal = attr.getValueCount();
                if(attr.getValueCount() < minVal) minVal = attr.getValueCount();
            }

            newAttrs.put(attr.getName(), new Attribute(attr));
        }

        if(maxVal == Integer.MIN_VALUE || minVal == Integer.MAX_VALUE) throw new IllegalArgumentException(
                "Attribute "+ leftAttrName + " or " + rightAttrName + " Not Found In " + op.toString());

        int TR = input.getOutput().getTupleCount(); // get tuple count from Scan or Product

        Relation output = new Relation(TR / maxVal);

        newAttrs.put(leftAttrName, new Attribute(leftAttrName, minVal)); // update left attr with min value

        newAttrs.put(rightAttrName, new Attribute(leftAttrName, minVal)); // update right attr with min value

        for(Attribute attr: newAttrs.values()) { output.addAttribute(attr); }

        return output;
    }

    private Relation visitSelectByVal(Select op) {
        String leftAttrName = op.getPredicate().getLeftAttribute().getName();

        // Assume there will only be single input,
        // the single input can only be a Product or Scan
        Operator input = op.getInputs().get(0);

        Integer VR = null;

        HashMap<String, Attribute> newAttrs = new HashMap<>();

        for(Attribute attr: input.getOutput().getAttributes()) {
            // search attribute with same name
            if(leftAttrName.equals(attr.getName())) VR = attr.getValueCount();

            newAttrs.put(attr.getName(), new Attribute(attr));
        }

        if(VR == null) throw new NullPointerException(
                "Attribute: "+ leftAttrName + " Not Found In " + op.toString());

        int TR = input.getOutput().getTupleCount(); // get tuple count from Scan or Product

        Relation output = new Relation(TR / VR);

        newAttrs.put(leftAttrName, new Attribute(leftAttrName, 1)); // update attr with value 1

        for(Attribute attr: newAttrs.values()) { output.addAttribute(attr); }

        return output;
    }

    /**
     * @param op Product operator to be visited
     */
    public void visit(Product op) {
	    List<Operator> scans = op.getInputs();

	    int updatedTupleCount = 1;

        for(Operator scan: scans) updatedTupleCount *= scan.getOutput().getTupleCount();

        Relation output  = new Relation(updatedTupleCount);

        // update new relation with attributes of the scans
        for(Operator scan: scans) {
            for(Attribute attr: scan.getOutput().getAttributes()) {
                output.addAttribute(new Attribute(attr));
            }
        }

	    op.setOutput(output);
	}
	
	public void visit(Join op) {

	}
}
