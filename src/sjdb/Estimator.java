package sjdb;

import org.w3c.dom.Attr;

import java.util.*;

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

        Relation output = op.getInputs().get(0).getOutput();

        return buildNewSelectByAttr(output, leftAttrName, rightAttrName);
    }

    private Relation buildNewSelectByAttr(Relation output, String leftAttrName, String rightAttrName) {

        int maxVal = Integer.MIN_VALUE;
        int minVal = Integer.MAX_VALUE;

        HashMap<String, Attribute> newAttrs = new HashMap<>();

        for(Attribute attr: output.getAttributes()) {

            if(leftAttrName.equals(attr.getName())) {
                if(attr.getValueCount() > maxVal) maxVal = attr.getValueCount();
                if(attr.getValueCount() < minVal) minVal = attr.getValueCount();

            } else if(rightAttrName.equals(attr.getName())) {
                if(attr.getValueCount() > maxVal) maxVal = attr.getValueCount();
                if(attr.getValueCount() < minVal) minVal = attr.getValueCount();
            }

            newAttrs.put(attr.getName(), new Attribute(attr));
        }

        if(maxVal == Integer.MIN_VALUE || minVal == Integer.MAX_VALUE)
            throw new IllegalArgumentException(
                    "Attribute "+ leftAttrName + " or " + rightAttrName + " Not Found In " + output.render());

        int TR = output.getTupleCount();

        // create new output
        Relation newOutput = new Relation(TR / maxVal);

        newAttrs.put(leftAttrName, new Attribute(leftAttrName, minVal)); // update left attr with min value

        newAttrs.put(rightAttrName, new Attribute(leftAttrName, minVal)); // update right attr with min value

        for(Attribute attr: newAttrs.values()) { newOutput.addAttribute(attr); }

        return newOutput;
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
        // Join uses Binary Operator - two inputs
        Operator base = op.inputs.get(0);
        Operator scan = op.inputs.get(1);
        Predicate pred = op.getPredicate();

        String leftPredAttrName = pred.getLeftAttribute().getName();
        String rightPredAttrName = pred.getRightAttribute().getName();

        HashMap<String, Attribute> baseAttrMap = new HashMap<>();
        for(Attribute attr: base.getOutput().getAttributes()) {
            baseAttrMap.put(attr.getName(), attr);
        }

        HashMap<String, Attribute> scanAttrMap = new HashMap<>();
        for(Attribute attr: scan.getOutput().getAttributes()) {
            scanAttrMap.put(attr.getName(), attr);
        }

        Relation relation;

        // case 1, when left and right attr already existed in base i.e. no need to join
        if(baseAttrMap.containsKey(leftPredAttrName)
                && baseAttrMap.containsKey(rightPredAttrName)) {

            relation = buildNewSelectByAttr(base.output, leftPredAttrName, rightPredAttrName);

        } else if(baseAttrMap.containsKey(leftPredAttrName)
                    && scanAttrMap.containsKey(rightPredAttrName) ) {

            relation = buildJoin(base.getOutput(), scan.getOutput(), leftPredAttrName, rightPredAttrName);

        } else if(baseAttrMap.containsKey(rightPredAttrName)
                    && scanAttrMap.containsKey(leftPredAttrName) ) {
            relation = buildJoin(base.getOutput(), scan.getOutput(), rightPredAttrName, leftPredAttrName);

        } else {
            throw new IllegalArgumentException("Invalid Attributes in Join " + op.toString());
        }

        op.setOutput(relation);
	}

	private Relation buildJoin(Relation baseOuput,
                               Relation scanOutput,
                               String baseAttrName,
                               String scanAttrName) {

        int maxVal = Integer.MIN_VALUE;
        int minVal = Integer.MAX_VALUE;

        HashMap<String, Attribute> newAttrs = new HashMap<>();

        for(Attribute attr: baseOuput.getAttributes()) {

            if(baseAttrName.equals(attr.getName())) {
                if(attr.getValueCount() > maxVal) maxVal = attr.getValueCount();
                if(attr.getValueCount() < minVal) minVal = attr.getValueCount();
            }

            newAttrs.put(attr.getName(), new Attribute(attr));
        }

        for(Attribute attr: scanOutput.getAttributes()) {

            if(scanAttrName.equals(attr.getName())) {
                if(attr.getValueCount() > maxVal) maxVal = attr.getValueCount();
                if(attr.getValueCount() < minVal) minVal = attr.getValueCount();
            }

            newAttrs.put(attr.getName(), new Attribute(attr));
        }

        if(maxVal == Integer.MIN_VALUE || minVal == Integer.MAX_VALUE)
            throw new IllegalArgumentException(
                    "Attributes "+ baseAttrName + " or " + scanAttrName +
                            " Not Found In \n" + baseOuput.render() +
                            "\nor\n" + scanOutput.render());

        int TR = baseOuput.getTupleCount() * scanOutput.getTupleCount();

        // create new output
        Relation newOutput = new Relation(TR / maxVal);

        newAttrs.put(baseAttrName, new Attribute(baseAttrName, minVal)); // update left attr with min value

        newAttrs.put(scanAttrName, new Attribute(scanAttrName, minVal)); // update right attr with min value

        for(Attribute attr: newAttrs.values()) { newOutput.addAttribute(attr); }

        return newOutput;
    }
}
