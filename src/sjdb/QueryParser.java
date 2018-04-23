/**
 * 
 */
package sjdb;

import org.w3c.dom.Attr;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * This class parses a canonical query provided on stdin
 * 
 * The canonical query is of the form:
 * 
 * SELECT <attribute name>,<attribute name>,...,<attribute name>
 * FROM <relation name>,<relation name>,...,<relation name>
 * WHERE <predicate>,<predicate>,...,<predicate>
 * 
 * where <predicate> is of one of the following two forms:
 * 
 * <attribute name>="<value>"
 * <attribute name>=<attribute name>
 * 
 * The WHERE line (corresponding to the select operators) is optional and 
 * may be omitted; the other lines are required.
 * 
 * To form the canonical query, a left-deep tree of cartesian
 * products over scans over the named relations is built, following by a series
 * of select with the given predicates, and then a single project 
 * with the given attributes.
 * 
 * Note that the author of this class was extremely lazy, and so the parsing 
 * is unforgiving and may be sensitive to extraneous whitespace. In particular, 
 * values in predicates that contain spaces (or for that matter commas) will 
 * break the parsing of the WHERE clause.
 * 
 * @author nmg
 */
public class QueryParser {
	private BufferedReader reader;
	private Catalogue catalogue;
	static Set<String> relations;

	/**
	 * Create a new QueryParser. This class is intended to be used once only;
	 * repeated calls to parse() may cause unexpected behaviour.
	 * 
	 * @param catalogue
	 * @param input
	 * @throws Exception
	 */
	public QueryParser(Catalogue catalogue, Reader input) throws Exception {
		this.catalogue = catalogue;
		this.reader = new BufferedReader(input);
		relations = new HashSet<>();
	}
	
	/**
	 * Read a query from the input (via the BufferedReader) and parse it
	 * to create a canonical query plan.
	 * 
	 * @return
	 * @throws Exception
	 */
	public Operator parse() throws Exception {
		Operator product = null, base = null;

		String projectLine = this.reader.readLine();
		String productLine = this.reader.readLine();
		product = parseProduct(productLine);

		String line;
		List<String> lines = new ArrayList<>(); // storing joins and where

		while(true) {
			line = this.reader.readLine();
			if(line == null) break;
			lines.add(line);
		}

		if(lines.size() == 0) return parseProject(projectLine, product);

		base = product;

		for(int i = 0; i < lines.size()-1; i++) {

			line = lines.get(i);

			if(line.startsWith("JOIN")) {

				String relationName = getJoinRelation(line);

				if(relations.contains(relationName)) {
					throw new IllegalArgumentException("Cannot repeat Relation in JOIN");
				}

				relations.add(relationName);
				base = parseJoin(line, base);
			}
		}

		String lastLine = lines.get(lines.size()-1);

		if(lastLine.startsWith("WHERE")) {
			base = parseSelect(lastLine, base);
		}

		return parseProject(projectLine, base);
	}

	public String getJoinRelation(String line) {

		String[] relationship = line.split("JOIN\\s+"); // get relation to join

		String[] relPred = relationship[1].split("\\s*ON\\s*"); // get predicates

		return relPred[0];
	}

	public Operator parseJoin(String line, Operator base) { // base to be product of relationships

		String[] relationship = line.split("JOIN\\s+"); // get relation to join

		String[] relPred = relationship[1].split("\\s*ON\\s*"); // get predicates

		String[] predicates = relPred[1].split("\\s*,\\s*"); // multi predicates in JOIN

		Operator right = buildScan(relPred[0]); // get Scan of relationship

		for(String re: predicates) {
			base = buildJoin(re, base, right);
		}

		return base;
	}

	private Operator buildJoin(String pred, Operator left, Operator right) {
		Pattern p = Pattern.compile("(\\w+)=\"(\\w+)\"");
		Matcher m = p.matcher(pred);

		if (m.matches()) {

			Attribute leftAttr = new Attribute(m.group(1));
			Attribute rightAttr = new Attribute(m.group(2));
			return new Join(left, right, new Predicate(leftAttr, rightAttr));

		} else {
			throw new IllegalArgumentException("Invalid JOIN predicate statement");
		}
	}

	/**
	 * Parse a "FROM ..." line 
	 * @param line
	 * @return
	 */
	public Operator parseProduct(String line) {
		String[] rels = line.split("FROM\\s+");
		String[] reln = rels[1].split("\\s*,\\s*");
		return buildProduct(reln);
	}
	
	/**
	 * Build a left-deep cartesian product tree from the relations
	 * with the given names
	 * @param names
	 * @return
	 */
	private Operator buildProduct(String[] names) {

		Operator left = buildScan(names[0].trim());
		Operator right;
		
		if (names.length>1) {

			for (int i = 1; i < names.length; i++) {

				right = buildScan(names[i].trim());

				if(relations.contains(names[i].trim())) continue; // avoid duplication

				relations.add(names[i].trim());

				left = new Product(left, right);
			}
		}
		
		return left;
	}
	
	/**
	 * Build a scan operator that reads the relation with the given
	 * name
	 * @param name
	 * @return
	 */
	private Operator buildScan(String name) {
		Operator op = null;
		try {
			op = new Scan(this.catalogue.getRelation(name));
		} catch (Exception e) {
			System.err.println(e.toString());
		}
		return op;
	}
	
	/**
	 * Parse a "WHERE ..." line.
	 * @param line
	 * @param op
	 * @return
	 */
	private Operator parseSelect(String line, Operator op) {

		String[] prds = line.split("WHERE\\s+");

		String[] pred = prds[1].split("\\s*,\\s*");

		Operator ret = op;
		
		for (int i=0; i<pred.length; i++) {
			ret = buildSelect(pred[i].trim(), ret);
		}
		
		return ret;
	}
	
	/**
	 * Build a chain of select operators.
	 * @param pred
	 * @param op
	 * @return
	 */
	private Operator buildSelect(String pred, Operator op) {
		Pattern p = Pattern.compile("(\\w+)=\"(\\w+)\"");
		Matcher m = p.matcher(pred);
		Predicate ret;
		
		if (m.matches()) {
			ret = new Predicate(new Attribute(m.group(1)), m.group(2));
		} else {
			String[] atts = pred.split("=");
			ret = new Predicate(new Attribute(atts[0]), new Attribute(atts[1]));
		}
		
		return new Select(op, ret);
	}
	
	/**
	 * Parse a "SELECT ..." line and build the corresponding project operator.
	 * @param line
	 * @param op
	 * @return
	 */
	private Operator parseProject(String line, Operator op) {
		String[] atts = line.split("SELECT\\s+");		
		if (atts[1].trim().equals("*")) {
			return op;
		} else {
			String[] attr = atts[1].split("\\s*,\\s*");
			ArrayList<Attribute> attributes = new ArrayList<Attribute>();

			for (int i=0; i<attr.length; i++) {
				attributes.add(new Attribute(attr[i].trim()));
			}

			return new Project(op, attributes);
		}
	}
}