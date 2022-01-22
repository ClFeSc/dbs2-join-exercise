package join.algorithms;

import java.util.ArrayList;
import java.util.Stack;
import java.util.function.Consumer;

import join.datastructures.Block;
import join.datastructures.Relation;
import join.datastructures.Tuple;
import join.helper.PinningTupleIterator;
import join.manager.BlockManager;

public class HashEquiJoin implements Join {
	protected final int numBuckets;
	protected final BlockManager blockManager;

	public HashEquiJoin(int numBuckets, BlockManager blockManager) {
		this.numBuckets = numBuckets;
		this.blockManager = blockManager;
	}

	public ArrayList<Stack<Block>> hash(Relation relation, int joinAttribute) {
		ArrayList<Stack<Block>> hashedRel = new ArrayList<>(numBuckets);
		for (int i = 0; i < numBuckets; i++) {
			hashedRel.add(new Stack());
			hashedRel.get(i).push(blockManager.getFreeBlock(false));
			blockManager.pin(hashedRel.get(i).peek());
		}
		PinningTupleIterator tuples1 = new join.helper.PinningTupleIterator(relation.iterator(), blockManager);
		while (tuples1.hasNext()) {
			Tuple tuple = tuples1.next();
			String relevantAttribute = tuple.getData(joinAttribute);
			int hashCode = (int)((relevantAttribute.hashCode() & 0xffffffffL) % numBuckets);
			if (!hashedRel.get(hashCode).peek().addTuple(tuple)) {
				blockManager.unpin(hashedRel.get(hashCode).peek());
				hashedRel.get(hashCode).push(blockManager.getFreeBlock(false));
				blockManager.pin(hashedRel.get(hashCode).peek());
			}
		}
		return hashedRel;
	}

	@Override
	public void join(Relation relation1, int joinAttribute1, Relation relation2, int joinAttribute2,
					 Consumer<Tuple> consumer) {
		// TODO: hash
		ArrayList<Stack<Block>> hashedRel1 = hash(relation1, joinAttribute1);
		ArrayList<Stack<Block>> hashedRel2 = hash(relation2, joinAttribute2);

		NestedLoopEquiJoin nestedLoopJoin = new NestedLoopEquiJoin(blockManager);
		for (int i = 0; i < numBuckets; ++i) {
			// TODO: join
			Stack<Block> bucket1 = hashedRel1.get(i);
			Stack<Block> bucket2 = hashedRel2.get(i);
			Stack<Block> smaller = bucket1.size() < bucket2.size() ? bucket1 : bucket2;
			int smallerJoinAttribute = bucket1.size() < bucket2.size() ? joinAttribute1 : joinAttribute2;
			Stack<Block> larger = bucket1.size() >= bucket2.size() ? bucket1 : bucket2;
			int largerJoinAttribute = bucket1.size() >= bucket2.size() ? joinAttribute1 : joinAttribute2;
			for (Block block : smaller) {
				blockManager.pin(block);
			}
			while (!larger.empty()) {
				Block current = larger.pop();
				blockManager.pin(current);
				for (Block currentLeftBlock : smaller) {
					for (Tuple currentLeft : currentLeftBlock) {
						for (Tuple currentRight : current) {
							if (currentLeft.getData(smallerJoinAttribute).equals(currentRight.getData(largerJoinAttribute))) {
								consumer.accept(Join.joinTuple(currentLeft, smallerJoinAttribute, currentRight, largerJoinAttribute));
							}
						}
					}
				}
				blockManager.unpin(current);
			}
			for (Block block : smaller) {
				blockManager.unpin(block);
			}
		}
	}

	@Override
	public int getIOEstimate(Relation relation1, Relation relation2) {
		return 3 * (relation1.getBlockCount() + relation2.getBlockCount());
	}

}
