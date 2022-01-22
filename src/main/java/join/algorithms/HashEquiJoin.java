package join.algorithms;

import java.util.function.Consumer;

import join.datastructures.Block;
import join.datastructures.Relation;
import join.datastructures.Tuple;
import join.manager.BlockManager;

public class HashEquiJoin implements Join {
	protected final int numBuckets;
	protected final BlockManager blockManager;

	public HashEquiJoin(int numBuckets, BlockManager blockManager) {
		this.numBuckets = numBuckets;
		this.blockManager = blockManager;
	}

	private Relation[] hash(Relation relation, int joinAttribute) {
		Relation[] hashedRel = new Relation[numBuckets];
		Block[] currentBlocks = new Block[numBuckets];
		for (int i = 0; i < hashedRel.length; i++) {
			hashedRel[i] = new Relation();
			currentBlocks[i] = null;
		}
		for (Block currentOriginal : relation) {
			blockManager.pin(currentOriginal);
			for (Tuple current : currentOriginal) {
				String valueToHash = current.getData(joinAttribute);
				int hashCode = (valueToHash.hashCode() & 0x7fffffff) % numBuckets;
				if (currentBlocks[hashCode] == null) {
					currentBlocks[hashCode] = hashedRel[hashCode].getFreeBlock(blockManager);
					blockManager.pin(currentBlocks[hashCode]);
				}
				if (!currentBlocks[hashCode].addTuple(current)) {
					blockManager.unpin(currentBlocks[hashCode]);
					currentBlocks[hashCode] = hashedRel[hashCode].getFreeBlock(blockManager);
					blockManager.pin(currentBlocks[hashCode]);
					currentBlocks[hashCode].addTuple(current);
				}
			}
		}
		return hashedRel;
	}

	@Override
	public void join(Relation relation1, int joinAttribute1, Relation relation2, int joinAttribute2,
					 Consumer<Tuple> consumer) {
		// TODO: hash
		Relation[] hashedRel1 = hash(relation1, joinAttribute1);
		Relation[] hashedRel2 = hash(relation2, joinAttribute2);

		NestedLoopEquiJoin nestedLoopJoin = new NestedLoopEquiJoin(blockManager);
		for (int i = 0; i < numBuckets; ++i) {
			// TODO: join
			nestedLoopJoin.join(hashedRel1[i], joinAttribute1, hashedRel2[i], joinAttribute2, consumer);
		}
	}

	@Override
	public int getIOEstimate(Relation relation1, Relation relation2) {
		return 3 * (relation1.getBlockCount() + relation2.getBlockCount());
	}

}
