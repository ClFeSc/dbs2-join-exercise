package join.algorithms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
				hashCode = 0;
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
		for (Block currentBlock : currentBlocks) {
			if (currentBlock == null)
				continue;
			blockManager.unpin(currentBlock);
		}
		return hashedRel;
	}

	@Override
	public void join(Relation relation1, int joinAttribute1, Relation relation2, int joinAttribute2,
					 Consumer<Tuple> consumer) {
		// TODO: hash
		System.out.println(relation1.getBlockCount());
		Relation[] hashedRel1 = hash(relation1, joinAttribute1);
		Relation[] hashedRel2 = hash(relation2, joinAttribute2);

		/*NestedLoopEquiJoin nestedLoopJoin = new NestedLoopEquiJoin(blockManager);
		for (int i = 0; i < numBuckets; ++i) {
			// TODO: join
			nestedLoopJoin.join(hashedRel1[i], joinAttribute1, hashedRel2[i], joinAttribute2, consumer);
		}*/
		int size1 = Arrays.stream(hashedRel1).map((Relation::getBlockCount)).reduce(0, Integer::sum);
		int size2 = Arrays.stream(hashedRel2).map((Relation::getBlockCount)).reduce(0, Integer::sum);
		boolean swapped = size1 > size2;
		Relation[] smaller = swapped ? hashedRel2 : hashedRel1;
		Relation[] larger = swapped ? hashedRel1 : hashedRel2;

		List<Block> blocksOfSmaller = new ArrayList<>(smaller.length);
		for (Relation relation : smaller) {
			for (Block block : relation) {
				System.out.println("Pinning at outer");
				blockManager.pin(block);
				blocksOfSmaller.add(block);
			}
		}
		for (Relation relation : larger){
			for (Block currentRight : relation) {
				System.out.println("Pinning at inner");
				blockManager.pin(currentRight);
				for (Block currentLeft : blocksOfSmaller) {
					Join.joinTuples(currentLeft, swapped ? joinAttribute2 : joinAttribute1, currentRight, swapped ? joinAttribute1 : joinAttribute2, consumer);
				}
				blockManager.unpin(currentRight);
			}
		}
		for (Relation relation : smaller) {
			for (Block block : relation) {
				blockManager.unpin(block);
				blocksOfSmaller.add(block);
			}
		}
	}

	@Override
	public int getIOEstimate(Relation relation1, Relation relation2) {
		return 3 * (relation1.getBlockCount() + relation2.getBlockCount());
	}

}
