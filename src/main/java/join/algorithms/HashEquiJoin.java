package join.algorithms;

import java.util.function.Consumer;
import java.util.ArrayList;

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

	private ArrayList<ArrayList<Block>> fillBuckets(Relation relation, int joinattribute){
		//create buckets
		ArrayList<ArrayList<Block>> buckets = new ArrayList<>();
		int[] currentBlocks = new int[numBuckets];
		for (int i = 0; i < numBuckets; ++i) {
			buckets.add(new ArrayList<>());
			buckets.get(i).add(blockManager.getFreeBlock(false));
			blockManager.pin(buckets.get(i).get(0));
			currentBlocks[i] = 0;
		}

		//hashing
		for (Block block : relation) {
			blockManager.pin(block);
			for (Tuple tuple : block) {
				int h = tuple.getData(joinattribute).hashCode();
				int i = (h % numBuckets + numBuckets) % numBuckets;
				if(!buckets.get(i).get(currentBlocks[i]).addTuple(tuple)){
					blockManager.unpin(buckets.get(i).get(currentBlocks[i]));
					currentBlocks[i]++;
					buckets.get(i).add(blockManager.getFreeBlock(false));
					blockManager.pin(buckets.get(i).get(currentBlocks[i]));
				}
			}
			blockManager.unpin(block);
		}
		for (int i = 0; i < numBuckets; ++i){
			blockManager.unpin(buckets.get(i).get(currentBlocks[i]));
		}
		return buckets;
	}

	@Override
	public void join(Relation relation1, int joinAttribute1, Relation relation2, int joinAttribute2,
			Consumer<Tuple> consumer) {
		// TODO: hash
		// use smaller relation as outer relation
		boolean swapped = relation2.getBlockCount() < relation1.getBlockCount();
		Relation s = swapped ? relation2 : relation1;
		Relation r = swapped ? relation1 : relation2;

		ArrayList<ArrayList<Block>> sbuckets = fillBuckets(s, swapped ? joinAttribute2 : joinAttribute1);
		ArrayList<ArrayList<Block>> rbuckets = fillBuckets(r, swapped ? joinAttribute1 : joinAttribute2);

		// Not needed, commented out to save space
		// NestedLoopEquiJoin nestedLoopJoin = new NestedLoopEquiJoin(blockManager);
		for (int i = 0; i < numBuckets; ++i) {
			// TODO: join
			for (Block block : sbuckets.get(i)){
				blockManager.pin(block);
			}
			for (Block rblock : rbuckets.get(i)){
				blockManager.pin(rblock);
				for (Block sblock : sbuckets.get(i)) {
					Join.joinTuples(swapped ? rblock : sblock, joinAttribute1,
							swapped ? sblock : rblock, joinAttribute2, consumer);
				}
				blockManager.unpin(rblock);
			}
			for (Block block : sbuckets.get(i)){
				blockManager.unpin(block);
			}
		}
	}

	@Override
	public int getIOEstimate(Relation relation1, Relation relation2) {
		return 3 * (relation1.getBlockCount() + relation2.getBlockCount());
	}

}
