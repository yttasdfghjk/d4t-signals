import gc
import tracemalloc
    
def clean_trash():
    tracemalloc.start()
    snapshot1 = tracemalloc.take_snapshot()

    collected = gc.collect()
    print(f"Removed objects: {collected}")
    
    snapshot2 = tracemalloc.take_snapshot()
    stats = snapshot2.compare_to(snapshot1, 'lineno')
    print(f"Memory released: {stats[0].size_diff / 10**6:.2f} MB")
