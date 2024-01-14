import multiprocessing as mp
import random
import time

def sort(arr):
    n = len(arr)
    for i in range(n):
        swapped = False
        for j in range(n - i - 1):
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
                swapped = True
        if not swapped:
            break
    return arr

def merge(left, right):
    res = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            res.append(left[i])
            i += 1
        else:
            res.append(right[j])
            j += 1
    res += left[i:]
    res += right[j:]
    return res

def worker(in_queue, out_queue):
    while True:
        sublist = in_queue.get()
        if sublist is None:
            break
        sorted_sublist = sort(sublist)
        out_queue.put(sorted_sublist)

if __name__ == '__main__':
    clock = time.time()
    in_queue = mp.Queue()
    out_queue = mp.Queue()

    array = [random.randint(0, 500000) for i in range(300000)]
    num_sets = 100
    num_processes = min(mp.cpu_count(), 5)
    i = len(array) // num_sets
    sublists = [array[x * i:(x + 1) * i] for x in range(num_sets)]

    processes = [mp.Process(target=worker, args=(in_queue, out_queue)) for _ in range(num_processes)]
    for process in processes:
        process.start()

    for sublist in sublists:
        in_queue.put(sublist)

    for _ in range(num_processes):
        in_queue.put(None)

    sorted_sublists = []
    for _ in range(len(sublists)):
        sorted_sublists.append(out_queue.get())

    for process in processes:
        process.join()

    while len(sorted_sublists) > 1:
        new_sorted_sublists = []
        for i in range(0, len(sorted_sublists), 2):
            if i + 1 < len(sorted_sublists):
                new_sorted_sublists.append(merge(sorted_sublists[i], sorted_sublists[i + 1]))
            else:
                new_sorted_sublists.append(sorted_sublists[i])
        sorted_sublists = new_sorted_sublists

    time_taken = time.time() - clock
    print(f"Time taken to sort the entire array: {time_taken:.2f} seconds")

    if sorted_sublists[0] == sorted(array):
        print("\nYou did it!!!!!")
    else:
        print("\nYou failed :(")
