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
       merge_start_time = time.time()
       if left[i] <= right[j]:
           res.append(left[i])
           i += 1
       else:
           res.append(right[j])
           j += 1
       time_merge_single = time.time() - merge_start_time
       minutes, seconds = divmod(time_merge_single, 60)
       print("Time taken to merge single array: {:.0f} minutes {:.10f} seconds".format(minutes, seconds))
   res += left[i:]
   res += right[j:]
   return res

def worker(in_queue, out_queue):
   while True:
       worker_start = time.time()

       getting_time_start = time.time()
       sublist = in_queue.get()
       time_getting = time.time() - getting_time_start
       minutes1, seconds1 = divmod(time_getting, 60)

       if sublist is None:
           break

       single_sort_start = time.time()
       sorted_sublist = sort(sublist)
       time_single_sort = time.time() - single_sort_start
       minutes2, seconds2 = divmod(time_single_sort, 60)

       out_queue_time = time.time()
       out_queue.put(sorted_sublist)
       time_out = time.time() - out_queue_time
       minutes3, seconds3 = divmod(time_out, 60)

       worker_time = time.time() - worker_start
       minutes4, seconds4 = divmod(worker_time, 60)

   print("Input queue time: {:.0f} minutes {:.10f} seconds".format(minutes1, seconds1))
   print("Single sort time: {:.0f} minutes {:.10f} seconds".format(minutes2, seconds2))
   print("Output queue time: {:.0f} minutes {:.10f} seconds".format(minutes3, seconds3))
   print("Total Worker time: {:.0f} minutes {:.10f} seconds\n".format(minutes4, seconds4))


if __name__ == '__main__':
   clock = time.time()
   in_queue = mp.Queue()
   out_queue = mp.Queue()

   creating_array_start = time.time()
   array = [random.randint(0, 500000) for i in range(300000)]
   time_creating_array = time.time() - creating_array_start
   minutes, seconds = divmod(time_creating_array, 60)
   print("Time taken to create the array: {:.0f} minutes {:.10f} seconds".format(minutes, seconds))

   num_sets = 100
   num_processes = min(mp.cpu_count(), 5)
   i = len(array) // num_sets
   sublists = [array[x * i:(x + 1) * i] for x in range(num_sets)]

   worker_process_start = time.time()
   processes = [mp.Process(target=worker, args=(in_queue, out_queue)) for i in range(num_processes)]
   for process in processes:
       process.start()
   time_processes = time.time() - worker_process_start
   minutes1, seconds1 = divmod(time_processes, 60)
   print("Time to start worker process: {:.0f} minutes {:.10f} seconds".format(minutes1, seconds1))

   sublist_start = time.time()
   for sublist in sublists:
       in_queue.put(sublist)
   time_sublist = time.time() - sublist_start
   minutes2, seconds2 = divmod(time_sublist, 60)
   print("Time to start adding sublists to in_queue: {:.0f} minutes {:.10f} seconds".format(minutes2, seconds2))

   for i in range(num_processes):
       in_queue.put(None)

   sorted_sub_start = time.time()
   sorted_sublists = []
   for i in range(len(sublists)):
       sorted_sublists.append(out_queue.get())
   time_sorted_sub = time.time() - sorted_sub_start
   minutes4, seconds4 = divmod(time_sorted_sub, 60)

   for process in processes:
       process.join()

   merge_start = time.time()
   while len(sorted_sublists) > 1:
       new_sorted_sublists = []
       for i in range(0, len(sorted_sublists), 2):
           if i + 1 == len(sorted_sublists):
               new_sorted_sublists.append(sorted_sublists[i])
           else:
               new_sorted_sublists.append(merge(sorted_sublists[i], sorted_sublists[i + 1]))
       sorted_sublists = new_sorted_sublists
   time_sorted_merge = time.time() - merge_start
   minutes5, seconds5 = divmod(time_sorted_merge, 60)

   print("*" * 60)
   print("Time to sort everything: {:.0f} minutes {:.2f} seconds".format(minutes4, seconds4))
   print("Time to merge everything: {:.0f} minutes {:.2f} seconds".format(minutes5, seconds5))

   time_taken = time.time() - clock
   minutes, seconds = divmod(time_taken, 60)
   print("Time taken to sort the entire array: {:.0f} minutes {:.2f} seconds".format(minutes, seconds))
   #print("\nFirst ten numbers of the sorted array: ")
   #print(sorted_sublists[0][:10])
   #print("\nLast ten numbers of the sorted array: ")
   #print(sorted_sublists[0][-10:])

   if sorted_sublists[0] == sorted(array):
       print("\nYou did it!!!!!")
   else:
       print("\nYou failed :(")
