import os
import dispy
import time

# implemented with Prometheus and Grafana on an orange pi cluster (5 nodes)

def sort_file(array):
    n = len(array)
    for i in range(n - 1):
        for j in range(0, n - i - 1):
            if array[j] > array[j + 1]:
                array[j], array[j + 1] = array[j + 1], array[j]
    return array

def split_file(file_name, chunk_size):
    output_dir = os.path.dirname(file_name)
    with open(file_name, 'rb') as input_file:
        chunk_number = 0
        while True:
            chunk = input_file.read(chunk_size)
            if not chunk:
                break
            output_file_name = os.path.join(output_dir, f'chunk_{chunk_number + 1}.txt')
            with open(output_file_name, 'wb') as output_file:
                output_file.write(chunk)
            chunk_number += 1
            yield output_file_name

def sort_and_save(chunk_file_path):
    with open(chunk_file_path, 'r') as chunk_file:
        integers = [int(x) for x in chunk_file.read().split()]
    sorted_integers = sort_file(integers)
    with open(chunk_file_path, 'w') as chunk_file:
        chunk_file.write('\n'.join(str(x) for x in sorted_integers))
    sorted_file_path = chunk_file_path + '_sorted'
    os.rename(chunk_file_path, sorted_file_path)
    return sorted_file_path

def merge_files(filepaths):
    if len(filepaths) == 1:
        return filepaths[0]
    merged_filepath = f"merged_{time.time()}.txt"
    merge_two_files(filepaths[0], filepaths[1], merged_filepath)
    os.remove(filepaths[0])
    os.remove(filepaths[1])
    return merged_filepath

def merge_two_files(filepath1, filepath2, output_filepath):
    with open(filepath1, 'r') as file1, open(filepath2, 'r') as file2, open(output_filepath, 'w') as output_file:
        line1 = file1.readline()
        line2 = file2.readline()
        while line1 and line2:
            if int(line1) < int(line2):
                output_file.write(line1)
                line1 = file1.readline()
            else:
                output_file.write(line2)
                line2 = file2.readline()
        while line1:
            output_file.write(line1)
            line1 = file1.readline()
        while line2:
            output_file.write(line2)
            line2 = file2.readline()

if __name__ == '__main__':
    start_time = time.time()
    cluster = dispy.JobCluster(sort_and_save)
    merge_cluster = dispy.JobCluster(merge_files)

    chunk_files = list(split_file('path_to_your_large_file.txt', 1000000))

    jobs = [cluster.submit(chunk_file) for chunk_file in chunk_files]
    sorted_files = [job() for job in jobs] # Wait for all sorting jobs to complete and collect results

    cluster.close()  # Close the sorting cluster

    # Merge sorted files
    while len(sorted_files) > 1:
        merge_jobs = []
        for i in range(0, len(sorted_files), 2):
            if i + 1 < len(sorted_files):
                merge_job = merge_cluster.submit(sorted_files[i], sorted_files[i + 1])
                merge_jobs.append(merge_job)
            else:
                merge_jobs.append(None)

        new_sorted_files = []
        for job in merge_jobs:
            if job:
                new_sorted_files.append(job())
            else:
                new_sorted_files.append(sorted_files[-1])

        sorted_files = new_sorted_files

    merge_cluster.close()  # Close the merging cluster

    elapsed_time = time.time() - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    print(f"Time taken to sort the file: {minutes:.0f} minutes {seconds:.2f} seconds")

    if os.path.exists(sorted_files[0]):
        print("You did it!!!")
    else:
        print("You failed :(")

    print(f"Final sorted file: {sorted_files[0]}")
    print("\n\nFINISHED!")
