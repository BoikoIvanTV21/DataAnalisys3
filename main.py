import pandas as pd
from multiprocessing.pool import ThreadPool
import multiprocessing
import time


def evaluate_efficiency(region_data):
    total_consumption = region_data["Global_active_power"].sum()
    avg_consumption = total_consumption / len(region_data)
    efficiency = 1000 / avg_consumption if avg_consumption != 0 else 0
    return efficiency


def parallel_energy_evaluation(regions, num_threads):
    with ThreadPool(num_threads) as pool:
        results = pool.map(evaluate_efficiency, regions)
    return results


def main():
    df = pd.read_csv("household_power_consumption.txt", sep=";",
                     na_values="?",
                     low_memory=False)

    df["datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"], dayfirst=True)

    df.drop(columns=["Date", "Time"], inplace=True)

    df.dropna(inplace=True)

    df["Global_active_power"] = df["Global_active_power"].astype(float)

    df["date"] = df["datetime"].dt.date
    regions = [group for _, group in df.groupby("date")]

    num_processes = min(len(regions), multiprocessing.cpu_count())

    start_time = time.time()
    sequential_results = [evaluate_efficiency(region) for region in regions]
    sequential_time = time.time() - start_time

    start_time = time.time()
    parallel_results = parallel_energy_evaluation(regions, num_processes)
    parallel_time = time.time() - start_time

    print(f"Час послідовного виконання: {sequential_time:.2f} сек")
    print(f"Час паралельного виконання: {parallel_time:.2f} сек")
    print(f"Прискорення: {sequential_time / parallel_time:.2f} разів")


if __name__ == "__main__":
    main()
