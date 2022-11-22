import matplotlib.pyplot as plt
import pandas as pd
import os


def plot_elbow(path: str):
    frame: pd.DataFrame = pd.read_csv(f"{path}/{next(filter(lambda item: item.endswith('.csv'), os.listdir(path)))}")
    os.makedirs(f"plots/{path[: path.find('/')]}", exist_ok=True)
    plt.grid(True)
    plt.plot(frame.numClusters, frame.value, '-*', label='kmeans-cost')
    plt.title(f"{path[path.find('/') + 1:].replace('_', ' ')} cost")
    plt.xlabel('Number of clusters')
    plt.ylabel('Silhouette metric')
    plt.legend()
    plt.savefig(f'plots/{path}.png')
    plt.clf()


if __name__ == '__main__':
    plot_elbow("results/kmeans_cost_C1")
    plot_elbow("results/kmeans_cost_C2")
    plot_elbow("results/kmeans_cost_C3")
    plot_elbow("results/bisecting_kmeans_cost_C1")
    plot_elbow("results/bisecting_kmeans_cost_C2")
    plot_elbow("results/bisecting_kmeans_cost_C3")