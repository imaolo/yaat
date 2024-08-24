import matplotlib.pyplot as plt, numpy as np

chart_data = np.load('chart.npy')

x_axis = [i for i in range(chart_data.shape[-1])]
for pred in chart_data:
    plt.figure(figsize=(10, 6))
    plt.plot(x_axis, pred[0], label='actual', marker='o')
    plt.plot(x_axis, pred[1], label='preds', marker='x')
    plt.xlabel('Index')
    plt.ylabel('Value')
    plt.legend()
    plt.grid(True)
    plt.show()