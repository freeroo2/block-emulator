import matplotlib.pyplot as plt

def fig1():
    l1 = [
        (5, 2.1),
        (10, 2.0),
        (15, 2.3),
        (20, 2.2),
        (21, 2.5),
        (22, 4.1),
        (21.5, 5.2),
    ]
    x1, y1 = zip(*l1)

    l2 = [
        (5, 3.2),
        (10, 3.1),
        (15, 3.3),
        (20, 3.2),
        (25, 3.1),
        (30, 3.5),
        (35, 4),
        (37, 5.2),
        (38, 6.6),
    ]
    x2, y2 = zip(*l2)

    l3 = [
        (5, 2.5),
        (10, 2.6),
        (15, 2.5),
        (20, 2.6),
        (25, 2.8),
        (30, 2.9),
        (35, 2.9),
        (40, 2.9),
        (42, 4.2),
        (43, 4.7),
        (44, 7.0),
    ]
    x3, y3 = zip(*l3)

    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Times New Roman'] + plt.rcParams['font.serif']

    # Enlarged fonts for axis labels
    plt.xlabel('Throughput (Kops/sec)', fontsize=15)
    plt.ylabel('Latency (ms)', fontsize=15)

    # Setting the log scale for x-axis
    # plt.xscale('log', base=2)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    # Adding grid for better readability
    plt.grid(linestyle=":", color="gray")

    # Plotting data with distinct colors and highlighting "Ours" in red
    plt.plot(x1, y1, marker='o', markersize=6, linestyle='-', color='red', label='Narwhal-Tusk')
    plt.plot(x2, y2, marker='s', markersize=6, linestyle='-', color='orange', label='Boardcast-TxPool')
    plt.plot(x3, y3, marker='^', markersize=6, linestyle='-', color='gold', label='Sharded-TxPool')
    # plt.plot(x, y4, marker='x', markersize=6, linestyle='-',  color='deepskyblue', label='SAQ-T, W4A4')
    # plt.plot(x, y5, marker='D', markersize=6, linestyle='-',  color='dodgerblue',  label='SAQ-T, W8A8')

    # Making the legend more prominent with a larger font and a box
    plt.legend(loc='upper left', fontsize=10, frameon=True, shadow=True)
    plt.rcParams.update({'font.size':10})
    # Show plot
    # plt.show()

    plt.savefig('./line_chart_demo.png', dpi=300)

def main():
    fig1()


if __name__ == '__main__':
    main()