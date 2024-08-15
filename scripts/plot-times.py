import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

from common import PROJ_DIR


FIG_SIZE = (8, 4)
WIDTH_RATIOS = [2,3]
BAR_WIDTH = 0.4

times = pd.read_csv(os.path.join(PROJ_DIR, "results", "times.csv"))
fig, ax = plt.subplots(nrows=1, ncols=2, sharey=True, figsize=FIG_SIZE,
                       width_ratios=WIDTH_RATIOS)
x_axis = np.arange(3)

times1 = times[times["mode"]=="single"]
width1 = BAR_WIDTH*WIDTH_RATIOS[1]/WIDTH_RATIOS[0]
ax[0].bar(x=x_axis, height=times1["time"], align="center", width=width1,
          color="#2ca02c")
ax[0].set_ylim(top=35)
ax[0].set_xticks(range(3), times1["vcpus-per-node"])
ax[0].set_xlabel("VCPUs per node")
ax02 = ax[0].twiny()
ax02.bar(x=x_axis, height=np.zeros(3), align="center", width=width1)
ax02.set_xticks(range(3), times1["ram-per-node"])
ax02.set_xlabel("RAM per node, GB")
ax[0].set_title("Single-node setup", fontweight="bold")

times2 = times[times["mode"]=="multi"]
times2 = times2.sort_values(by=["vcpus-per-node", "worker-nodes"])
times2_small = times2[times2["vcpus-per-node"]==4]
times2_large = times2[times2["vcpus-per-node"]==16]
ax[1].bar(x=x_axis-BAR_WIDTH/2, height=times2_small["time"],
          label="  4 VCPUs,   8 GB RAM",
          width=BAR_WIDTH, color="#1f77b4")
ax[1].bar(x=x_axis+BAR_WIDTH/2, height=times2_large["time"],
          label="16 VCPUs, 16 GB RAM",
          width=BAR_WIDTH, color="#ff7f0e")
ax[1].set_xticks(range(3), times2_small["worker-nodes"])
ax[1].set_xlabel("Worker nodes")
ax[1].tick_params(axis="y", left=True, labelleft=True)
ax[1].set_title("Multi-node setup", fontweight="bold")
plt.suptitle("(with a dedicated master node)", x=0.69)
leg = ax[1].legend(title="Node specs:")
leg._legend_box.align = "left"

for i in range(2):
	ax[i].set_ylabel("Execution time, minutes")
	ax[i].grid()
fig.align_titles()

plt_file = os.path.join(PROJ_DIR, "results", "times.png")
plt.savefig(plt_file, dpi=300, bbox_inches="tight")
plt.close()
print(f"\nPlot saved to {plt_file}")
