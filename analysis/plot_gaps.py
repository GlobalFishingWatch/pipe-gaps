import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import ScalarFormatter

plt.style.use("style.mplstyle")


class ScalarFormatterForceFormat(ScalarFormatter):
    def _set_format(self):  # Override function that finds format to use.
        self.format = "%1.1f"  # Give format here


# filename_w_filter = "gaps_per_day_with_good_seg_and_ov_short_filter.csv"
# filename_w_filter = "gaps_per_day_with_good_seg_filter.csv"

filename_w_filter = "gaps_per_day_with_good_seg2_filter.csv"
filename_wo_filter = "gaps_per_day_without_filters.csv"


filename_per_country = "gaps_per_country-2021-2023.csv"
output = "gaps_good_seg2"

w_filter = pd.read_csv(
    filename_w_filter, index_col=False, parse_dates=["day_mon_year"])

wo_filter = pd.read_csv(
    filename_wo_filter, index_col=False, parse_dates=["day_mon_year"])

wo_monthly = wo_filter.groupby(wo_filter['day_mon_year'].dt.to_period('m')).sum("count")
w_monthly = w_filter.groupby(w_filter['day_mon_year'].dt.to_period('m')).sum("count")

time = wo_monthly.index.astype('datetime64[ns]')
wo_ys = wo_monthly["count"]
w_ys = w_monthly["count"]

fig, axs = plt.subplots(1, 2, figsize=(8, 4.4))

axs[0].set_title("Gaps per Month \n(2021-2023)")

difference = abs(w_ys - wo_ys)
avg_diff = round(difference.mean())

print("Average difference: ", avg_diff)

axs[0].plot(time, w_ys, ".--", lw=2, label="Filtering good_seg2")
axs[0].plot(time, wo_ys, "-", lw=1, label="No filters", alpha=1)
axs[0].plot(time, difference, "-", lw=2, label=f"Difference\nAvg: {avg_diff}")
axs[0].legend(fontsize=10)

axs[0].set_ylim(top=7e6)
axs[0].set_xlim(time[0] - pd.Timedelta(weeks=30))
axs[0].xaxis.set_major_locator(mdates.MonthLocator(1, 12))
axs[0].xaxis.set_minor_locator(mdates.MonthLocator())
axs[0].set_ylabel('Gaps Count')

axs[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%b'))

for label in axs[0].get_xticklabels(which='major'):
    label.set(rotation=40, horizontalalignment='right')

per_country = pd.read_csv(filename_per_country, index_col=False)

countries = per_country["flag"][0:10]
count = per_country["count"][0:10]

axs[1].set_title("Top 10 Countries\n(2021-2023)")
axs[1].bar(countries, count, ec="k", align='center')
axs[1].set_ylabel('Gaps Count')

yfmt = ScalarFormatterForceFormat()
yfmt.set_powerlimits((0, 0))
axs[1].yaxis.set_major_formatter(yfmt)

for label in axs[1].get_xticklabels(which='major'):
    label.set(rotation=60, ha="center")

fig.subplots_adjust(hspace=3)
fig.tight_layout()
plt.savefig(f"{output}.svg")
plt.savefig(f"{output}.png")

plt.show()
