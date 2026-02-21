"""Barcode-style visualisation of daily activity and work-shift periods."""

import matplotlib.pyplot as plt
import numpy as np
import matplotlib as mpl
import matplotlib.ticker as ticker
import os
import gc
import warnings


def gen_plot(df, index, ot_index, epd, settings):
    """Build per-day activity and work-time arrays for barcode plotting."""
    act_column = settings['act_column']
    plot = {day: [df[act_column][i] for i in range(start, end)] for day, (start, end) in index.items()}

    temp = [0] * len(df)
    if ot_index:
        for day, (start, end) in ot_index.items():
            for i in range(start, end):
                temp[i] = 1
    plot_work = {day: temp[start:end] for day, (start, end) in index.items()}

    first_key = min(plot.keys())
    last_key = max(plot.keys())

    for _ in range(len(plot[first_key]), epd):
        plot[first_key].insert(0, 0)
        plot_work[first_key].insert(0, 0)
    for _ in range(len(plot[last_key]), epd):
        plot[last_key].append(0)
        plot_work[last_key].append(0)

    for day in plot.keys():
        plot[day] = np.array(plot[day])
        plot_work[day] = np.array(plot_work[day])

    return plot, plot_work


def plotter(plot, ot_plot, date_info, subject_id, data_path, settings):
    """Render and save the barcode chart as a PDF in the barcode/ folder."""
    barcode = settings['barcode']
    categories = barcode['categories']

    padding_color = barcode['padding_color']
    bounds = [0] + [cat['lower'] for cat in categories] + [barcode['upper_bound']]
    colors = [padding_color] + [cat['color'] for cat in categories]
    legend_labels = [cat['label'] for cat in categories]
    legend_colors = [cat['color'] for cat in categories]

    cmap = mpl.colors.ListedColormap(colors)
    norm = mpl.colors.BoundaryNorm(bounds, cmap.N)

    times = ['24:00', '21:00', '18:00', '15:00', '12:00', '09:00', '06:00', '03:00', '00:00']

    height = 4
    width = height * 1.75
    pixel_per_bar = 3
    epd = len(plot[min(plot.keys())])
    dpi = epd * pixel_per_bar / height

    fig = plt.figure(figsize=(width, height), dpi=dpi)

    time_axes = fig.add_axes([0, 0, 0.1, 1])
    time_axes2 = fig.add_axes([0.12 * (len(plot) + 0.475), 0, 0, 1])
    bot_axes = fig.add_axes([0, -0.15, 0.1175 * len(plot), 0.05])
    intro_axes = fig.add_axes([-0.05, 1.05, 0.1175 * len(plot) + 0.1, 0.03])

    axes = [fig.add_axes([i * 0.12, 0, 0.1, 1]) for i in range(len(plot))]
    work_axes = [fig.add_axes([0.1025 + i * 0.12, 0, 0.005, 1]) for i in range(len(plot))]
    mix_axes = [time_axes, time_axes2, bot_axes, intro_axes]

    start, end = time_axes.get_ylim()
    for i in range(2):
        mix_axes[i].yaxis.set_ticks(np.arange(start, end + 0.1, 0.125))
        mix_axes[i].yaxis.set_major_formatter(ticker.FormatStrFormatter('%0.1f'))
        mix_axes[i].set_yticklabels(times, minor=False, fontsize=10)
        mix_axes[i].tick_params(axis=u'both', which=u'both', length=0)
        mix_axes[i].xaxis.set_visible(False)
        for spine in ['top', 'right', 'bottom', 'left']:
            mix_axes[i].spines[spine].set_visible(False)

    for i in range(len(axes)):
        axes[i].tick_params(axis=u'both', which=u'both', length=0)
        axes[i].yaxis.set_visible(False)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            axes[i].set_xticklabels([f'{date_info[i+1]["day_str"]}\n{date_info[i+1]["date"]}'],
                                horizontalalignment='left', rotation=12.5, minor=False, fontsize=8)
        for spine in ['top', 'right', 'bottom', 'left']:
            axes[i].spines[spine].set_visible(False)

    for i in range(len(work_axes)):
        work_axes[i].tick_params(axis=u'both', which=u'both', length=0)
        work_axes[i].yaxis.set_visible(False)
        work_axes[i].xaxis.set_visible(False)
        for spine in ['top', 'right', 'bottom', 'left']:
            work_axes[i].spines[spine].set_visible(False)

    for i in range(2, len(mix_axes)):
        mix_axes[i].tick_params(axis=u'both', which=u'both', length=0)
        mix_axes[i].yaxis.set_visible(False)
        mix_axes[i].xaxis.set_visible(False)
        for spine in ['top', 'right', 'bottom', 'left']:
            mix_axes[i].spines[spine].set_visible(False)
        mix_axes[i].set_ylim(0, 1)
        mix_axes[i].set_xlim(0, 1)

    for i in range(len(plot)):
        axes[i].imshow(plot[i+1].reshape(-1, 1), aspect='auto', cmap=cmap, norm=norm, interpolation='nearest')
        work_axes[i].imshow(ot_plot[i+1].reshape(-1, 1), aspect='auto', cmap='binary', interpolation='nearest',
                            vmin=0, vmax=1)

    n_cats = len(legend_labels)
    spacing = 1.0 / max(n_cats, 1)
    for i in range(n_cats):
        x = 0.05 + i * spacing
        bot_axes.plot(x, 0.5, marker='s', c=legend_colors[i])
        bot_axes.text(x + 0.015, 0.325, legend_labels[i], fontsize=10)

    intro_axes.text(0, 0.95, 'ID: ' + str(subject_id), fontsize=11)

    fig.savefig(os.path.join(data_path, 'barcode', f'{subject_id}.pdf'), bbox_inches='tight')
    plt.close(fig)
    gc.collect()
