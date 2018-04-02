#coding=utf-8

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D


# https://stackoverflow.com/questions/44810875/how-to-draw-a-classic-stock-chart-with-matplotlib


def westerncandlestick(ax, quotes, width=0.2, colorup='k', colordown='r',
                 ochl=True, linewidth=0.5):


    """
    Plot the time, open, high, low, close as a vertical line ranging
    from low to high.  Use a rectangular bar to represent the
    open-close span.  If close >= open, use colorup to color the bar,
    otherwise use colordown
    Parameters
    ----------
    ax : `Axes`
        an Axes instance to plot to
    quotes : sequence of quote sequences
        data to plot.  time must be in float date format - see date2num
        (time, open, high, low, close, ...) vs
        (time, open, close, high, low, ...)
        set by `ochl`
    width : float
        fraction of a day for the open and close lines
    colorup : color
        the color of the lines close >= open
    colordown : color
         the color of the lines where close <  open
    ochl: bool
        argument to select between ochl and ohlc ordering of quotes
    linewidth: float
        linewidth of lines
    Returns
    -------
    ret : tuple
        returns (lines, openlines, closelines) where lines is a list of lines
        added
    """

    OFFSET = width / 2.0

    lines = []
    openlines = []
    closelines = []
    for q in quotes:
        if ochl:
            t, open, close, high, low = q[:5]
        else:
            t, open, high, low, close = q[:5]

        if close >= open:
            color = colorup
        else:
            color = colordown

        vline = Line2D( xdata=(t, t), ydata=(low, high),
            color=color, linewidth=linewidth, antialiased=True)
        lines.append(vline)

        openline = Line2D(xdata=(t - OFFSET, t), ydata=(open,open),
                          color=color, linewidth=linewidth, antialiased=True)
        openlines.append(openline)
        closeline = Line2D(xdata=(t , t+OFFSET), ydata=(close,close),
                          color=color, linewidth=linewidth, antialiased=True)
        closelines.append(closeline)

        ax.add_line(vline)
        ax.add_line(openline)
        ax.add_line(closeline)

    ax.autoscale_view()
    return lines, openlines, closelines

def __to_quote_tuple(i, arr):
    d = arr[0]; d1 = (int(d/10000), int(d%10000/100), int(d%100))
    return (i, arr[1], arr[2], arr[3], arr[4])

def candle_k():
    ax_pos = [0.05, 0.07, 0.9, 0.86] # 调整图形在主图中的位置 [x, y, width, height]
    ax = plt.axes(ax_pos)
    k_file = "e:/stock/list11/300608.txt"
    dc = np.loadtxt(k_file, dtype=float)[-50:, :]
    quotes = [__to_quote_tuple(i, dc[i, :]) for i in range(dc.shape[0])]
    tip_idx = 21
    L, H = np.min(dc[:, 4]), np.max(dc[:, 3])
    ax.add_line(Line2D(xdata=(tip_idx, tip_idx), ydata=(L, H), color='lightblue'))
    plt.annotate("%d"%dc[tip_idx, 0], xy=(tip_idx, H), xytext=(tip_idx+5, H),
                 arrowprops=dict(facecolor='black', shrink=0.05),
                 )
    westerncandlestick(ax, quotes, width=0.6, linewidth=1.44, ochl=True)

    ax.get_figure().canvas.draw()
    labels = [item.get_text() for item in ax.get_xticklabels()]
    for i in range(1, len(labels)-1):
        n = int(labels[i])
        if n >= dc.shape[0]:
            n = dc.shape[0]-1
        labels[i] = "%d"%dc[n, 0]
    ax.set_xticklabels(labels)

    #ax.autoscale_view()
    plt.grid(True)
    plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')

    plt.show()


def candle_k_date(ax, k_data_path, code, date, n_before, n_after, center):
    # ax_pos = [0.05, 0.07, 0.9, 0.86] # 调整图形在主图中的位置 [x, y, width, height]
    # ax = plt.axes(ax_pos)

    #plt.title("%s  -> %s"%(code, date))
    k_file = "%s/%s.txt"%(k_data_path, code)
    dc = np.loadtxt(k_file, dtype=float)
    pos = -1
    for i in range(dc.shape[0]):  # 寻找指定的日期
        if dc[i, 0] >= date:
            pos = i
            break
    if pos > 0:
        dc = dc[pos-n_before:pos+n_after, :]
    else:
        print("=== not found")
        return

    if n_before > center.shape[0]:
        n = n_before - center.shape[0]
        C = dc[n:, 2]
        center_ = np.zeros(n_before)
        center_[n:] = center
        center_line = center_ * C / 100.0 + C
    else:
        C = dc[0, 2]
        center_line = center * C / 100.0 + C
    ax.plot(center_line)

    quotes = [__to_quote_tuple(i, dc[i, :]) for i in range(dc.shape[0])]
    tip_idx = n_before
    L, H = np.min(dc[:, 4]), np.max(dc[:, 3])

    # 标识出识别点的位置
    ax.add_line(Line2D(xdata=(tip_idx, tip_idx), ydata=(L, H), color='lightblue'))
    plt.annotate("%d"%(date), xy=(tip_idx, H), xytext=(tip_idx+5, H),
                 arrowprops=dict(facecolor='black', shrink=0.05),
                 )
    westerncandlestick(ax, quotes, width=0.6, linewidth=1.44, ochl=True)
    ax.get_figure().canvas.draw()

    # 修改 x 轴标签
    # labels = [item.get_text() for item in ax.get_xticklabels()]
    # for i in range(1, len(labels)-1):
    #     n = int(labels[i])
    #     if n >= dc.shape[0]:
    #         n = dc.shape[0]-1
    #     labels[i] = "%d"%dc[n, 0]
    # ax.set_xticklabels(labels)

    ax.autoscale_view()
    # plt.grid(True)
    # plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
    #
    # plt.show()

_c = '''
from tushare_ut import daily_find_cluster
from k_figure import CandleK
import os

os.chdir('e:/stock')
centers_file, cluster_result_path, tags_file = "centers0330.txt", "clustering_out_0402", "day_tags0402.txt"
tags, center = daily_find_cluster(centers_file, cluster_result_path, tags_file, 50)
k_data_path = 'e:/stock/list11'
c = CandleK(k_data_path, tags, center)
'''

class CandleK:
    def __init__(self, k_data_path, tags, center, n_before=30, n_after=12):
        self.idx = 0
        self.k_data_path = k_data_path
        self.tags = tags
        self.center = center
        self.n_before = n_before
        self.n_after = n_after

        #self.fig = self.ax.get_figure()

        self.draw()
        #plt.grid(True)
        #plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')

        plt.show()

    def draw(self, event=None):
        plt.clf()

        if event is not None:
            # print('%s click: button=%d, x=%d, y=%d, xdata=%f, ydata=%f' %
            #       ('double' if event.dblclick else 'single', event.button,
            #        event.x, event.y, event.xdata, event.ydata))
            if event.button == 3 and self.idx >= 2:
                self.idx -= 2
        if self.idx >= len(self.tags):
            self.idx = 0
        code, date = self.tags[self.idx]
        ax_pos = [0.05, 0.07, 0.9, 0.86]  # 调整图形在主图中的位置 [x, y, width, height]
        ax = plt.axes(ax_pos)
        plt.title("%s  -> %s    %d/%d" % (code, date, self.idx, len(self.tags)))
        candle_k_date(ax, self.k_data_path, code, date, self.n_before, self.n_after, self.center)

        ax.get_figure().canvas.mpl_connect('button_press_event', self.draw)
        self.idx += 1

