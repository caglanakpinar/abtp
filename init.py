
def show_chart(df, x, y1, y2, is_bar_chart):
    """
    plots line chart with to lines
    params: df; x, y1, y2 fields included, x: x axis of data set, y1, y2 and y3; y axis of data sets
    params: is_bar_chart if True shows Bar chart, is_sorted: if True sorts y1, y2 and y3
    return: return multi dimensional line chart or bar chart on plotly
    """
    import plotly.graph_objs as go
    import plotly.offline as offline
    offline.init_notebook_mode()

    chart = go.Bar if is_bar_chart else go.Scatter
    marker = {'size': 15, 'opacity': 0.5, 'line': {'width': 0.5, 'color': 'white'}}
    x = df[x]
    _y1, _y2 = df[y1], df[y2]
    trace = []
    names = [y1, y2]
    counter = 0
    for _y in [_y1, _y2]:
        if not is_bar_chart:
            trace.append(chart(x=x, y=_y, mode='lines+markers', name=names[counter], marker=marker))
        else:
            trace.append(chart(x=x, y=_y, name=names[counter]))
        counter += 1
    offline.iplot(trace)