import base64
import json
import webbrowser
import zlib
import logging
import os
from pathlib import Path

import qsig.settings


# Util method to quickly display a plot.  This creates a Report, adds a single
# plot, and this opens a browser to display it.
def quick_plot(df,
               columns=None,
               columns2=None,
               title="unnamed",
               styles=None,   # dict of column styles
               open_browser=True):
    # create a standard report
    report = Report(title)
    columns = columns or df.columns
    report.add_line_plot(df,
                         columns=columns,
                         columns2=columns2,
                         styles=styles)
    # open a browser
    if open_browser:
        report.open_browser()


def encode_json_obj(obj):
    as_str = json.dumps(obj, separators=(',', ':'))
    as_bin = zlib.compress(bytes(as_str, 'UTF-8'))
    as_b64 = base64.b64encode(as_bin)
    return as_b64


html_start = """
<!doctype html>
<html lang="en">
    <head>
      <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
      <script src="https://cdnjs.cloudflare.com/ajax/libs/pako/1.0.10/pako_inflate.min.js"></script>
      <script>

function __inflate_jsonenc(s)
{
    var rcv_bytes=window.atob(s);
    var rcv_bin = pako.inflate(rcv_bytes);
    //var rcv_json = String.fromCharCode.apply(null, new Uint16Array(rcv_bin));

    var decoder = new TextDecoder('utf-8');
    var rcv_json = decoder.decode(rcv_bin);

    var rcv_object = JSON.parse(rcv_json);
    return rcv_object;
}

      </script>
      <style>

    html, body {
      height: 100%;
      margin: 0;
    }



</style>

</head>
<body>
"""

html_end = """
</body>
</html>
"""


def build_html(outfilename, pagetitle, sections):
    html = [html_start, '<div style="margin-top:5%;padding:1px 50px;">', "<h2>", pagetitle, "</h2>"]

    for section in sections:
        divid, title, h = section
        html.extend(h)

    html.append(html_end)
    logging.info("writing html file '{}'".format(outfilename))
    with open(outfilename, "w") as fp:
        fp.write("\n".join(html))


def _dict_to_json(d):
    return json.dumps(d, indent=2)


_unique_id = 1


def get_unique_id():
    global _unique_id
    unique_id = _unique_id
    _unique_id += 1
    return unique_id


def build_plotly_line_chart(doc):
    divid = "scatterchart_{}".format(get_unique_id())

    title = doc.get("label", None)
    # xAxisLabel = doc["axes"]["x"]["label"]
    # yscale = doc['yscale'] if 'yscale' in doc else 0.75
    # xscale = doc['xscale'] if 'xscale' in doc else 0.75
    # xrot = doc['xrot'] if 'xrot' in doc else 45.0

    x_values = ['{}'.format(i) for i in doc["data"]["x"]]

    html = list()
    html.append("<div id=\"{}_fig\"  class=\"childimage\" style=\"width:100%;height:800px;\">".format(divid))
    html.append("<script>")

    # Build the javascript representations of the data values.  Rather than
    # embed numeric literals, we represent the data as an encoded object, to
    # reduce the size of the final HTML file.
    html.append("\n/* data */\n")
    trace_id = 1

    obj_enc = encode_json_obj(x_values)
    html.append('var default_enc_x = "{}";'.format(obj_enc.decode()))
    for dataset in doc["data"]["datasets"]:
        varname = "trace{}".format(trace_id)
        html.append('var {}_enc_x = default_enc_x;'.format(varname))
        obj = ['{}'.format(i) for i in dataset["y"]]
        obj_enc = encode_json_obj(obj)
        html.append('var {}_enc_y = "{}";'.format(varname, obj_enc.decode()))
        trace_id += 1

    del obj, obj_enc

    # build each of the 'var trace' json elements - these define the actual
    # separate data series on the plot
    html.append("\n/* trace definitions */\n")
    trace_id = 1
    for dataset in doc["data"]["datasets"]:
        varname = "trace{}".format(trace_id)
        yaxis = dataset.get('yaxis', 'y')
        style = dataset["style"]
        # print(f"{dataset["label"]} style: {style}")

        html.append("var " + varname+" = {")
        html.append("  " + "x: __inflate_jsonenc({}_enc_x),".format(varname))
        html.append("  " + "y: __inflate_jsonenc({}_enc_y),".format(varname))
        html.append("  name: '{}',".format(dataset["label"]))
        html.append("  yaxis: '{}',".format(yaxis))
        html.append("  type: 'scatter',")

        triangle_green = """{ symbol: 'triangle-up', size: 10, color: 'green'}"""
        triangle_red = """{symbol: 'triangle-down', size: 10, color: 'red'}"""

        # markers
        if style == "red-triangle":
            html.append("  mode: 'markers',")
            html.append(f"  marker: {triangle_red}")
        if style == "green-triangle":
            html.append("  mode: 'markers',")
            html.append(f"  marker: {triangle_green}")

        html.append("};")
        html.append("")
        trace_id += 1

    html.append("var data = [" + ', '.join(["trace{}".format(x) for x in range(1, trace_id)]) + "]; ")
    del trace_id

    # build the 'layout' item, which has items like the graph title and y-axes
    # titles and positions
    html.append("\n/* graph layout configuration */\n")
    layout = dict()
    if title is not None:
        layout["title"] = title
    layout["xaxis"] = {"domain": [0.0, 0.9]}
    layout["dragmode"] = 'pan'
    yaxis_index = 1
    position = 0.9
    if isinstance(doc["axes"]["y"], list):  # we have multiple indices
        for item in doc["axes"]["y"]:
            yaxis_settings = dict()
            yaxis_settings['title'] = item['label']
            if yaxis_index == 1:
                yaxis_settings['side'] = 'left'
                layout["yaxis"] = yaxis_settings
            else:
                yaxis_settings['overlaying'] = 'y'
                yaxis_settings['position'] = position
                layout["yaxis{}".format(yaxis_index)] = yaxis_settings
                yaxis_settings['side'] = 'right'
                position += 0.05
            yaxis_index += 1
            del yaxis_settings
    else:
        item = doc["axes"]["y"]
        yaxis_settings = dict()
        yaxis_settings['title'] = item['label']
        yaxis_settings['side'] = 'left'
        layout["yaxis"] = yaxis_settings

    layout_json = _dict_to_json(layout)

    html.append("var layout = {};".format(layout_json))

    html.append("""
var options = {
    scrollZoom: true,
    modeBarButtonsToRemove: [
        'sendDataToCloud',
        'autoScale2d',
        '__hoverClosestCartesian',
        '__hoverCompareCartesian',
        'lasso2d',
        'select2d'],
    displaylogo: false,
    showTips: true,
    responsive: true
              };
""")
    html.append("Plotly.newPlot('{}_fig', data, layout, options);".format(divid))
    html.append("</script></div>")
    return divid, title, html


class Report:

    def __init__(self,
                 report_filename=None,
                 report_dir=None):
        self._html = []
        self._sections = []
        self._report_dir = report_dir or qsig.settings.reports_home()
        self._report_filename = report_filename
        if self._report_filename is None:
            self._report_filename = "unnamed"
        self._filepath = Path(self._report_dir, self._report_filename).with_suffix(".html")
        #os.path.join(dir_name, base_filename + "." + format)
        # self.add_text(self._filepath)


    def add_text(self, text):
        divid = "0"
        title = ""
        h = ["<div>", text, "</div>"]
        comb = (divid, title, h)
        self._sections.append(comb)


    def add_line_plot(self,
                      df,
                      columns=None,
                      columns2=None,
                      title=None,
                      styles=None):
        doc = dict()

        # attempt to generate default Y axis label
        default_y_label = 'Y'

        if columns is not None:
            default_y_label = columns[0]

        axes = {
            'x':  {'label': df.index.name},
            'y': [
                {'label': default_y_label}
            ]
        }

        if columns2 is not None:
            default_y2_label = columns2[0]
            axes['y'].append({'label': default_y2_label})

        doc["axes"] = axes
        if title is not None:
            doc["label"] = title

        # datasets
        data = dict()
        doc["data"] = data
        datasets = []
        data["datasets"] = datasets
        data["x"] = df.index.values

        if columns is None:
            columns = df.columns.tolist()

        for col in columns:
            ds = dict()
            ds['y'] = df[col].values
            ds['label'] = col
            ds['yaxis'] = "y"
            ds["style"] = styles.get(col,None) if styles else None
            datasets.append(ds)
            del ds

        if columns2 is not None:
            for col in columns2:
                ds = dict()
                ds['y'] = df[col].values
                ds['label'] = col
                ds['yaxis'] = "y2"
                ds["style"] = styles.get(col,None) if styles else None
                datasets.append(ds)
                del ds

        self._sections.append(build_plotly_line_chart(doc))


    def open_browser(self):
        if not os.path.isdir(self._report_dir):
            os.makedirs(self._report_dir, exist_ok=True)
        fn = self._filepath
        build_html(fn,  os.path.basename(fn), self._sections)
        url = 'file://' + os.path.realpath(fn)
        webbrowser.open(url, new=2)
