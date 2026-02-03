import React, { useEffect } from "react";
import * as am5 from "@amcharts/amcharts5";
import * as am5xy from "@amcharts/amcharts5/xy";

export default function NWChart({ data }) {
  useEffect(() => {
    const root = am5.Root.new("chartdiv");

    const chart = root.container.children.push(
      am5xy.XYChart.new(root, {
        panX: true,
        panY: true,
        wheelX: "panX",
        wheelY: "zoomX"
      })
    );

    const xAxis = chart.xAxes.push(
      am5xy.DateAxis.new(root, {
        baseInterval: { timeUnit: "minute", count: 5 },
        renderer: am5xy.AxisRendererX.new(root, {})
      })
    );

    const yAxis = chart.yAxes.push(
      am5xy.ValueAxis.new(root, {
        renderer: am5xy.AxisRendererY.new(root, {})
      })
    );

    const series = chart.series.push(
      am5xy.LineSeries.new(root, {
        xAxis,
        yAxis,
        valueYField: "networth",
        valueXField: "datetime"
      })
    );

    series.data.setAll(data);

    return () => root.dispose();
  }, [data]);

  return <div id="chartdiv" style={{ width: "100%", height: "500px" }} />;
}
