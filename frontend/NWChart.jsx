import React, { useEffect, useMemo } from "react";
import * as am5 from "@amcharts/amcharts5";
import * as am5xy from "@amcharts/amcharts5/xy";
import am5themes_Animated from "@amcharts/amcharts5/themes/Animated";

export default function NWChart({ data }) {
  // Convert API points: {t, v} -> chart points: {date, value}
  const chartData = useMemo(() => {
    if (!Array.isArray(data)) return [];
    return data
      .map((p) => {
        const ms = Date.parse(p.t); // ISO -> ms
        if (!Number.isFinite(ms)) return null;
        return { date: ms, value: Number(p.v) };
      })
      .filter(Boolean);
  }, [data]);

  useEffect(() => {
    // Unique container per component instance (avoid id collisions)
    const el = document.createElement("div");
    el.style.width = "100%";
    el.style.height = "500px";

    // Attach to placeholder wrapper
    const wrapper = document.getElementById("nw-chart-wrapper");
    if (!wrapper) return;
    wrapper.innerHTML = "";
    wrapper.appendChild(el);

    const root = am5.Root.new(el);

    // Optional theme (looks nicer, safe)
    root.setThemes([am5themes_Animated.new(root)]);

    const chart = root.container.children.push(
      am5xy.XYChart.new(root, {
        panX: true,
        panY: false,
        wheelX: "panX",
        wheelY: "zoomX",
      })
    );

    const xAxis = chart.xAxes.push(
      am5xy.DateAxis.new(root, {
        baseInterval: { timeUnit: "minute", count: 5 },
        renderer: am5xy.AxisRendererX.new(root, {}),
        tooltip: am5.Tooltip.new(root, {}),
      })
    );

    const yAxis = chart.yAxes.push(
      am5xy.ValueAxis.new(root, {
        renderer: am5xy.AxisRendererY.new(root, {}),
      })
    );

    const series = chart.series.push(
      am5xy.LineSeries.new(root, {
        name: "Networth",
        xAxis,
        yAxis,
        valueYField: "value",
        valueXField: "date",
        tooltip: am5.Tooltip.new(root, {
          labelText: "{valueY}",
        }),
      })
    );

    series.data.setAll(chartData);

    // Cursor (hover + zoom feel)
    chart.set(
      "cursor",
      am5xy.XYCursor.new(root, {
        xAxis,
      })
    );

    return () => {
      root.dispose();
    };
  }, [chartData]);

  // Wrapper div (no fixed id chartdiv collisions)
  return <div id="nw-chart-wrapper" style={{ width: "100%" }} />;
}
