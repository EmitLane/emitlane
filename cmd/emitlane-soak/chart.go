package main

import (
	"fmt"
	"html"
	"math"
	"strings"
	"time"
)

type timelinePoint struct {
	Elapsed           time.Duration
	Phase             string
	Committed         int64
	Observed          int64
	Backlog           int64
	Restarts          int64
	Crashes           int64
	KafkaOutages      int64
	PauseCycles       int64
	MembershipChanges int64
}

func timelineSVG(runID string, points []timelinePoint) string {
	const width, height = 1200.0, 640.0
	const left, right = 82.0, 34.0
	const eventTop, eventBottom = 82.0, 374.0
	const backlogTop, backlogBottom = 458.0, 566.0
	plotWidth := width - left - right
	maxElapsed := time.Second
	var maxEvents, maxBacklog int64
	for _, point := range points {
		if point.Elapsed > maxElapsed {
			maxElapsed = point.Elapsed
		}
		maxEvents = max(maxEvents, point.Committed, point.Observed)
		maxBacklog = max(maxBacklog, point.Backlog)
	}
	maxEvents = max(int64(1), maxEvents)
	maxBacklog = max(int64(1), maxBacklog)
	x := func(d time.Duration) float64 { return left + plotWidth*float64(d)/float64(maxElapsed) }
	eventY := func(value int64) float64 {
		return eventBottom - (eventBottom-eventTop)*float64(value)/float64(maxEvents)
	}
	backlogY := func(value int64) float64 {
		return backlogBottom - (backlogBottom-backlogTop)*float64(value)/float64(maxBacklog)
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %.0f %.0f" role="img" aria-labelledby="title desc">`, width, height, width, height)
	fmt.Fprintf(&b, `<title id="title">EmitLane soak timeline</title><desc id="desc">Committed and observed events, delivery backlog, and injected faults for run %s.</desc>`, html.EscapeString(runID))
	b.WriteString(`<rect width="1200" height="640" rx="18" fill="#0b1220"/><style>text{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;fill:#cbd5e1}.grid{stroke:#263449;stroke-width:1}.axis{stroke:#64748b;stroke-width:1.2}.small{font-size:12px}.label{font-size:14px}.title{font-size:22px;font-weight:700;fill:#f8fafc}</style>`)
	b.WriteString(`<text x="82" y="38" class="title">EmitLane local soak timeline</text>`)
	fmt.Fprintf(&b, `<text x="82" y="60" class="small">Run %s · %s · %d samples</text>`, html.EscapeString(runID), durationText(maxElapsed), len(points))

	// Highlight the recovery/quiescence window.
	for i, point := range points {
		if point.Phase == "recovering" || point.Phase == "verifying" {
			start := x(point.Elapsed)
			end := width - right
			if i+1 < len(points) {
				end = x(points[i+1].Elapsed)
			}
			fmt.Fprintf(&b, `<rect x="%.2f" y="70" width="%.2f" height="510" fill="#334155" opacity="0.26"/>`, start, max(0.0, end-start))
		}
	}

	for i := 0; i <= 4; i++ {
		y := eventBottom - (eventBottom-eventTop)*float64(i)/4
		value := int64(math.Round(float64(maxEvents) * float64(i) / 4))
		fmt.Fprintf(&b, `<line x1="%.0f" y1="%.2f" x2="%.0f" y2="%.2f" class="grid"/><text x="72" y="%.2f" text-anchor="end" class="small">%d</text>`, left, y, width-right, y, y+4, value)
	}
	for i := 0; i <= 4; i++ {
		xp := left + plotWidth*float64(i)/4
		elapsed := time.Duration(float64(maxElapsed) * float64(i) / 4)
		fmt.Fprintf(&b, `<line x1="%.2f" y1="70" x2="%.2f" y2="580" class="grid"/><text x="%.2f" y="606" text-anchor="middle" class="small">%s</text>`, xp, xp, xp, durationText(elapsed))
	}
	b.WriteString(`<line x1="82" y1="374" x2="1166" y2="374" class="axis"/><line x1="82" y1="566" x2="1166" y2="566" class="axis"/>`)
	b.WriteString(`<text x="82" y="76" class="label">Events</text><text x="82" y="448" class="label">Not observed yet</text>`)

	polyline := func(values func(timelinePoint) int64, y func(int64) float64, color string) {
		b.WriteString(`<polyline fill="none" stroke="` + color + `" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" points="`)
		for _, point := range points {
			fmt.Fprintf(&b, "%.2f,%.2f ", x(point.Elapsed), y(values(point)))
		}
		b.WriteString(`"/>`)
	}
	polyline(func(p timelinePoint) int64 { return p.Committed }, eventY, "#38bdf8")
	polyline(func(p timelinePoint) int64 { return p.Observed }, eventY, "#4ade80")
	polyline(func(p timelinePoint) int64 { return p.Backlog }, backlogY, "#fbbf24")

	for i := 1; i < len(points); i++ {
		previous, point := points[i-1], points[i]
		labels := ""
		if point.KafkaOutages > previous.KafkaOutages {
			labels += "K"
		}
		if point.Crashes > previous.Crashes {
			labels += "C"
		}
		if point.Restarts > previous.Restarts {
			labels += "R"
		}
		if point.PauseCycles > previous.PauseCycles {
			labels += "P"
		}
		if point.MembershipChanges > previous.MembershipChanges {
			labels += "M"
		}
		if labels != "" {
			xp := x(point.Elapsed)
			fmt.Fprintf(&b, `<line x1="%.2f" y1="70" x2="%.2f" y2="580" stroke="#f472b6" stroke-width="1.5" stroke-dasharray="5 5" opacity="0.8"/><text x="%.2f" y="94" text-anchor="middle" font-size="12" fill="#f9a8d4">%s</text>`, xp, xp, xp, labels)
		}
	}

	b.WriteString(`<circle cx="850" cy="38" r="5" fill="#38bdf8"/><text x="862" y="43" class="small">Committed</text><circle cx="952" cy="38" r="5" fill="#4ade80"/><text x="964" y="43" class="small">Observed</text><circle cx="1056" cy="38" r="5" fill="#fbbf24"/><text x="1068" y="43" class="small">Backlog</text>`)
	b.WriteString(`<text x="82" y="628" class="small">Fault markers: K Kafka · C crash · R restart · P pause · M membership</text></svg>`)
	return b.String()
}
