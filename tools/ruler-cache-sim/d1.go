package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	surface   = "#fcfcfb"
	textPri   = "#0b0b0b"
	textSec   = "#52514e"
	gridInk   = "#b9b8b2"
	absentHex = 0xd9d8d3
)

// phaseColor maps a phase in [0, CycleP) onto a constant-lightness OKLCH hue ring.
func phaseColor(p float64) color.RGBA {
	h := 2 * math.Pi * p / CycleP
	L, C := 0.70, 0.12
	a, b := C*math.Cos(h), C*math.Sin(h)
	l := math.Pow(L+0.3963377774*a+0.2158037573*b, 3)
	m := math.Pow(L-0.1055613458*a-0.0638541728*b, 3)
	s := math.Pow(L-0.0894841775*a-1.2914855480*b, 3)
	r := 4.0767416621*l - 3.3077115913*m + 0.2309699292*s
	g := -1.2684380046*l + 2.6097574011*m - 0.3413193965*s
	bl := -0.0041960863*l - 0.7034186147*m + 1.7076147010*s
	gam := func(x float64) uint8 {
		x = math.Max(0, math.Min(1, x))
		if x <= 0.0031308 {
			x *= 12.92
		} else {
			x = 1.055*math.Pow(x, 1/2.4) - 0.055
		}
		return uint8(math.Round(255 * x))
	}
	return color.RGBA{gam(r), gam(g), gam(bl), 255}
}

type d1File struct {
	T         []float64   `json:"t"`
	Phase     [][]float32 `json:"phase"`
	Facet     [][]float32 `json:"facet"`
	FacetBins int         `json:"facetBins"`
	Events    []Event     `json:"events"`
	SwitchAt  float64     `json:"switchAt"`
}

// dominantPhase returns the circular mean phase of each row.
// It moves smoothly when two cohorts of similar size swap rank, unlike the mode.
func dominantPhase(rows [][]float32) []float64 {
	out := make([]float64, len(rows))
	for i, row := range rows {
		var sx, sy float64
		for _, v := range row {
			if v >= 0 {
				a := 2 * math.Pi * float64(v) / CycleP
				sx += math.Cos(a)
				sy += math.Sin(a)
			}
		}
		out[i] = math.Mod(math.Atan2(sy, sx)/(2*math.Pi)*CycleP+CycleP, CycleP)
	}
	return out
}

// rasterPNG colours each cell by its phase offset from the row's dominant phase.
func rasterPNG(rows [][]float32, rowH int, dom []float64) string {
	w := len(rows[0])
	img := image.NewRGBA(image.Rect(0, 0, w, len(rows)*rowH))
	ab := color.RGBA{absentHex >> 16, absentHex >> 8 & 0xff, absentHex & 0xff, 255}
	for y, row := range rows {
		for x, v := range row {
			c := ab
			if v >= 0 {
				c = phaseColor(math.Mod(float64(v)-dom[y]+CycleP/2+CycleP, CycleP))
			}
			for k := 0; k < rowH; k++ {
				img.SetRGBA(x, y*rowH+k, c)
			}
		}
	}
	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func renderD1(r *runData, rep string) error {
	b, err := os.ReadFile(filepath.Join(r.dir, "d1rows.json"))
	if err != nil {
		return err
	}
	var d d1File
	if err := json.Unmarshal(b, &d); err != nil {
		return err
	}
	rowH := 2
	if len(d.T) > 320 {
		rowH = 1
	}
	name := filepath.Base(r.dir)
	desc := scnDesc[r.sum.Scenario]
	start := "cold cache"
	if r.sum.Mode == "warm" {
		start = "cold cache, server events replayed from 24 h, policy switch at 48 h"
	}
	// List-order map.
	dom := dominantPhase(d.Phase)
	listImg := rasterPNG(d.Phase, rowH, dom)
	svg := d1SVG(fmt.Sprintf("Cohort map: %s %s", r.sum.Scenario, desc),
		fmt.Sprintf("One row per 615 s, %s, fixed 30 s arrivals, seed %d. Cell = modal expiry phase of 73 keys.", start, r.sum.Seed),
		[]facet{{img: listImg, w: D1Bins, label: "", xmax: N}}, len(d.T), rowH, d)
	if err := os.WriteFile(filepath.Join(rep, "d1_"+name+".svg"), []byte(svg), 0o644); err != nil {
		return err
	}
	// Faceted by memcached server index.
	var fs []facet
	for sh := 0; sh < Shards; sh++ {
		rows := make([][]float32, len(d.Facet))
		for i := range d.Facet {
			rows[i] = d.Facet[i][sh*d.FacetBins : (sh+1)*d.FacetBins]
		}
		fs = append(fs, facet{img: rasterPNG(rows, rowH, dom), w: d.FacetBins, label: fmt.Sprintf("server idx %d", sh), xmax: 0})
	}
	svg = d1SVG(fmt.Sprintf("Cohort map by memcached server: %s %s", r.sum.Scenario, desc),
		fmt.Sprintf("Same run. Each panel holds the keys of one server index, in list order. %s, seed %d.", start, r.sum.Seed),
		fs, len(d.T), rowH, d)
	return os.WriteFile(filepath.Join(rep, "d1_"+name+"_by_server.svg"), []byte(svg), 0o644)
}

type facet struct {
	img   string
	w     int
	label string
	xmax  int
}

func d1SVG(title, sub string, fs []facet, nrows, rowH int, d d1File) string {
	const left, top, gap, right, bottom = 64, 112, 14, 210, 46
	plotH := nrows * rowH
	plotW := 0
	for i, f := range fs {
		if i > 0 {
			plotW += gap
		}
		plotW += f.w
	}
	W, Hh := left+plotW+right, top+plotH+bottom
	var sb strings.Builder
	fmt.Fprintf(&sb, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" font-family="system-ui, -apple-system, Segoe UI, sans-serif">`+"\n", W, Hh, W, Hh)
	fmt.Fprintf(&sb, `<rect width="100%%" height="100%%" fill="%s"/>`+"\n", surface)
	fmt.Fprintf(&sb, `<text x="%d" y="26" font-size="17" font-weight="600" fill="%s">%s</text>`+"\n", left, textPri, esc(title))
	fmt.Fprintf(&sb, `<text x="%d" y="46" font-size="12" fill="%s">%s</text>`+"\n", left, textSec, esc(sub))
	// Legend strip.
	lx, ly, lw := left, 62, 300
	for i := 0; i < lw; i++ {
		c := phaseColor(float64(i) / float64(lw) * CycleP)
		fmt.Fprintf(&sb, `<rect x="%d" y="%d" width="1.2" height="12" fill="#%02x%02x%02x"/>`, lx+i, ly, c.R, c.G, c.B)
	}
	sb.WriteString("\n")
	for _, v := range []float64{-300, -150, 0, 150, 300} {
		x := float64(lx) + (v+CycleP/2)/CycleP*float64(lw)
		fmt.Fprintf(&sb, `<text x="%.1f" y="%d" font-size="11" fill="%s" text-anchor="middle">%+.0f</text>`, x, ly+26, textSec, v)
	}
	fmt.Fprintf(&sb, `<text x="%d" y="%d" font-size="11" fill="%s">expiry phase offset from the row's circular mean, s (cyclic, period 615 s)</text>`, lx+lw+12, ly+10, textSec)
	fmt.Fprintf(&sb, `<rect x="%d" y="%d" width="12" height="12" fill="#%06x"/><text x="%d" y="%d" font-size="11" fill="%s">not cached at row time</text>`+"\n", lx+lw+12, ly+16, absentHex, lx+lw+30, ly+26, textSec)
	// Panels.
	x := left
	for _, f := range fs {
		fmt.Fprintf(&sb, `<image x="%d" y="%d" width="%d" height="%d" preserveAspectRatio="none" style="image-rendering:pixelated" href="data:image/png;base64,%s"/>`+"\n", x, top, f.w, plotH, f.img)
		if f.label != "" {
			fmt.Fprintf(&sb, `<text x="%d" y="%d" font-size="12" fill="%s" text-anchor="middle">%s</text>`+"\n", x+f.w/2, top-6, textPri, f.label)
		}
		if f.xmax > 0 {
			for _, v := range []int{0, 20000, 40000, 60000} {
				px := float64(x) + float64(v)/float64(f.xmax)*float64(f.w)
				fmt.Fprintf(&sb, `<line x1="%.1f" x2="%.1f" y1="%d" y2="%d" stroke="%s"/><text x="%.1f" y="%d" font-size="11" fill="%s" text-anchor="middle">%dk</text>`+"\n", px, px, top+plotH, top+plotH+5, gridInk, px, top+plotH+18, textSec, v/1000)
			}
			fmt.Fprintf(&sb, `<text x="%d" y="%d" font-size="12" fill="%s" text-anchor="middle">list position (rule group index in list order)</text>`+"\n", x+f.w/2, top+plotH+36, textSec)
		} else {
			fmt.Fprintf(&sb, `<text x="%d" y="%d" font-size="11" fill="%s" text-anchor="middle">list order</text>`+"\n", x+f.w/2, top+plotH+18, textSec)
		}
		x += f.w + gap
	}
	// Y axis: simulated hours.
	end := float64(nrows) * CycleP
	step := 6.0
	if end > 60*H {
		step = 12
	}
	for h := 0.0; h*H <= end; h += step {
		y := float64(top) + h*H/CycleP*float64(rowH)
		fmt.Fprintf(&sb, `<line x1="%d" x2="%d" y1="%.1f" y2="%.1f" stroke="%s"/><text x="%d" y="%.1f" font-size="11" fill="%s" text-anchor="end">%.0f h</text>`+"\n", left-5, left, y, y, gridInk, left-8, y+4, textSec, h)
	}
	// Events and policy switch, marked on the right edge.
	rx := left + plotW
	mark := func(t float64, label string) {
		y := float64(top) + t/CycleP*float64(rowH)
		fmt.Fprintf(&sb, `<line x1="%d" x2="%d" y1="%.1f" y2="%.1f" stroke="%s" stroke-width="2"/><text x="%d" y="%.1f" font-size="11" fill="%s">%s</text>`+"\n", rx+2, rx+12, y, y, textPri, rx+16, y+4, textSec, esc(label))
	}
	for i := 0; i < len(d.Events); {
		j := i
		var idx []string
		labels := map[string]bool{}
		for j < len(d.Events) && d.Events[j].T-d.Events[i].T < 2*H {
			for _, s := range d.Events[j].Shards {
				idx = append(idx, fmt.Sprint(s))
			}
			labels[d.Events[j].Label] = true
			mark(d.Events[j].T, "")
			j++
		}
		var ls []string
		for l := range labels {
			ls = append(ls, l)
		}
		sort.Strings(ls)
		y := float64(top) + d.Events[i].T/CycleP*float64(rowH)
		fmt.Fprintf(&sb, `<text x="%d" y="%.1f" font-size="11" fill="%s">idx %s lost (%s)</text>`+"\n", rx+16, y+4, textSec, strings.Join(idx, ","), strings.Join(ls, ", "))
		i = j
	}
	if d.SwitchAt > 0 {
		mark(d.SwitchAt, "policy switch")
	}
	sb.WriteString("</svg>\n")
	return sb.String()
}

func esc(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
