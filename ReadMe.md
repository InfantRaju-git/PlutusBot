// This source code is subject to the terms of the Mozilla Public License 2.0 at https://mozilla.org/MPL/2.0/
// © infantraju123

//@version=5
strategy("InfantRaju ", overlay=true)

start = input(0.02)
increment = input(0.2)
maximum = input(0.2)
ema50 = ta.ema(close, 50)
var bool uptrend = na
var float EP = na
var float SAR = na
var float AF = start
var float nextBarSAR = na
if bar_index > 0
	firstTrendBar = false
	SAR := nextBarSAR
	if bar_index == 1
		float prevSAR = na
		float prevEP = na
		lowPrev = low[1]
		highPrev = high[1]
		closeCur = close
		closePrev = close[1]
		if closeCur > closePrev 
			uptrend := true
			EP := high
			prevSAR := lowPrev
			prevEP := high
		else
			uptrend := false
			EP := low
			prevSAR := highPrev
			prevEP := low
		firstTrendBar := true
		SAR := prevSAR + start * (prevEP - prevSAR)
	if uptrend
		if SAR > low
			firstTrendBar := true
			uptrend := false
			SAR := math.max(EP, high)
			EP := low
			AF := start
	else
		if SAR < high
			firstTrendBar := true
			uptrend := true
			SAR := math.min(EP, low)
			EP := high
			AF := start
	if not firstTrendBar
		if uptrend
			if high > EP
				EP := high
				AF := math.min(AF + increment, maximum)
		else
			if low < EP
				EP := low
				AF := math.min(AF + increment, maximum)
	if uptrend
		SAR := math.min(SAR, low[1])
		if bar_index > 1
			SAR := math.min(SAR, low[2])
	else
		SAR := math.max(SAR, high[1])
		if bar_index > 1
			SAR := math.max(SAR, high[2])
	nextBarSAR := SAR + AF * (EP - SAR)
	if barstate.isconfirmed
		if uptrend
			strategy.entry("S", strategy.short, stop=nextBarSAR, comment="S")
			strategy.cancel("L")
		else
			strategy.entry("L", strategy.long, stop=nextBarSAR, comment="L")
			strategy.cancel("S")
//plot(SAR, style=plot.style_cross, linewidth=1, color=color.orange)
//plot(ema50, color=color.blue, title="20 EMA")

len = 100
p1 =close
p2 = close[100] 
sma = ta.sma(p1, len)
sma1 = ta.sma(p2, len)
avg = ta.atr(len)
fibratio1 = 1.618
fibratio3 = 4.236
r1 = avg * fibratio1
r3 = avg * fibratio3
top3 = sma + r3
bott1 = sma - r1
top2 = sma1 + r3
bott2 = sma1 - r1
src5 = input(close)
len5 = 100
ma = ta.ema(src5*volume, len5) / ta.ema(volume, len5)
src1 = ma
p(src1, len5) =>
    n = 0.0
    s = 0.0
    for i = 0 to len5 - 1
        w = (len5 - i) * len5
        n := n + w
        s := s + src5[i] * w
    s / n

hm = 2.0 * p(src1, math.floor(len5 / 2)) - p(src1, len5)
vhma = p(hm, math.floor(math.sqrt(len5)))
lineColor = vhma > vhma[1] ? color.lime : color.red
plot(vhma, title="VHMA", color=lineColor ,linewidth=3)
alertcondition(ta.crossover(close, vhma) or ta.crossunder(close, vhma), title="New Signal", message="New Signal confirmed")

// hColor = true,vis = true
// hu = hColor ? (vhma > vhma[2] ? #00ff00 : #ff0000) : #ff9800
// vl = vhma[0]
// ll = vhma[1]
// m1 = plot(vl, color=hu, linewidth=1, transp=60)
// m2 = plot(vis ? ll : na,  color=hu, linewidth=2, transp=80)
// fill(m1, m2,  color=hu, transp=70)


