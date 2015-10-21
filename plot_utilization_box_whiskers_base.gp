set terminal pdfcairo font 'Times,20' size 5,2.5 linewidth 4 rounded dashlength 2

# Line style for axes
set style line 80 lt 1 lc rgb "#808080"

# Line style for grid
set style line 81 lt 0 # dashed
set style line 81 lt rgb "#808080"  # grey


set grid ytics back linestyle 81
set border 3 back linestyle 80 # Remove border on top and right.  These
                               # borders are useless and make it harder
                               # to see plotted lines near the border.
                               # Also, put it in grey; no need for so much emphasis on a border.
#set xtics nomirror rotate by -45 font 'Times,16'
set ytics nomirror

set output "__PREFIX___utilization_box_whiskers.pdf"

set boxwidth 0.2
set style fill pattern 1 border -1
set grid ytics
set ylabel "Utilization" offset 1
set key above
set xrange [-0.25:0.75]

plot "__PREFIX___cpu_utilization" using ($2):(1-$4):(1-$3):(1-$7):(1-$6) with candlesticks fs pattern 0 lc rgb "white" title "CPU" whiskerbars,\
"__PREFIX___cpu_utilization" using ($2):(1-$5):(1-$5):(1-$5):(1-$5) with candlesticks notitle,\
"__PREFIX___disk_utilization" using ($2+0.25):(1-$4):(1-$3):(1-$7):(1-$6) with candlesticks fs pattern 2 lc rgb "#377EB8" title "Disk" whiskerbars,\
"__PREFIX___disk_utilization" using ($2+0.25):(1-$5):(1-$5):(1-$5):(1-$5) with candlesticks notitle,\
"__PREFIX___network_utilization" using ($2+0.5):(1-$4):(1-$3):(1-$7):(1-$6) with candlesticks fs pattern 3 lc rgb "#4DAF4A" title "Network" whiskerbars,\
"__PREFIX___network_utilization" using ($2+0.5):(1-$5):(1-$5):(1-$5):(1-$5) with candlesticks notitle
