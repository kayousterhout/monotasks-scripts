set terminal pdfcairo font 'Times,20' rounded dashlength 2

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
set xtics nomirror
set ytics nomirror

set output "__NAME__.pdf"

set style fill solid border -1
set grid xtics
set grid ytics
set yrange [0:1.2]
set key above

set ylabel "Utilization" offset 1
set xlabel "Time"

plot "__NAME__" using 1:4 with l ls 3 title "CPU Utilization",\
"__NAME__" using 1:5 with l ls 4 title "Network Utilization (recv'd)",\
"__NAME__" using 1:6 with l ls 5 title "Network Utilization (trans'd)",\
"__NAME__" using 1:2 with l ls 2 title "Disk Utilization (disk 1)",\
"__NAME__" using 1:3 with l ls 1 title "Disk Utilization (disk 2)",\
"__NAME__" using 1:9 with l ls 6 title "GC fraction",\
"__NAME__" using 1:13 with l ls 7 title "System CPU Utilization"
