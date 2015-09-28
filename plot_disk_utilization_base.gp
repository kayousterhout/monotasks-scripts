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

set output "__OUT_FILENAME__"

set style fill solid border -1
set grid xtics
set grid ytics
set y2tics
set xtics 1000000
set yrange [0:1.2]
set y2range [0:100000000]
set key above

set ylabel "Utilization" offset 1
set y2label "Throughput"
set xlabel "Time"

plot "__NAME__" using 1:2 with l ls 2 title "Disk Utilization (disk 1)",\
"__NAME__" using 1:3 with l ls 1 title "Disk Utilization (disk 2)",\
"__NAME__" using 1:15 with l ls 3 title "Read Throughput (disk 1)" axes x1y2,\
"__NAME__" using 1:17 with l ls 4 title "Read Throughput (disk 2)" axes x1y2,\
"__NAME__" using 1:16 with l ls 6 title "Write Throughput (disk 1)" axes x1y2,\
"__NAME__" using 1:18 with l ls 7 title "Write Throughput (disk 2)" axes x1y2
