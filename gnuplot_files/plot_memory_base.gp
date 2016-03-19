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
set xtics 100000
set yrange [0:100]
set key above

set ylabel "Free Memory (GiB)" offset 1
set xlabel "Time"

plot "__NAME__" using 1:21 with l ls 2 title "Free Heap Memory",\
"__NAME__" using 1:22 with l ls 1 title "Free Off-Heap Memory"
