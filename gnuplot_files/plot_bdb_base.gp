set terminal pdfcairo font 'Times,19' size 10,3 linewidth 1 rounded dashlength 1

set output "__OUTPUT_FILEPATH__"

set key left

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

set xtics nomirror __XTICS__

set boxwidth 0.25
set style fill pattern 2 border -1

set grid ytics

set ylabel "Runtime (s)" offset 1
set xlabel "Query"

set yrange [0:]

plot "__SPARK_DATA_FILEPATH__" using 2:3:4:5:(0.25) lc rgb "#984EA3" with boxerrorbars title "Spark", \
     "__MONOTASKS_DATA_FILEPATH__" using ($2+0.25):3:4:5:(0.25) lc rgb "#377EB8" with boxerrorbars title "Monotasks"
