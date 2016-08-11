set terminal pdfcairo font 'Times,20' rounded dashed dashlength 2

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

set style fill solid border -1
set grid xtics
set grid ytics
set yrange [0:]
set key above

set ylabel "Time (s)" offset 1
set xlabel "Number of tasks"

set output "__OUTPUT_FILEPATH__"

plot "__TOTAL_TIMES_FILEPATH__" using 1:4 with l ls 1 title "Actual times",\
"__TOTAL_TIMES_FILEPATH__" using 1:10 with l ls 2 title "Ideal times, fixed executors",\
"__TOTAL_TIMES_FILEPATH__" using 1:7 with l ls 3 title "Ideal times, balanced executors"
