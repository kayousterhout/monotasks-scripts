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
set y2tics

set output "__NAME___map_reduce.pdf"

set style fill solid border -1
set grid xtics
set grid ytics
set key above
set xrange [0:]
set yrange [0:]
set y2range [0:]

set ylabel "Runtime (s)" offset 1
set y2label "Relative diff." offset 1
set xlabel "# Tasks"

plot "__NAME__" using 1:($15 / 1000):($14 / 1000):($16 / 1000) with yerrorbars ls 3 lw 4 title "Map Actual Runtime",\
  "__NAME__" using 1:($15 / 1000) with l ls 3 lw 4 notitle,\
  "__NAME__" using 1:($18 / 1000) with l ls 2 lw 4 title "Map Ideal Runtime",\
  "__NAME__" using 1:($21 / 1000):($20 / 1000):($22 / 1000) with yerrorbars ls 4 lw 4 title "Reduce Actual Runtime",\
  "__NAME__" using 1:($21 / 1000) with l ls 4 lw 4 notitle,\
  "__NAME__" using 1:($24 / 1000) with l ls 5 lw 4 title "Reduce Ideal Runtime"
