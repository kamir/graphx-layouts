set terminal svg

unset key

set title "FDL"

set xzeroaxis
set yzeroaxis
set xrange [ -1000 : 1000 ] noreverse nowriteback
set yrange [ -1000 : 1000 ] noreverse nowriteback

unset key

set size ratio -1

set output 'demo_START.svg'
plot 'fdl/demo-graph.dump.ini2_0_NL.csv' u 2:3:(5) with circles lt 3 fs transparent solid 0.25 noborder,\
     'fdl/demo-graph.dump.ini2_0_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     'fdl/demo-graph.dump.ini2_0_NL.csv' u 2:3 with points lw 2 lc rgb "blue"



set output 'demo_END.svg'
plot 'fdl/demo-graph.debug.dump_15_NL.csv' u 2:3:(5) with circles lt 3 fs transparent solid 0.25 noborder,\
     'fdl/demo-graph.debug.dump_15_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     'fdl/demo-graph.debug.dump_15_NL.csv' u 2:3 with points lw 2 lc rgb "blue"
