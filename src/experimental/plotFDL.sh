set terminal svg

unset key

set title "FDL"

set xzeroaxis
set yzeroaxis
set xrange [ -1000 : 1000 ] noreverse nowriteback
set yrange [ -1000 : 1000 ] noreverse nowriteback

unset key

set size ratio -1

set output './graphxlayouter/demo_START.svg'

plot './graphxlayouter/demo-graph.dump.ini2_1_NL.csv' u 2:3:(5) with circles lt 3 fs transparent solid 0.25 noborder,\
     './graphxlayouter/demo-graph.dump.ini2_1_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     './graphxlayouter/demo-graph.dump.ini2_1_NL.csv' u 2:3 with points lw 2 lc rgb "blue"


set output './graphxlayouter/demo_5.svg'
plot './graphxlayouter/debug.demo-graph.dump.C_5_NL.csv' u 2:3:(5) with circles lt 3 fs transparent solid 0.25 noborder,\
     './graphxlayouter/debug.demo-graph.dump.C_5_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     './graphxlayouter/debug.demo-graph.dump.C_5_NL.csv' u 2:3 with points lw 2 lc rgb "blue"

set output './graphxlayouter/demo_4.svg'
plot './graphxlayouter/debug.demo-graph.dump.C_4_NL.csv' u 2:3:(5) with circles lt 3 fs transparent solid 0.25 noborder,\
     './graphxlayouter/debug.demo-graph.dump.C_4_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     './graphxlayouter/debug.demo-graph.dump.C_4_NL.csv' u 2:3 with points lw 2 lc rgb "blue"

set output './graphxlayouter/demo_3.svg'
plot './graphxlayouter/debug.demo-graph.dump.C_3_NL.csv' u 2:3:(5) with circles lt 3 fs transparent solid 0.25 noborder,\
     './graphxlayouter/debug.demo-graph.dump.C_3_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     './graphxlayouter/debug.demo-graph.dump.C_3_NL.csv' u 2:3 with points lw 2 lc rgb "blue"

set output './graphxlayouter/demo_2.svg'
plot './graphxlayouter/debug.demo-graph.dump.C_2_NL.csv' u 2:3:(5) with circles lt 3 fs transparent solid 0.25 noborder,\
     './graphxlayouter/debug.demo-graph.dump.C_2_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     './graphxlayouter/debug.demo-graph.dump.C_2_NL.csv' u 2:3 with points lw 2 lc rgb "blue"

set output './graphxlayouter/demo_1.svg'
plot './graphxlayouter/debug.demo-graph.dump.C_1_NL.csv' u 2:3:(5) with circles lt 3 fs transparent solid 0.25 noborder,\
     './graphxlayouter/debug.demo-graph.dump.C_1_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     './graphxlayouter/debug.demo-graph.dump.C_1_NL.csv' u 2:3 with points lw 2 lc rgb "blue"

set xrange [ -1000 : 1000 ] noreverse nowriteback
set yrange [ -1000 : 1000 ] noreverse nowriteback

set output './graphxlayouter/demo_10.svg'
plot './graphxlayouter/debug.demo-graph.dump.C_10_NL.csv' u 2:3:(50) with circles lt 3 fs transparent solid 0.25 noborder,\
     './graphxlayouter/debug.demo-graph.dump.C_10_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     './graphxlayouter/debug.demo-graph.dump.C_10_NL.csv' u 2:3 with points lw 2 lc rgb "blue"

set output './graphxlayouter/demo_15.svg'
plot './graphxlayouter/debug.demo-graph.dump.C_15_NL.csv' u 2:3:(50) with circles lt 3 fs transparent solid 0.25 noborder,\
     './graphxlayouter/debug.demo-graph.dump.C_15_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     './graphxlayouter/debug.demo-graph.dump.C_15_NL.csv' u 2:3 with points lw 2 lc rgb "blue"

set output './graphxlayouter/demo_20.svg'
plot './graphxlayouter/debug.demo-graph.dump.C_20_NL.csv' u 2:3:(50) with circles lt 3 fs transparent solid 0.25 noborder,\
     './graphxlayouter/debug.demo-graph.dump.C_20_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     './graphxlayouter/debug.demo-graph.dump.C_20_NL.csv' u 2:3 with points lw 2 lc rgb "blue"




