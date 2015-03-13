set terminal svg

unset key

set title "FDL"

set xrange [ -500 : 500 ] noreverse nowriteback
set yrange [ -500 : 500 ] noreverse nowriteback

unset key
set size ratio -1

set output '/mnt/hgfs/SHARE.VM.MAC/DATA/graphxlayouter/demo_START.svg'

plot '/mnt/hgfs/SHARE.VM.MAC/DATA/graphxlayouter/demo-graph.dump.ini2_1_NL.csv' u 2:3:(50) with circles lt 3 fs transparent solid 0.25 noborder,\
     '/mnt/hgfs/SHARE.VM.MAC/DATA/graphxlayouter/demo-graph.dump.ini2_1_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     '/mnt/hgfs/SHARE.VM.MAC/DATA/graphxlayouter/demo-graph.dump.ini2_1_NL.csv' u 2:3:(sprintf("x=%d\ny=%d", $2, $3)) with labels lw 2 lc rgb "black"

set xrange [ -1000 : 1000 ] noreverse nowriteback
set yrange [ -1000 : 1000 ] noreverse nowriteback

set output '/mnt/hgfs/SHARE.VM.MAC/DATA/graphxlayouter/demo_1.svg'

plot '/mnt/hgfs/SHARE.VM.MAC/DATA/graphxlayouter/demo-graph.dump_1_NL.csv' u 2:3:(50) with circles lt 3 fs transparent solid 0.25 noborder,\
     '/mnt/hgfs/SHARE.VM.MAC/DATA/graphxlayouter/demo-graph.dump_1_NL.csv' u 2:3:4:5 with vectors nohead filled lt 2,\
     '/mnt/hgfs/SHARE.VM.MAC/DATA/graphxlayouter/demo-graph.dump_1_NL.csv' u 2:3 with points lw 2 lc rgb "black"


