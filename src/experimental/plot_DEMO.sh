unset key
unset border
unset yzeroaxis
unset xtics
unset ytics
unset ztics

set title "splot with variable size points\nit is possible to specify size and color separately"

set view map

unset hidden3d
splot '/mnt/hgfs/SHARE.VM.MAC/DATA/demo-graph.dump_1_NL.csv' using 2:3:(0.5-rand(0)):(5.*rand(0)) with points pt 5 ps var lt palette



splot '/mnt/hgfs/SHARE.VM.MAC/DATA/demo-graph.dump_1_NL.csv' using 2:3:(0.5-rand(0)):(5.*rand(0)) with circles lc rgb "blue" fs transparent solid 0.15 noborder
