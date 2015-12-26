set terminal svg

unset key

set xzeroaxis
set yzeroaxis
set xrange [ -10000 : 10000 ] noreverse nowriteback
set yrange [ -10000 : 10000 ] noreverse nowriteback

unset key

set size ratio -1

set output 'blog/images/original-simple.svg'
set title "FDL local (original)"
plot '/GITHUB/ETOSHA.WS/tmp/original_simple/nodes.csv' u 2:3:(150) with circles lt 3 fs transparent solid 0.25 noborder

set output 'blog/images/fdl-simple.svg'
set title "FDL local (layout done)"
plot '/GITHUB/ETOSHA.WS/tmp/fdl_simple/nodes.csv' u 2:3:(150) with circles lt 3 fs transparent solid 0.25 noborder
 